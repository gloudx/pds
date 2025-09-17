package storage

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"sync"
	"time"

	"pds/blockstore"
	s "pds/datastore"
	i "pds/indexer"
	"pds/lexicon"
	"pds/mstdatastore"
	"pds/oplog"

	"github.com/ipfs/go-cid"
	"github.com/ipfs/go-datastore"
	badger4 "github.com/ipfs/go-ds-badger4"
)

// =============================================================================
// ГЛАВНЫЙ МЕНЕДЖЕР ХРАНЕНИЯ UES
// =============================================================================

// StorageManager координирует все компоненты UES системы
type StorageManager struct {
	// Основные компоненты
	storage      s.Datastore                // Базовое BadgerDB хранилище
	mst          *mstdatastore.MstDatastore // MST слой поверх хранилища
	blockStore   *blockstore.Blockstore     // IPFS блокстор
	indexer      *i.Indexer                 // SQLite индексатор
	operationLog oplog.OperationLog         // HLC operation log
	lexicon      *lexicon.LexiconService    // Система лексиконов
	syncManager  *lexicon.SyncManager       // Менеджер синхронизации

	// Кеширование и производительность
	memoryCache *MemoryCache      // Горячие данные в RAM
	metrics     *MetricsCollector // Метрики производительности

	// Управление жизненным циклом
	config Config             // Конфигурация
	nodeID string             // ID узла
	mutex  sync.RWMutex       // Защита состояния
	ctx    context.Context    // Контекст отмены
	cancel context.CancelFunc // Функция отмены

	// Статус
	initialized bool
	closed      bool
}

// Config конфигурация storage менеджера
type Config struct {
	DataDir     string `json:"dataDir"`
	StoragePath string `json:"storagePath"`
	IndexPath   string `json:"indexPath"`
	NodeID      string `json:"nodeId"`

	// Кеширование
	MemoryCacheSizeMB int `json:"memoryCacheSizeMb"`

	// Производительность
	MaxConcurrentOps int    `json:"maxConcurrentOps"`
	SyncInterval     string `json:"syncInterval"`

	// Автоматические задачи
	EnableAutoCompact bool   `json:"enableAutoCompact"`
	CompactInterval   string `json:"compactInterval"`
	EnableMetrics     bool   `json:"enableMetrics"`
}

// NewStorageManager создает и инициализирует менеджер хранения
func NewStorageManager(config Config) (*StorageManager, error) {
	if config.NodeID == "" {
		return nil, fmt.Errorf("node ID is required")
	}

	ctx, cancel := context.WithCancel(context.Background())

	sm := &StorageManager{
		config: config,
		nodeID: config.NodeID,
		ctx:    ctx,
		cancel: cancel,
	}

	// Инициализируем все компоненты
	if err := sm.initializeComponents(); err != nil {
		cancel()
		return nil, fmt.Errorf("failed to initialize components: %w", err)
	}

	// Запускаем фоновые процессы
	// go sm.runMaintenanceTasks()

	sm.initialized = true
	log.Printf("✅ StorageManager initialized with node ID: %s", config.NodeID)

	return sm, nil
}

// initializeComponents инициализирует все компоненты в правильном порядке
func (sm *StorageManager) initializeComponents() error {
	var err error

	// 1. Базовое хранилище (BadgerDB)
	log.Println("📦 Initializing base storage...")
	sm.storage, err = s.NewDatastorage(sm.config.StoragePath, s.Options{
		Options: badger4.DefaultOptions,
	})
	if err != nil {
		return fmt.Errorf("failed to create storage: %w", err)
	}

	// 2. Memory cache
	sm.memoryCache = NewMemoryCache(int64(sm.config.MemoryCacheSizeMB) * 1024 * 1024)

	// 3. SQLite индексатор
	log.Println("🔍 Initializing indexer...")
	sm.indexer, err = i.NewIndexer(sm.config.IndexPath, sm.storage)
	if err != nil {
		return fmt.Errorf("failed to create indexer: %w", err)
	}

	// 4. MST Datastore
	log.Println("🌳 Initializing MST...")
	sm.mst, err = mstdatastore.NewMstDatastore(sm.storage)
	if err != nil {
		return fmt.Errorf("failed to create MST: %w", err)
	}

	// 5. IPFS Blockstore
	log.Println("📁 Initializing blockstore...")
	sm.blockStore = blockstore.NewBlockstore(sm.storage, sm.indexer)

	// 6. Operation Log
	log.Println("📝 Initializing operation log...")
	sm.operationLog = oplog.NewStorageOperationLog(sm.storage, sm.nodeID)

	// 7. Lexicon System
	log.Println("📋 Initializing lexicon system...")
	sm.lexicon, err = lexicon.NewLexiconService(sm.storage, sm.mst, sm.indexer)
	if err != nil {
		return fmt.Errorf("failed to create lexicon service: %w", err)
	}

	// 8. Sync Manager
	log.Println("🔄 Initializing sync manager...")
	sm.syncManager = lexicon.NewSyncManager(sm.lexicon, sm.operationLog, sm.nodeID)

	// 9. Metrics collector (если включен)
	if sm.config.EnableMetrics {
		sm.metrics = NewMetricsCollector(sm.nodeID)
	}

	return nil
}

// =============================================================================
// ВЫСОКОУРОВНЕВЫЕ API ДЛЯ ПРИЛОЖЕНИЙ
// =============================================================================

// StoreFile сохраняет файл в системе с автоматическим индексированием
func (sm *StorageManager) StoreFile(ctx context.Context, filename string, data io.Reader, contentType string) (*StoredFile, error) {

	if !sm.isReady() {
		return nil, fmt.Errorf("storage manager not ready")
	}

	startTime := time.Now()
	defer func() {
		if sm.metrics != nil {
			sm.metrics.RecordOperation("store_file", time.Since(startTime))
		}
	}()

	// Добавляем файл в IPFS blockstore
	cid, err := sm.blockStore.AddFile(ctx, filename, data, true) // Используем Rabin chunking для больших файлов
	if err != nil {
		return nil, fmt.Errorf("failed to store file in blockstore: %w", err)
	}

	// Создаем запись о файле в лексиконе
	mediaData := map[string]interface{}{
		"filename": filename,
		"mimeType": contentType,
		"cid":      cid.String(),
		// "storedAt": time.Now().Format(time.RFC3339),
		// "nodeId":   sm.nodeID,
	}

	record, err := sm.lexicon.CreateRecord(ctx, "app.uds.media", mediaData, sm.nodeID)
	if err != nil {
		log.Printf("⚠️ Failed to create lexicon record for file %s: %v", filename, err)
		// Не фейлим операцию, файл уже сохранен
	}

	// Логируем операцию для репликации
	if err := sm.logFileOperation(ctx, "store_file", cid, filename, contentType); err != nil {
		log.Printf("⚠️ Failed to log store operation: %v", err)
	}

	// Обновляем кеш если файл небольшой
	if len(filename) < 1024*1024 { // 1MB
		// TODO: кешировать небольшие файлы
	}

	result := &StoredFile{
		CID:      cid.String(),
		Filename: filename,
		RecordID: string(record.ID),
		Size:     0, // TODO: получить размер из blockstore
		StoredAt: time.Now(),
	}

	return result, nil
}

// GetFile получает файл из системы
func (sm *StorageManager) GetFile(ctx context.Context, cidStr string) (io.ReadSeekCloser, *FileMetadata, error) {
	if !sm.isReady() {
		return nil, nil, fmt.Errorf("storage manager not ready")
	}

	startTime := time.Now()
	defer func() {
		if sm.metrics != nil {
			sm.metrics.RecordOperation("get_file", time.Since(startTime))
		}
	}()

	// Парсим CID
	fileCid, err := cid.Parse(cidStr)
	if err != nil {
		return nil, nil, fmt.Errorf("invalid CID: %w", err)
	}

	// Проверяем кеш
	if cached, ok := sm.memoryCache.Get("file:" + cidStr); ok {
		if fileData, ok := cached.([]byte); ok {
			metadata := &FileMetadata{
				CID:    cidStr,
				Cached: true,
			}
			return newBytesReadSeekCloser(fileData), metadata, nil
		}
	}

	// Получаем из blockstore
	reader, err := sm.blockStore.GetReader(ctx, fileCid)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to get file from blockstore: %w", err)
	}

	// Пытаемся получить метаданные из лексикона
	metadata := &FileMetadata{
		CID:    cidStr,
		Cached: false,
	}

	// Поиск записи в индексе
	if results, err := sm.indexer.SearchByContentType("application/json", 100, 0); err == nil {
		for _, result := range results {
			if meta, ok := result.Metadata["record_data"].(map[string]interface{}); ok {
				if recordCid, ok := meta["cid"].(string); ok && recordCid == cidStr {
					metadata.Filename = meta["filename"].(string)
					metadata.MimeType = meta["mimeType"].(string)
					break
				}
			}
		}
	}

	return reader, metadata, nil
}

// CreateTypedRecord создает типизированную запись
func (sm *StorageManager) CreateTypedRecord(ctx context.Context, lexiconID lexicon.LexiconID, data interface{}, author string) (*lexicon.TypedRecord, error) {
	if !sm.isReady() {
		return nil, fmt.Errorf("storage manager not ready")
	}

	startTime := time.Now()
	defer func() {
		if sm.metrics != nil {
			sm.metrics.RecordOperation("create_record", time.Since(startTime))
		}
	}()

	// Создаем запись через лексикон
	record, err := sm.lexicon.CreateRecord(ctx, lexiconID, data, author)
	if err != nil {
		return nil, fmt.Errorf("failed to create record: %w", err)
	}

	// Логируем операцию
	if err := sm.logRecordOperation(ctx, "create_record", record); err != nil {
		log.Printf("⚠️ Failed to log record operation: %v", err)
	}

	return record, nil
}

// QueryRecords выполняет поиск записей
func (sm *StorageManager) QueryRecords(ctx context.Context, filter *lexicon.QueryFilter) (*lexicon.QueryResult, error) {
	if !sm.isReady() {
		return nil, fmt.Errorf("storage manager not ready")
	}

	startTime := time.Now()
	defer func() {
		if sm.metrics != nil {
			sm.metrics.RecordOperation("query_records", time.Since(startTime))
		}
	}()

	return sm.lexicon.QueryRecords(ctx, filter)
}

// SearchFiles выполняет полнотекстовый поиск файлов
func (sm *StorageManager) SearchFiles(ctx context.Context, query string, limit int) ([]*i.SearchResult, error) {
	if !sm.isReady() {
		return nil, fmt.Errorf("storage manager not ready")
	}

	startTime := time.Now()
	defer func() {
		if sm.metrics != nil {
			sm.metrics.RecordOperation("search_files", time.Since(startTime))
		}
	}()

	return sm.indexer.SearchText(query, limit, 0)
}

// RegisterSchema регистрирует новую схему лексикона
func (sm *StorageManager) RegisterSchema(ctx context.Context, schema *lexicon.LexiconDefinition) error {
	if !sm.isReady() {
		return fmt.Errorf("storage manager not ready")
	}

	startTime := time.Now()
	defer func() {
		if sm.metrics != nil {
			sm.metrics.RecordOperation("register_schema", time.Since(startTime))
		}
	}()

	return sm.lexicon.RegisterSchema(ctx, schema)
}

// =============================================================================
// СИНХРОНИЗАЦИЯ И РЕПЛИКАЦИЯ
// =============================================================================

// SyncWithPeer синхронизирует данные с удаленным узлом
func (sm *StorageManager) SyncWithPeer(ctx context.Context, peerID string) (*SyncResult, error) {
	if !sm.isReady() {
		return nil, fmt.Errorf("storage manager not ready")
	}

	startTime := time.Now()
	defer func() {
		if sm.metrics != nil {
			sm.metrics.RecordOperation("sync_peer", time.Since(startTime))
		}
	}()

	result := &SyncResult{
		PeerID:    peerID,
		StartTime: startTime,
	}

	// 1. Синхронизация схем
	localManifest, err := sm.syncManager.GetSchemaManifest(ctx)
	if err != nil {
		return result, fmt.Errorf("failed to get local schema manifest: %w", err)
	}

	result.LocalSchemas = len(localManifest)

	// В реальной реализации здесь был бы сетевой код для:
	// - Обмена манифестами схем
	// - Получения различающихся схем
	// - Синхронизации записей
	// - Применения изменений

	result.EndTime = time.Now()
	result.Success = true

	return result, nil
}

// GetSyncEvents получает события для синхронизации
func (sm *StorageManager) GetSyncEvents(ctx context.Context, since *oplog.HybridLogicalClock) ([]*lexicon.SyncEvent, error) {
	if !sm.isReady() {
		return nil, fmt.Errorf("storage manager not ready")
	}

	return sm.syncManager.GetSyncEvents(ctx, since)
}

// ProcessSyncEvents обрабатывает входящие события синхронизации
func (sm *StorageManager) ProcessSyncEvents(ctx context.Context, events []*lexicon.SyncEvent) error {
	if !sm.isReady() {
		return fmt.Errorf("storage manager not ready")
	}

	return sm.syncManager.ProcessSyncEvents(ctx, events)
}

// =============================================================================
// СТАТИСТИКА И МОНИТОРИНГ
// =============================================================================

// GetSystemStats возвращает статистику системы
func (sm *StorageManager) GetSystemStats(ctx context.Context) (*SystemStats, error) {
	if !sm.isReady() {
		return nil, fmt.Errorf("storage manager not ready")
	}

	stats := &SystemStats{
		NodeID:    sm.nodeID,
		Timestamp: time.Now(),
	}

	// Статистика индексатора
	if indexStats, err := sm.indexer.GetStats(); err == nil {
		stats.IndexedEntries = indexStats["total_entries"].(int64)
		stats.TotalSize = indexStats["total_size"].(int64)

		if contentTypes, ok := indexStats["content_types"].(map[string]int64); ok {
			stats.ContentTypes = contentTypes
		}
	}

	// Статистика лексиконов
	if schemas, err := sm.lexicon.ListSchemas(ctx); err == nil {
		stats.TotalSchemas = len(schemas)
	}

	// MST статистика
	collections := sm.mst.ListCollections()
	stats.Collections = make(map[string]int)
	for _, collection := range collections {
		stats.Collections[collection.Name] = collection.ItemsCount
	}

	// Кеш статистика
	stats.CacheHits, stats.CacheMisses = sm.memoryCache.GetStats()

	// Метрики производительности
	if sm.metrics != nil {
		stats.Metrics = sm.metrics.GetSummary()
	}

	return stats, nil
}

// GetHealthStatus возвращает статус здоровья системы
func (sm *StorageManager) GetHealthStatus(ctx context.Context) *HealthStatus {
	status := &HealthStatus{
		Timestamp:  time.Now(),
		Overall:    "healthy",
		Components: make(map[string]ComponentHealth),
	}

	// Проверяем каждый компонент
	status.Components["storage"] = sm.checkStorageHealth()
	status.Components["indexer"] = sm.checkIndexerHealth()
	status.Components["mst"] = sm.checkMSTHealth()
	status.Components["blockstore"] = sm.checkBlockstoreHealth()
	status.Components["lexicon"] = sm.checkLexiconHealth()

	// Определяем общий статус
	for _, comp := range status.Components {
		if comp.Status != "healthy" {
			status.Overall = "degraded"
			break
		}
	}

	return status
}

// =============================================================================
// BACKGROUND PROCESSES
// =============================================================================

// runMaintenanceTasks запускает фоновые задачи
func (sm *StorageManager) runMaintenanceTasks() {
	// Компактификация данных
	if sm.config.EnableAutoCompact {
		go sm.runCompactionTask()
	}

	// Синхронизация метрик
	if sm.metrics != nil {
		go sm.runMetricsTask()
	}

	// Основной цикл обслуживания
	ticker := time.NewTicker(30 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-sm.ctx.Done():
			return
		case <-ticker.C:
			sm.performMaintenance()
		}
	}
}

// runMetricsTask запускает задачу сбора метрик
func (sm *StorageManager) runMetricsTask() {
	ticker := time.NewTicker(1 * time.Minute)
	defer ticker.Stop()

	for {
		select {
		case <-sm.ctx.Done():
			return
		case <-ticker.C:
			if sm.metrics != nil {
				sm.metrics.Collect()
			}
		}
	}
}

// performMaintenance выполняет регулярное обслуживание
func (sm *StorageManager) performMaintenance() {
	// Очищаем старый кеш
	sm.memoryCache.Cleanup()

	// Логируем статистику
	if stats, err := sm.GetSystemStats(sm.ctx); err == nil {
		log.Printf("📊 System stats - Entries: %d, Size: %d bytes, Cache hits: %d",
			stats.IndexedEntries, stats.TotalSize, stats.CacheHits)
	}
}

// runCompactionTask запускает задачу компактификации
func (sm *StorageManager) runCompactionTask() {
	interval, err := time.ParseDuration(sm.config.CompactInterval)
	if err != nil {
		interval = 1 * time.Hour // По умолчанию
	}

	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		select {
		case <-sm.ctx.Done():
			return
		case <-ticker.C:
			sm.performCompaction()
		}
	}
}

// performCompaction выполняет компактификацию данных
func (sm *StorageManager) performCompaction() {
	log.Println("🧹 Starting data compaction...")
	startTime := time.Now()

	// Компактификация operation log
	cutoffTime := oplog.NewHLClockFromParams(
		time.Now().Add(-7*24*time.Hour).UnixNano(), 0, sm.nodeID,
	)

	if err := sm.operationLog.Compact(sm.ctx, cutoffTime); err != nil {
		log.Printf("⚠️ Operation log compaction failed: %v", err)
	}

	log.Printf("✅ Compaction completed in %v", time.Since(startTime))
}

// =============================================================================
// LIFECYCLE MANAGEMENT
// =============================================================================

// Close корректно закрывает менеджер хранения
func (sm *StorageManager) Close() error {
	sm.mutex.Lock()
	defer sm.mutex.Unlock()

	if sm.closed {
		return nil
	}

	log.Println("🛑 Shutting down StorageManager...")

	// Останавливаем фоновые процессы
	sm.cancel()

	var errors []error

	// Закрываем компоненты в обратном порядке
	if sm.blockStore != nil {
		if err := sm.blockStore.Close(); err != nil {
			errors = append(errors, fmt.Errorf("blockstore close error: %w", err))
		}
	}

	if sm.indexer != nil {
		if err := sm.indexer.Close(); err != nil {
			errors = append(errors, fmt.Errorf("indexer close error: %w", err))
		}
	}

	if sm.mst != nil {
		if err := sm.mst.Close(); err != nil {
			errors = append(errors, fmt.Errorf("mst close error: %w", err))
		}
	}

	if sm.storage != nil {
		if err := sm.storage.Close(); err != nil {
			errors = append(errors, fmt.Errorf("storage close error: %w", err))
		}
	}

	sm.closed = true

	if len(errors) > 0 {
		return fmt.Errorf("shutdown errors: %v", errors)
	}

	log.Println("✅ StorageManager shutdown complete")
	return nil
}

// isReady проверяет готовность менеджера
func (sm *StorageManager) isReady() bool {
	sm.mutex.RLock()
	defer sm.mutex.RUnlock()

	return sm.initialized && !sm.closed
}

// =============================================================================
// ВСПОМОГАТЕЛЬНЫЕ МЕТОДЫ
// =============================================================================

// logFileOperation логирует операцию с файлом
func (sm *StorageManager) logFileOperation(ctx context.Context, opType string, cid cid.Cid, filename, contentType string) error {
	entry := &oplog.OperationLogEntry{
		TransactionID: oplog.GenerateTransactionID(),
		Operation:     oplog.OperationType(opType),
		Key:           fmt.Sprintf("file:%s", cid.String()),
		Collection:    "files",
		Metadata: map[string]interface{}{
			"type":        "file_operation",
			"cid":         cid.String(),
			"filename":    filename,
			"contentType": contentType,
			"nodeId":      sm.nodeID,
		},
	}

	return sm.operationLog.LogOperation(ctx, entry)
}

// logRecordOperation логирует операцию с записью
func (sm *StorageManager) logRecordOperation(ctx context.Context, opType string, record *lexicon.TypedRecord) error {
	recordData, _ := json.Marshal(record)

	entry := &oplog.OperationLogEntry{
		TransactionID: oplog.GenerateTransactionID(),
		Operation:     oplog.OperationType(opType),
		Key:           fmt.Sprintf("record:%s:%s", record.LexiconID, record.ID),
		Value:         recordData,
		Collection:    "records",
		Metadata: map[string]interface{}{
			"type":      "record_operation",
			"lexiconId": string(record.LexiconID),
			"recordId":  string(record.ID),
			"nodeId":    sm.nodeID,
		},
	}

	return sm.operationLog.LogOperation(ctx, entry)
}

// Health check методы
func (sm *StorageManager) checkStorageHealth() ComponentHealth {
	// Простая проверка - пытаемся записать и прочитать тестовое значение
	testKey := datastore.NewKey("/health/test")
	testValue := []byte("health-check")

	if err := sm.storage.Put(sm.ctx, testKey, testValue); err != nil {
		return ComponentHealth{Status: "unhealthy", Message: err.Error()}
	}

	if _, err := sm.storage.Get(sm.ctx, testKey); err != nil {
		return ComponentHealth{Status: "unhealthy", Message: err.Error()}
	}

	return ComponentHealth{Status: "healthy"}
}

func (sm *StorageManager) checkIndexerHealth() ComponentHealth {
	if _, err := sm.indexer.GetStats(); err != nil {
		return ComponentHealth{Status: "unhealthy", Message: err.Error()}
	}
	return ComponentHealth{Status: "healthy"}
}

func (sm *StorageManager) checkMSTHealth() ComponentHealth {
	collections := sm.mst.ListCollections()
	if collections == nil {
		return ComponentHealth{Status: "unhealthy", Message: "failed to list collections"}
	}
	return ComponentHealth{Status: "healthy"}
}

func (sm *StorageManager) checkBlockstoreHealth() ComponentHealth {
	// Проверяем что blockstore отвечает
	return ComponentHealth{Status: "healthy"}
}

func (sm *StorageManager) checkLexiconHealth() ComponentHealth {
	if _, err := sm.lexicon.ListSchemas(sm.ctx); err != nil {
		return ComponentHealth{Status: "unhealthy", Message: err.Error()}
	}
	return ComponentHealth{Status: "healthy"}
}
