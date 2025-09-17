package storage

import (
	"fmt"
	"io"
	"sync"
	"time"
)

// =============================================================================
// ОСНОВНЫЕ СТРУКТУРЫ ДАННЫХ
// =============================================================================

// StoredFile информация о сохраненном файле
type StoredFile struct {
	CID      string    `json:"cid"`
	Filename string    `json:"filename"`
	RecordID string    `json:"recordId"`
	Size     int64     `json:"size"`
	MimeType string    `json:"mimeType,omitempty"`
	StoredAt time.Time `json:"storedAt"`
}

// FileMetadata метаданные файла
type FileMetadata struct {
	CID      string                 `json:"cid"`
	Filename string                 `json:"filename,omitempty"`
	MimeType string                 `json:"mimeType,omitempty"`
	Size     int64                  `json:"size,omitempty"`
	Cached   bool                   `json:"cached"`
	Metadata map[string]interface{} `json:"metadata,omitempty"`
}

// SyncResult результат синхронизации с пиром
type SyncResult struct {
	PeerID        string        `json:"peerId"`
	Success       bool          `json:"success"`
	LocalSchemas  int           `json:"localSchemas"`
	RemoteSchemas int           `json:"remoteSchemas"`
	SyncedSchemas int           `json:"syncedSchemas"`
	SyncedRecords int           `json:"syncedRecords"`
	StartTime     time.Time     `json:"startTime"`
	EndTime       time.Time     `json:"endTime"`
	Duration      time.Duration `json:"duration"`
	Error         string        `json:"error,omitempty"`
}

// SystemStats статистика системы
type SystemStats struct {
	NodeID         string                 `json:"nodeId"`
	Timestamp      time.Time              `json:"timestamp"`
	IndexedEntries int64                  `json:"indexedEntries"`
	TotalSize      int64                  `json:"totalSize"`
	TotalSchemas   int                    `json:"totalSchemas"`
	Collections    map[string]int         `json:"collections"`
	ContentTypes   map[string]int64       `json:"contentTypes"`
	CacheHits      int64                  `json:"cacheHits"`
	CacheMisses    int64                  `json:"cacheMisses"`
	Metrics        map[string]interface{} `json:"metrics,omitempty"`
}

// HealthStatus статус здоровья системы
type HealthStatus struct {
	Overall    string                     `json:"overall"`
	Timestamp  time.Time                  `json:"timestamp"`
	Components map[string]ComponentHealth `json:"components"`
}

// ComponentHealth здоровье компонента
type ComponentHealth struct {
	Status    string    `json:"status"` // "healthy", "degraded", "unhealthy"
	Message   string    `json:"message,omitempty"`
	LastCheck time.Time `json:"lastCheck"`
}

// =============================================================================
// MEMORY CACHE
// =============================================================================

// MemoryCache кеш горячих данных в памяти
type MemoryCache struct {
	data    map[string]*CacheEntry
	maxSize int64
	curSize int64
	hits    int64
	misses  int64
	mutex   sync.RWMutex

	// LRU tracking
	head, tail *CacheEntry
}

// CacheEntry элемент кеша
type CacheEntry struct {
	Key        string
	Value      interface{}
	Size       int64
	CreatedAt  time.Time
	LastAccess time.Time

	// LRU pointers
	prev, next *CacheEntry
}

// NewMemoryCache создает новый кеш
func NewMemoryCache(maxSize int64) *MemoryCache {
	mc := &MemoryCache{
		data:    make(map[string]*CacheEntry),
		maxSize: maxSize,
	}

	// Создаем dummy head/tail для упрощения LRU
	mc.head = &CacheEntry{}
	mc.tail = &CacheEntry{}
	mc.head.next = mc.tail
	mc.tail.prev = mc.head

	return mc
}

// Set добавляет элемент в кеш
func (mc *MemoryCache) Set(key string, value interface{}) {
	mc.mutex.Lock()
	defer mc.mutex.Unlock()

	// Оцениваем размер (упрощенно)
	size := mc.estimateSize(value)

	// Удаляем существующий элемент если есть
	if existing, exists := mc.data[key]; exists {
		mc.removeEntry(existing)
	}

	// Освобождаем место если нужно
	for mc.curSize+size > mc.maxSize && mc.tail.prev != mc.head {
		mc.removeEntry(mc.tail.prev)
	}

	// Создаем новый элемент
	entry := &CacheEntry{
		Key:        key,
		Value:      value,
		Size:       size,
		CreatedAt:  time.Now(),
		LastAccess: time.Now(),
	}

	// Добавляем в начало списка
	mc.addToHead(entry)
	mc.data[key] = entry
	mc.curSize += size
}

// Get получает элемент из кеша
func (mc *MemoryCache) Get(key string) (interface{}, bool) {
	mc.mutex.Lock()
	defer mc.mutex.Unlock()

	entry, exists := mc.data[key]
	if !exists {
		mc.misses++
		return nil, false
	}

	// Обновляем время доступа и перемещаем в начало
	entry.LastAccess = time.Now()
	mc.moveToHead(entry)
	mc.hits++

	return entry.Value, true
}

// Delete удаляет элемент из кеша
func (mc *MemoryCache) Delete(key string) {
	mc.mutex.Lock()
	defer mc.mutex.Unlock()

	if entry, exists := mc.data[key]; exists {
		mc.removeEntry(entry)
	}
}

// GetStats возвращает статистику кеша
func (mc *MemoryCache) GetStats() (hits, misses int64) {
	mc.mutex.RLock()
	defer mc.mutex.RUnlock()

	return mc.hits, mc.misses
}

// Cleanup очищает устаревшие элементы
func (mc *MemoryCache) Cleanup() {
	mc.mutex.Lock()
	defer mc.mutex.Unlock()

	cutoff := time.Now().Add(-10 * time.Minute)

	// Идем от хвоста (самые старые элементы)
	current := mc.tail.prev
	for current != mc.head {
		if current.LastAccess.Before(cutoff) {
			next := current.prev
			mc.removeEntry(current)
			current = next
		} else {
			break // Остальные элементы еще свежие
		}
	}
}

// LRU helper methods
func (mc *MemoryCache) addToHead(entry *CacheEntry) {
	entry.next = mc.head.next
	entry.prev = mc.head
	mc.head.next.prev = entry
	mc.head.next = entry
}

func (mc *MemoryCache) removeEntry(entry *CacheEntry) {
	entry.prev.next = entry.next
	entry.next.prev = entry.prev

	delete(mc.data, entry.Key)
	mc.curSize -= entry.Size
}

func (mc *MemoryCache) moveToHead(entry *CacheEntry) {
	entry.prev.next = entry.next
	entry.next.prev = entry.prev
	mc.addToHead(entry)
}

// estimateSize оценивает размер объекта в байтах
func (mc *MemoryCache) estimateSize(value interface{}) int64 {
	switch v := value.(type) {
	case []byte:
		return int64(len(v))
	case string:
		return int64(len(v))
	default:
		return 1024 // По умолчанию 1KB
	}
}

// =============================================================================
// METRICS COLLECTOR
// =============================================================================

// MetricsCollector собирает метрики производительности
type MetricsCollector struct {
	nodeID    string
	metrics   map[string]*OperationMetrics
	mutex     sync.RWMutex
	startTime time.Time
}

// OperationMetrics метрики операции
type OperationMetrics struct {
	Count       int64         `json:"count"`
	TotalTime   time.Duration `json:"totalTime"`
	AverageTime time.Duration `json:"averageTime"`
	MinTime     time.Duration `json:"minTime"`
	MaxTime     time.Duration `json:"maxTime"`
	LastOpTime  time.Time     `json:"lastOpTime"`
}

// NewMetricsCollector создает новый сборщик метрик
func NewMetricsCollector(nodeID string) *MetricsCollector {
	return &MetricsCollector{
		nodeID:    nodeID,
		metrics:   make(map[string]*OperationMetrics),
		startTime: time.Now(),
	}
}

// RecordOperation записывает метрику операции
func (mc *MetricsCollector) RecordOperation(operation string, duration time.Duration) {
	mc.mutex.Lock()
	defer mc.mutex.Unlock()

	metric, exists := mc.metrics[operation]
	if !exists {
		metric = &OperationMetrics{
			MinTime: duration,
			MaxTime: duration,
		}
		mc.metrics[operation] = metric
	}

	metric.Count++
	metric.TotalTime += duration
	metric.AverageTime = metric.TotalTime / time.Duration(metric.Count)
	metric.LastOpTime = time.Now()

	if duration < metric.MinTime {
		metric.MinTime = duration
	}
	if duration > metric.MaxTime {
		metric.MaxTime = duration
	}
}

// GetSummary возвращает сводку метрик
func (mc *MetricsCollector) GetSummary() map[string]interface{} {
	mc.mutex.RLock()
	defer mc.mutex.RUnlock()

	summary := map[string]interface{}{
		"nodeId":     mc.nodeID,
		"uptime":     time.Since(mc.startTime),
		"operations": make(map[string]OperationMetrics),
	}

	for op, metrics := range mc.metrics {
		summary["operations"].(map[string]OperationMetrics)[op] = *metrics
	}

	return summary
}

// Collect собирает текущие метрики
func (mc *MetricsCollector) Collect() map[string]interface{} {
	// return mc.GetSummary()
	return  nil
}

// =============================================================================
// IO UTILITIES
// =============================================================================

// bytesReadSeekCloser реализует io.ReadSeekCloser для []byte
type bytesReadSeekCloser struct {
	data []byte
	pos  int64
}

// newBytesReadSeekCloser создает ReadSeekCloser из байтов
func newBytesReadSeekCloser(data []byte) io.ReadSeekCloser {
	return &bytesReadSeekCloser{data: data}
}

func (b *bytesReadSeekCloser) Read(p []byte) (int, error) {
	if b.pos >= int64(len(b.data)) {
		return 0, io.EOF
	}

	n := copy(p, b.data[b.pos:])
	b.pos += int64(n)
	return n, nil
}

func (b *bytesReadSeekCloser) Seek(offset int64, whence int) (int64, error) {
	var newPos int64

	switch whence {
	case io.SeekStart:
		newPos = offset
	case io.SeekCurrent:
		newPos = b.pos + offset
	case io.SeekEnd:
		newPos = int64(len(b.data)) + offset
	default:
		return 0, fmt.Errorf("invalid whence value")
	}

	if newPos < 0 {
		return 0, fmt.Errorf("negative seek position")
	}

	b.pos = newPos
	return newPos, nil
}

func (b *bytesReadSeekCloser) Close() error {
	return nil
}

// =============================================================================
// ERROR TYPES
// =============================================================================

// StorageError специализированная ошибка хранилища
type StorageError struct {
	Operation string
	Component string
	Err       error
}

func (e *StorageError) Error() string {
	return fmt.Sprintf("storage error in %s.%s: %v", e.Component, e.Operation, e.Err)
}

func (e *StorageError) Unwrap() error {
	return e.Err
}

// NewStorageError создает новую ошибку хранилища
func NewStorageError(component, operation string, err error) *StorageError {
	return &StorageError{
		Component: component,
		Operation: operation,
		Err:       err,
	}
}

// =============================================================================
// CONFIGURATION HELPERS
// =============================================================================

// DefaultConfig возвращает конфигурацию по умолчанию
func DefaultConfig() Config {
	return Config{
		DataDir:           "./data",
		StoragePath:       "./data/storage",
		IndexPath:         "./data/index.db",
		NodeID:            fmt.Sprintf("ues-node-%d", time.Now().UnixNano()),
		MemoryCacheSizeMB: 64,
		MaxConcurrentOps:  100,
		SyncInterval:      "5m",
		EnableAutoCompact: true,
		CompactInterval:   "1h",
		EnableMetrics:     true,
	}
}

// ValidateConfig проверяет корректность конфигурации
func ValidateConfig(config Config) error {
	if config.NodeID == "" {
		return fmt.Errorf("node ID is required")
	}

	if config.DataDir == "" {
		return fmt.Errorf("data directory is required")
	}

	if config.MemoryCacheSizeMB <= 0 {
		return fmt.Errorf("memory cache size must be positive")
	}

	if config.MaxConcurrentOps <= 0 {
		return fmt.Errorf("max concurrent operations must be positive")
	}

	return nil
}
