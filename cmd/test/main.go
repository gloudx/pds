// main.go - UES Node с централизованным StorageManager
package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"os/signal"
	"pds/lexicon"
	"pds/storage"
	"strings"
	"syscall"
	"time"
)

// UESNode представляет узел UES с централизованным управлением хранением
type UESNode struct {
	// Единый менеджер хранения - центральная точка управления
	StorageManager *storage.StorageManager

	// Конфигурация узла
	Config *UESConfig
	NodeID string

	// Управление жизненным циклом
	ctx    context.Context
	cancel context.CancelFunc
}

// UESConfig конфигурация узла UES
type UESConfig struct {
	// Базовые настройки
	DataDir     string `json:"dataDir"`
	StoragePath string `json:"storagePath"`
	IndexPath   string `json:"indexPath"`
	NodeID      string `json:"nodeId"`

	// Производительность
	MemoryCacheSizeMB int    `json:"memoryCacheSizeMb"`
	MaxConcurrentOps  int    `json:"maxConcurrentOps"`
	SyncInterval      string `json:"syncInterval"`

	// Автоматические задачи
	EnableAutoCompact bool   `json:"enableAutoCompact"`
	CompactInterval   string `json:"compactInterval"`
	EnableMetrics     bool   `json:"enableMetrics"`

	// Сетевые настройки (для будущего развития)
	ListenAddr string   `json:"listenAddr"`
	PeerAddrs  []string `json:"peerAddrs"`

	// Демо режим
	RunDemo    bool   `json:"runDemo"`
	SchemaDir  string `json:"schemaDir"`
	AutoImport bool   `json:"autoImport"`
}

// NewUESNode создает новый узел UES
func NewUESNode(configPath string) (*UESNode, error) {
	// Загружаем конфигурацию
	config, err := LoadConfig(configPath)
	if err != nil {
		return nil, fmt.Errorf("failed to load config: %w", err)
	}

	ctx, cancel := context.WithCancel(context.Background())

	node := &UESNode{
		Config: config,
		NodeID: config.NodeID,
		ctx:    ctx,
		cancel: cancel,
	}

	// Создаем конфигурацию для StorageManager
	storageConfig := storage.Config{
		DataDir:           config.DataDir,
		StoragePath:       config.StoragePath,
		IndexPath:         config.IndexPath,
		NodeID:            config.NodeID,
		MemoryCacheSizeMB: config.MemoryCacheSizeMB,
		MaxConcurrentOps:  config.MaxConcurrentOps,
		SyncInterval:      config.SyncInterval,
		EnableAutoCompact: config.EnableAutoCompact,
		CompactInterval:   config.CompactInterval,
		EnableMetrics:     config.EnableMetrics,
	}

	// Инициализируем единый StorageManager
	log.Println("🚀 Initializing UES Storage Manager...")
	node.StorageManager, err = storage.NewStorageManager(storageConfig)
	if err != nil {
		cancel()
		return nil, fmt.Errorf("failed to create storage manager: %w", err)
	}

	log.Printf("✅ UES Node initialized successfully with ID: %s", node.NodeID)
	return node, nil
}

// Start запускает узел UES
func (node *UESNode) Start(ctx context.Context) error {
	log.Println("🚀 Starting UES node...")

	// Запускаем основные сервисы
	go node.startMainServices(ctx)

	// Запускаем демо сценарий если включен
	if node.Config.RunDemo {
		if err := node.runDemoScenario(ctx); err != nil {
			log.Printf("⚠️  Demo scenario failed: %v", err)
		}
	}

	log.Println("✅ UES node started successfully")
	return nil
}

// Stop корректно останавливает узел
func (node *UESNode) Stop() error {
	log.Println("🛑 Stopping UES node...")

	// Отменяем контекст
	node.cancel()

	// Закрываем StorageManager
	if node.StorageManager != nil {
		if err := node.StorageManager.Close(); err != nil {
			log.Printf("Error closing storage manager: %v", err)
			return err
		}
	}

	log.Println("✅ UES node stopped")
	return nil
}

// startMainServices запускает основные сервисы узла
func (node *UESNode) startMainServices(ctx context.Context) {
	ticker := time.NewTicker(30 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			node.logSystemHealth(ctx)
		}
	}
}

// runDemoScenario запускает демонстрационный сценарий
func (node *UESNode) runDemoScenario(ctx context.Context) error {
	log.Println("\n🎭 Running UES Storage Manager Demo")
	log.Println("=" + strings.Repeat("=", 45))

	sm := node.StorageManager

	// 1. Создаем схему для блога через StorageManager
	log.Println("📋 1. Creating blog schema...")

	blogSchema := &lexicon.LexiconDefinition{
		ID:          "demo.blog.post",
		Version:     "1.0.0",
		Name:        "Blog Post Demo",
		Description: "Демонстрационная схема блог поста",
		Schema: map[string]interface{}{
			"type": "object",
			"properties": map[string]interface{}{
				"title": map[string]interface{}{
					"type":      "string",
					"maxLength": 200,
				},
				"content": map[string]interface{}{
					"type":      "string",
					"maxLength": 50000,
				},
				"author": map[string]interface{}{
					"type": "string",
				},
				"category": map[string]interface{}{
					"type": "string",
					"enum": []interface{}{"tech", "lifestyle", "business"},
				},
				"tags": map[string]interface{}{
					"type": "array",
					"items": map[string]interface{}{
						"type": "string",
					},
				},
				"published": map[string]interface{}{
					"type": "boolean",
				},
			},
			"required": []interface{}{"title", "content", "author"},
		},
		Indexes: []lexicon.IndexDefinition{
			{
				Name:   "content_search",
				Fields: []string{"title", "content"},
				Type:   "fts",
			},
		},
	}

	if err := sm.RegisterSchema(ctx, blogSchema); err != nil {
		return fmt.Errorf("failed to register schema: %w", err)
	}
	log.Printf("✅ Registered schema: %s", blogSchema.ID)

	// 2. Создаем записи блог постов
	log.Println("📝 2. Creating blog posts...")
	blogPosts := []map[string]interface{}{
		{
			"title":     "StorageManager - центр UES системы",
			"content":   "StorageManager объединяет все компоненты UES в единое целое, предоставляя простой API для работы с данными. Он координирует MST, индексатор, blockstore и lexicon system.",
			"author":    "ues-architect",
			"category":  "tech",
			"tags":      []interface{}{"storage", "architecture", "ues"},
			"published": true,
		},
		{
			"title":     "Производительность и кеширование",
			"content":   "Встроенный memory cache обеспечивает высокую производительность доступа к часто используемым данным. LRU алгоритм автоматически управляет памятью.",
			"author":    "performance-engineer",
			"category":  "tech",
			"tags":      []interface{}{"performance", "cache", "optimization"},
			"published": true,
		},
		{
			"title":     "Мониторинг и метрики",
			"content":   "Система автоматически собирает метрики всех операций, предоставляя детальную статистику для анализа производительности и диагностики проблем.",
			"author":    "ops-engineer",
			"category":  "business",
			"tags":      []interface{}{"monitoring", "metrics", "ops"},
			"published": false,
		},
	}

	var createdRecords []*lexicon.TypedRecord
	for i, postData := range blogPosts {
		record, err := sm.CreateTypedRecord(ctx, "demo.blog.post", postData, fmt.Sprintf("demo-user-%d", i))
		if err != nil {
			log.Printf("Failed to create post %d: %v", i, err)
			continue
		}
		log.Printf("✅ Created post: %s - %s", record.ID, postData["title"])
		createdRecords = append(createdRecords, record)
	}

	fmt.Println(createdRecords)

	// 3. Сохраняем медиа файлы
	log.Println("🖼️  3. Storing media files...")
	testFiles := []struct {
		name     string
		content  string
		mimeType string
	}{
		{"architecture.md", "# UES Architecture\n\nDetailed architecture documentation...", "text/markdown"},
		{"demo.json", `{"demo": true, "timestamp": "` + time.Now().Format(time.RFC3339) + `"}`, "application/json"},
		{"config.yaml", "# UES Config\nversion: 1.0\ncomponents: [storage, mst, indexer]", "text/yaml"},
	}

	var storedFiles []*storage.StoredFile
	for _, file := range testFiles {
		storedFile, err := sm.StoreFile(ctx, file.name, strings.NewReader(file.content), file.mimeType)
		if err != nil {
			log.Printf("Failed to store file %s: %v", file.name, err)
			continue
		}
		storedFiles = append(storedFiles, storedFile)
		log.Printf("✅ Stored file: %s → %s", file.name, storedFile.CID)
	}

	// 4. Демонстрируем поиск
	log.Println("🔍 4. Searching records and files...")

	// Поиск записей по категории
	filter := &lexicon.QueryFilter{
		LexiconID: "demo.blog.post",
		Fields: map[string]interface{}{
			"category": "tech",
		},
		Limit: 10,
	}

	techPosts, err := sm.QueryRecords(ctx, filter)
	if err == nil {
		log.Printf("📊 Found %d tech posts", len(techPosts.Records))
	}

	// Полнотекстовый поиск
	searchResults, err := sm.SearchFiles(ctx, "StorageManager", 10)
	if err == nil {
		log.Printf("📊 Full-text search found %d results", len(searchResults))
	}

	// 5. Получаем файл обратно
	if len(storedFiles) > 0 {
		log.Println("📁 5. Retrieving stored file...")
		firstFile := storedFiles[0]

		reader, metadata, err := sm.GetFile(ctx, firstFile.CID)
		if err == nil {
			defer reader.Close()
			log.Printf("✅ Retrieved file: %s (cached: %t)", metadata.Filename, metadata.Cached)
		}
	}

	// 6. Показываем статистику системы
	log.Println("📊 6. System statistics...")
	stats, err := sm.GetSystemStats(ctx)
	if err == nil {
		log.Printf("📈 System Stats:")
		log.Printf("   • Node ID: %s", stats.NodeID)
		log.Printf("   • Indexed entries: %d", stats.IndexedEntries)
		log.Printf("   • Total size: %d bytes", stats.TotalSize)
		log.Printf("   • Schemas: %d", stats.TotalSchemas)
		log.Printf("   • Cache hits: %d, misses: %d", stats.CacheHits, stats.CacheMisses)

		if stats.Collections != nil {
			log.Printf("   • Collections:")
			for name, count := range stats.Collections {
				log.Printf("     - %s: %d items", name, count)
			}
		}
	}

	// 7. Проверяем здоровье системы
	log.Println("🏥 7. Health check...")
	health := sm.GetHealthStatus(ctx)
	log.Printf("🩺 Overall health: %s", health.Overall)

	for component, status := range health.Components {
		statusIcon := "✅"
		if status.Status != "healthy" {
			statusIcon = "⚠️"
		}
		log.Printf("   %s %s: %s", statusIcon, component, status.Status)
		if status.Message != "" {
			log.Printf("     Message: %s", status.Message)
		}
	}

	// 8. Демонстрируем синхронизацию (mock)
	log.Println("🔄 8. Sync demonstration...")
	syncResult, err := sm.SyncWithPeer(ctx, "demo-peer-123")
	if err == nil {
		log.Printf("🔄 Sync with peer completed:")
		log.Printf("   • Peer ID: %s", syncResult.PeerID)
		log.Printf("   • Local schemas: %d", syncResult.LocalSchemas)
		log.Printf("   • Success: %t", syncResult.Success)
	}

	log.Println("\n🎉 StorageManager demo completed successfully!")
	log.Println("✨ All UES components working through unified API!")

	return nil
}

// logSystemHealth логирует здоровье системы
func (node *UESNode) logSystemHealth(ctx context.Context) {
	if stats, err := node.StorageManager.GetSystemStats(ctx); err == nil {
		log.Printf("💓 Node %s - Entries: %d, Cache: %d hits/%d misses",
			stats.NodeID, stats.IndexedEntries, stats.CacheHits, stats.CacheMisses)
	}
}

// LoadConfig загружает конфигурацию из файла
func LoadConfig(configPath string) (*UESConfig, error) {
	// Если файл не существует, создаем конфигурацию по умолчанию
	if _, err := os.Stat(configPath); os.IsNotExist(err) {
		defaultConfig := &UESConfig{
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
			ListenAddr:        "127.0.0.1:8080",
			RunDemo:           true,
			SchemaDir:         "./schemas",
			AutoImport:        true,
		}

		// Создаем директории
		os.MkdirAll(defaultConfig.DataDir, 0755)
		if defaultConfig.SchemaDir != "" {
			os.MkdirAll(defaultConfig.SchemaDir, 0755)
		}

		// Сохраняем конфигурацию по умолчанию
		if err := SaveConfig(configPath, defaultConfig); err != nil {
			return nil, fmt.Errorf("failed to save default config: %w", err)
		}

		return defaultConfig, nil
	}

	// Загружаем существующую конфигурацию
	file, err := os.ReadFile(configPath)
	if err != nil {
		return nil, fmt.Errorf("failed to read config file: %w", err)
	}

	var config UESConfig
	if err := json.Unmarshal(file, &config); err != nil {
		return nil, fmt.Errorf("failed to parse config JSON: %w", err)
	}

	return &config, nil
}

// SaveConfig сохраняет конфигурацию в файл
func SaveConfig(configPath string, config *UESConfig) error {
	data, err := json.MarshalIndent(config, "", "  ")
	if err != nil {
		return fmt.Errorf("failed to marshal config: %w", err)
	}

	return os.WriteFile(configPath, data, 0644)
}

// main функция - точка входа
func main() {
	log.Println("🌟 Starting Universal Entity Streams (UES) Node v2.0")
	log.Println("🔧 With centralized StorageManager architecture")

	// Определяем путь к конфигурации
	configPath := "./config.json"
	if len(os.Args) > 1 {
		configPath = os.Args[1]
	}

	// Создаем узел UES
	node, err := NewUESNode(configPath)
	if err != nil {
		log.Fatalf("Failed to create UES node: %v", err)
	}

	// Настраиваем корректное завершение работы
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Обработка сигналов завершения
	signalChan := make(chan os.Signal, 1)
	signal.Notify(signalChan, os.Interrupt, syscall.SIGTERM)

	go func() {
		<-signalChan
		log.Println("🛑 Shutdown signal received")
		cancel()
	}()

	// Запускаем узел
	if err := node.Start(ctx); err != nil {
		log.Fatalf("Failed to start UES node: %v", err)
	}

	// Ждем завершения
	<-ctx.Done()

	// Корректно останавливаем узел
	if err := node.Stop(); err != nil {
		log.Printf("Error during shutdown: %v", err)
		os.Exit(1)
	}

	log.Println("👋 UES node shutdown complete - StorageManager architecture rocks! 🚀")
}
