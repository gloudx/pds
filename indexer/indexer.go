package indexer

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"path/filepath"
	s "pds/store"
	"strings"
	"sync"
	"time"

	"github.com/ipfs/go-datastore/query"
	_ "github.com/mattn/go-sqlite3"
)

// IndexEntry представляет индексируемую запись
type IndexEntry struct {
	Key         string                 `json:"key"`
	ContentType string                 `json:"content_type"`
	Size        int64                  `json:"size"`
	CreatedAt   time.Time              `json:"created_at"`
	UpdatedAt   time.Time              `json:"updated_at"`
	Metadata    map[string]interface{} `json:"metadata"`
	Content     string                 `json:"content"` // Текстовое содержимое для FTS
	DataHash    string                 `json:"data_hash"`
}

// SearchResult результат поиска
type SearchResult struct {
	Key         string                 `json:"key"`
	ContentType string                 `json:"content_type"`
	Size        int64                  `json:"size"`
	CreatedAt   time.Time              `json:"created_at"`
	UpdatedAt   time.Time              `json:"updated_at"`
	Metadata    map[string]interface{} `json:"metadata"`
	Snippet     string                 `json:"snippet"`
	Score       float64                `json:"score"`
}

// SQLiteIndexer индексатор данных MST с помощью SQLite
type Indexer struct {
	db        *sql.DB
	mu        sync.RWMutex
	watchers  map[string]chan IndexEvent
	datastore s.Storage
}

// IndexEvent событие изменения индекса
type IndexEvent struct {
	Type string // "put", "delete"
	Key  string
	Data []byte
}

// NewIndexer создает новый SQLite индексатор
func NewIndexer(dbPath string, ds s.Storage) (*Indexer, error) {

	db, err := sql.Open("sqlite3", fmt.Sprintf("%s?_journal_mode=WAL&_synchronous=NORMAL&_cache_size=10000&_foreign_keys=ON", dbPath))
	if err != nil {
		return nil, fmt.Errorf("failed to open sqlite: %w", err)
	}

	db.SetMaxOpenConns(10)
	db.SetMaxIdleConns(5)
	db.SetConnMaxLifetime(time.Hour)

	indexer := &Indexer{
		db:        db,
		watchers:  make(map[string]chan IndexEvent),
		datastore: ds,
	}

	if err := indexer.createSchema(); err != nil {
		return nil, fmt.Errorf("failed to create schema: %w", err)
	}

	return indexer, nil
}

// createSchema создает необходимые таблицы и индексы
func (idx *Indexer) createSchema() error {
	queries := []string{
		// Основная таблица для метаданных
		`CREATE TABLE IF NOT EXISTS entries (
			key TEXT PRIMARY KEY,
			content_type TEXT NOT NULL,
			size INTEGER NOT NULL DEFAULT 0,
			created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
			updated_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
			metadata JSON,
			data_hash TEXT NOT NULL,
			UNIQUE(key)
		)`,

		// FTS5 таблица для полнотекстового поиска
		`CREATE VIRTUAL TABLE IF NOT EXISTS entries_fts USING fts5(
			key UNINDEXED,
			content,
			content='entries',
			content_rowid='rowid'
		)`,

		// Триггеры для автоматического обновления FTS
		`CREATE TRIGGER IF NOT EXISTS entries_fts_insert AFTER INSERT ON entries BEGIN
			INSERT INTO entries_fts(rowid, key, content) 
			VALUES (new.rowid, new.key, COALESCE(new.metadata->>'$.content', ''));
		END`,

		`CREATE TRIGGER IF NOT EXISTS entries_fts_delete AFTER DELETE ON entries BEGIN
			INSERT INTO entries_fts(entries_fts, rowid, key, content) 
			VALUES('delete', old.rowid, old.key, COALESCE(old.metadata->>'$.content', ''));
		END`,

		`CREATE TRIGGER IF NOT EXISTS entries_fts_update AFTER UPDATE ON entries BEGIN
			INSERT INTO entries_fts(entries_fts, rowid, key, content) 
			VALUES('delete', old.rowid, old.key, COALESCE(old.metadata->>'$.content', ''));
			INSERT INTO entries_fts(rowid, key, content) 
			VALUES (new.rowid, new.key, COALESCE(new.metadata->>'$.content', ''));
		END`,

		// Индексы для быстрого поиска
		`CREATE INDEX IF NOT EXISTS idx_entries_content_type ON entries(content_type)`,
		`CREATE INDEX IF NOT EXISTS idx_entries_created_at ON entries(created_at DESC)`,
		`CREATE INDEX IF NOT EXISTS idx_entries_updated_at ON entries(updated_at DESC)`,
		`CREATE INDEX IF NOT EXISTS idx_entries_size ON entries(size)`,
		`CREATE INDEX IF NOT EXISTS idx_entries_data_hash ON entries(data_hash)`,

		// Таблица для тэгов (многие ко многим)
		`CREATE TABLE IF NOT EXISTS tags (
			id INTEGER PRIMARY KEY AUTOINCREMENT,
			name TEXT NOT NULL UNIQUE
		)`,

		`CREATE TABLE IF NOT EXISTS entry_tags (
			entry_key TEXT NOT NULL,
			tag_id INTEGER NOT NULL,
			PRIMARY KEY (entry_key, tag_id),
			FOREIGN KEY (entry_key) REFERENCES entries(key) ON DELETE CASCADE,
			FOREIGN KEY (tag_id) REFERENCES tags(id) ON DELETE CASCADE
		)`,

		`CREATE INDEX IF NOT EXISTS idx_entry_tags_key ON entry_tags(entry_key)`,
		`CREATE INDEX IF NOT EXISTS idx_entry_tags_tag ON entry_tags(tag_id)`,
	}

	for _, query := range queries {
		if _, err := idx.db.Exec(query); err != nil {
			return fmt.Errorf("failed to execute query %s: %w", query, err)
		}
	}

	return nil
}

// IndexEntry добавляет или обновляет запись в индексе
func (idx *Indexer) IndexEntry(entry *IndexEntry) error {
	idx.mu.Lock()
	defer idx.mu.Unlock()

	tx, err := idx.db.Begin()
	if err != nil {
		return err
	}
	defer tx.Rollback()

	metadataJSON, err := json.Marshal(entry.Metadata)
	if err != nil {
		return fmt.Errorf("failed to marshal metadata: %w", err)
	}

	_, err = tx.Exec(`
		INSERT INTO entries (key, content_type, size, created_at, updated_at, metadata, data_hash)
		VALUES (?, ?, ?, ?, ?, ?, ?)
		ON CONFLICT(key) DO UPDATE SET
			content_type = excluded.content_type,
			size = excluded.size,
			updated_at = excluded.updated_at,
			metadata = excluded.metadata,
			data_hash = excluded.data_hash
	`, entry.Key, entry.ContentType, entry.Size, entry.CreatedAt, entry.UpdatedAt, string(metadataJSON), entry.DataHash)

	if err != nil {
		return err
	}

	return tx.Commit()
}

// RemoveEntry удаляет запись из индекса
func (idx *Indexer) RemoveEntry(key string) error {
	idx.mu.Lock()
	defer idx.mu.Unlock()

	_, err := idx.db.Exec("DELETE FROM entries WHERE key = ?", key)
	return err
}

// SearchText выполняет полнотекстовый поиск
func (idx *Indexer) SearchText(query string, limit, offset int) ([]*SearchResult, error) {
	idx.mu.RLock()
	defer idx.mu.RUnlock()

	// Подготавливаем FTS запрос
	ftsQuery := strings.ReplaceAll(query, "'", "''")

	rows, err := idx.db.Query(`
		SELECT 
			e.key, e.content_type, e.size, e.created_at, e.updated_at, 
			e.metadata, snippet(entries_fts, 1, '<mark>', '</mark>', '...', 32) as snippet,
			rank as score
		FROM entries_fts 
		JOIN entries e ON e.key = entries_fts.key
		WHERE entries_fts MATCH ?
		ORDER BY rank
		LIMIT ? OFFSET ?
	`, ftsQuery, limit, offset)

	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var results []*SearchResult
	for rows.Next() {
		result := &SearchResult{}
		var metadataJSON string

		err := rows.Scan(
			&result.Key, &result.ContentType, &result.Size,
			&result.CreatedAt, &result.UpdatedAt,
			&metadataJSON, &result.Snippet, &result.Score,
		)
		if err != nil {
			return nil, err
		}

		if err := json.Unmarshal([]byte(metadataJSON), &result.Metadata); err != nil {
			result.Metadata = make(map[string]interface{})
		}

		results = append(results, result)
	}

	return results, nil
}

// SearchByMetadata поиск по JSON метаданным
func (idx *Indexer) SearchByMetadata(jsonPath, value string, limit, offset int) ([]*SearchResult, error) {
	idx.mu.RLock()
	defer idx.mu.RUnlock()

	rows, err := idx.db.Query(`
		SELECT 
			key, content_type, size, created_at, updated_at, metadata
		FROM entries 
		WHERE json_extract(metadata, ?) = ?
		ORDER BY updated_at DESC
		LIMIT ? OFFSET ?
	`, jsonPath, value, limit, offset)

	if err != nil {
		return nil, err
	}
	defer rows.Close()

	return idx.scanSearchResults(rows)
}

// SearchByContentType поиск по типу контента
func (idx *Indexer) SearchByContentType(contentType string, limit, offset int) ([]*SearchResult, error) {
	idx.mu.RLock()
	defer idx.mu.RUnlock()

	pattern := contentType
	if !strings.Contains(pattern, "%") {
		pattern = pattern + "%"
	}

	rows, err := idx.db.Query(`
		SELECT 
			key, content_type, size, created_at, updated_at, metadata
		FROM entries 
		WHERE content_type LIKE ?
		ORDER BY updated_at DESC
		LIMIT ? OFFSET ?
	`, pattern, limit, offset)

	if err != nil {
		return nil, err
	}
	defer rows.Close()

	return idx.scanSearchResults(rows)
}

// SearchByDateRange поиск по диапазону дат
func (idx *Indexer) SearchByDateRange(from, to time.Time, limit, offset int) ([]*SearchResult, error) {
	idx.mu.RLock()
	defer idx.mu.RUnlock()

	rows, err := idx.db.Query(`
		SELECT 
			key, content_type, size, created_at, updated_at, metadata
		FROM entries 
		WHERE created_at BETWEEN ? AND ?
		ORDER BY created_at DESC
		LIMIT ? OFFSET ?
	`, from, to, limit, offset)

	if err != nil {
		return nil, err
	}
	defer rows.Close()

	return idx.scanSearchResults(rows)
}

// SearchByTags поиск по тегам
func (idx *Indexer) SearchByTags(tags []string, matchAll bool, limit, offset int) ([]*SearchResult, error) {
	idx.mu.RLock()
	defer idx.mu.RUnlock()

	if len(tags) == 0 {
		return []*SearchResult{}, nil
	}

	// Строим динамический запрос
	placeholders := strings.Repeat("?,", len(tags))
	placeholders = placeholders[:len(placeholders)-1]

	var query string
	if matchAll {
		// Требуем все теги (AND)
		query = fmt.Sprintf(`
			SELECT 
				e.key, e.content_type, e.size, e.created_at, e.updated_at, e.metadata
			FROM entries e
			WHERE e.key IN (
				SELECT et.entry_key
				FROM entry_tags et
				JOIN tags t ON et.tag_id = t.id
				WHERE t.name IN (%s)
				GROUP BY et.entry_key
				HAVING COUNT(DISTINCT t.id) = ?
			)
			ORDER BY e.updated_at DESC
			LIMIT ? OFFSET ?
		`, placeholders)
	} else {
		// Любой тег (OR)
		query = fmt.Sprintf(`
			SELECT DISTINCT
				e.key, e.content_type, e.size, e.created_at, e.updated_at, e.metadata
			FROM entries e
			JOIN entry_tags et ON e.key = et.entry_key
			JOIN tags t ON et.tag_id = t.id
			WHERE t.name IN (%s)
			ORDER BY e.updated_at DESC
			LIMIT ? OFFSET ?
		`, placeholders)
	}

	args := make([]interface{}, 0, len(tags)+3)
	for _, tag := range tags {
		args = append(args, tag)
	}
	if matchAll {
		args = append(args, len(tags))
	}
	args = append(args, limit, offset)

	rows, err := idx.db.Query(query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	return idx.scanSearchResults(rows)
}

// AddTags добавляет теги к записи
func (idx *Indexer) AddTags(key string, tags []string) error {
	if len(tags) == 0 {
		return nil
	}

	idx.mu.Lock()
	defer idx.mu.Unlock()

	tx, err := idx.db.Begin()
	if err != nil {
		return err
	}
	defer tx.Rollback()

	for _, tag := range tags {
		// Вставляем тег если не существует
		_, err = tx.Exec("INSERT OR IGNORE INTO tags (name) VALUES (?)", tag)
		if err != nil {
			return err
		}

		// Связываем с записью
		_, err = tx.Exec(`
			INSERT OR IGNORE INTO entry_tags (entry_key, tag_id)
			SELECT ?, id FROM tags WHERE name = ?
		`, key, tag)
		if err != nil {
			return err
		}
	}

	return tx.Commit()
}

// RemoveTags удаляет теги у записи
func (idx *Indexer) RemoveTags(key string, tags []string) error {
	if len(tags) == 0 {
		return nil
	}

	idx.mu.Lock()
	defer idx.mu.Unlock()

	placeholders := strings.Repeat("?,", len(tags))
	placeholders = placeholders[:len(placeholders)-1]

	args := make([]interface{}, 0, len(tags)+1)
	args = append(args, key)
	for _, tag := range tags {
		args = append(args, tag)
	}

	query := fmt.Sprintf(`
		DELETE FROM entry_tags 
		WHERE entry_key = ? AND tag_id IN (
			SELECT id FROM tags WHERE name IN (%s)
		)
	`, placeholders)

	_, err := idx.db.Exec(query, args...)
	return err
}

// GetStats возвращает статистику индекса
func (idx *Indexer) GetStats() (map[string]interface{}, error) {
	idx.mu.RLock()
	defer idx.mu.RUnlock()

	stats := make(map[string]interface{})

	// Общее количество записей
	var totalEntries int64
	err := idx.db.QueryRow("SELECT COUNT(*) FROM entries").Scan(&totalEntries)
	if err != nil {
		return nil, err
	}
	stats["total_entries"] = totalEntries

	// Статистика по типам контента
	rows, err := idx.db.Query(`
		SELECT content_type, COUNT(*) 
		FROM entries 
		GROUP BY content_type 
		ORDER BY COUNT(*) DESC
	`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	contentTypes := make(map[string]int64)
	for rows.Next() {
		var contentType string
		var count int64
		if err := rows.Scan(&contentType, &count); err != nil {
			return nil, err
		}
		contentTypes[contentType] = count
	}
	stats["content_types"] = contentTypes

	// Общий размер
	var totalSize int64
	err = idx.db.QueryRow("SELECT COALESCE(SUM(size), 0) FROM entries").Scan(&totalSize)
	if err != nil {
		return nil, err
	}
	stats["total_size"] = totalSize

	// Количество тегов
	var totalTags int64
	err = idx.db.QueryRow("SELECT COUNT(*) FROM tags").Scan(&totalTags)
	if err != nil {
		return nil, err
	}
	stats["total_tags"] = totalTags

	return stats, nil
}

// WatchIndex создает канал для наблюдения за изменениями
func (idx *Indexer) WatchIndex(ctx context.Context) <-chan IndexEvent {
	idx.mu.Lock()
	defer idx.mu.Unlock()

	eventChan := make(chan IndexEvent, 100)
	watcherID := fmt.Sprintf("watcher_%d", time.Now().UnixNano())
	idx.watchers[watcherID] = eventChan

	go func() {
		<-ctx.Done()
		idx.mu.Lock()
		delete(idx.watchers, watcherID)
		close(eventChan)
		idx.mu.Unlock()
	}()

	return eventChan
}

// SyncFromDatastore синхронизирует индекс с datastore
func (idx *Indexer) SyncFromDatastore(ctx context.Context) error {
	results, err := idx.datastore.Query(ctx, query.Query{})
	if err != nil {
		return err
	}
	defer results.Close()

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		result, ok := results.NextSync()
		if !ok {
			break
		}

		if result.Error != nil {
			return result.Error
		}

		entry, err := idx.analyzeData(result.Key, result.Value)
		if err != nil {
			continue // Пропускаем проблемные записи
		}

		if err := idx.IndexEntry(entry); err != nil {
			return fmt.Errorf("failed to index entry %s: %w", result.Key, err)
		}
	}

	return nil
}

// Close закрывает индексатор
func (idx *Indexer) Close() error {
	idx.mu.Lock()
	defer idx.mu.Unlock()

	for _, ch := range idx.watchers {
		close(ch)
	}
	idx.watchers = make(map[string]chan IndexEvent)

	return idx.db.Close()
}

// Вспомогательные методы

func (idx *Indexer) scanSearchResults(rows *sql.Rows) ([]*SearchResult, error) {
	var results []*SearchResult

	for rows.Next() {
		result := &SearchResult{}
		var metadataJSON string

		err := rows.Scan(
			&result.Key, &result.ContentType, &result.Size,
			&result.CreatedAt, &result.UpdatedAt, &metadataJSON,
		)
		if err != nil {
			return nil, err
		}

		if err := json.Unmarshal([]byte(metadataJSON), &result.Metadata); err != nil {
			result.Metadata = make(map[string]interface{})
		}

		results = append(results, result)
	}

	return results, nil
}

func (idx *Indexer) analyzeData(key string, data []byte) (*IndexEntry, error) {
	entry := &IndexEntry{
		Key:       key,
		Size:      int64(len(data)),
		CreatedAt: time.Now(),
		UpdatedAt: time.Now(),
		DataHash:  fmt.Sprintf("%x", data[:min(32, len(data))]), // Простой хеш для демо
	}

	// Пытаемся определить тип контента
	entry.ContentType = idx.detectContentType(data)

	// Создаем базовые метаданные
	metadata := make(map[string]interface{})
	metadata["size"] = entry.Size
	metadata["extension"] = filepath.Ext(key)

	// Если это JSON, парсим его
	if entry.ContentType == "application/json" {
		var jsonData interface{}
		if err := json.Unmarshal(data, &jsonData); err == nil {
			metadata["json_data"] = jsonData
			// Добавляем в контент для FTS
			if str, ok := jsonData.(string); ok {
				metadata["content"] = str
			}
		}
	} else if strings.HasPrefix(entry.ContentType, "text/") {
		// Текстовые файлы добавляем в контент для FTS
		metadata["content"] = string(data)
	}

	entry.Metadata = metadata
	return entry, nil
}

func (idx *Indexer) detectContentType(data []byte) string {
	if len(data) == 0 {
		return "application/octet-stream"
	}

	// Простое определение типа по заголовкам
	if len(data) >= 4 {
		if data[0] == 0x89 && data[1] == 0x50 && data[2] == 0x4E && data[3] == 0x47 {
			return "image/png"
		}
		if data[0] == 0xFF && data[1] == 0xD8 && data[2] == 0xFF {
			return "image/jpeg"
		}
	}

	// Проверяем на JSON
	var jsonData interface{}
	if json.Unmarshal(data, &jsonData) == nil {
		return "application/json"
	}

	// Проверяем на текст
	if isText(data) {
		return "text/plain"
	}

	return "application/octet-stream"
}

func isText(data []byte) bool {
	if len(data) == 0 {
		return true
	}

	for _, b := range data[:min(512, len(data))] {
		if b == 0 || (b < 32 && b != '\t' && b != '\n' && b != '\r') {
			return false
		}
	}
	return true
}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}

// NotifyDatastoreEvent уведомляет индексатор об изменениях в datastore
func (idx *Indexer) NotifyDatastoreEvent(event IndexEvent) {
	idx.mu.RLock()
	watchers := make([]chan IndexEvent, 0, len(idx.watchers))
	for _, watcher := range idx.watchers {
		watchers = append(watchers, watcher)
	}
	idx.mu.RUnlock()

	// Уведомляем всех наблюдателей
	for _, watcher := range watchers {
		select {
		case watcher <- event:
		default:
			// Пропускаем если канал заполнен
		}
	}
}
