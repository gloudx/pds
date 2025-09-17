// lexicon/lexicon.go
package lexicon

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"fmt"
	s "pds/datastore"
	i "pds/indexer"
	"pds/mstdatastore"
	"strings"
	"sync"
	"time"

	"github.com/ipfs/go-datastore"
	"github.com/ipfs/go-datastore/query"
)

const (
	LexiconPrefix     = "lexicon:"
	RecordPrefix      = "record:"
	SchemaCollection  = "schemas"
	RecordCollection  = "records"
	DefaultCollection = "default"
)

// LexiconService основной сервис для работы с типизированными данными
type LexiconService struct {
	storage   s.Datastore
	mst       *mstdatastore.MstDatastore
	indexer   *i.Indexer
	validator *Validator
	registry  *Registry
	mu        sync.RWMutex
}

// NewLexiconService создает новый сервис лексиконов
func NewLexiconService(storage s.Datastore, mst *mstdatastore.MstDatastore, indexer *i.Indexer) (*LexiconService, error) {
	ls := &LexiconService{
		storage:   storage,
		mst:       mst,
		indexer:   indexer,
		validator: NewValidator(),
		registry:  NewRegistry(),
	}

	// Загружаем существующие схемы
	if err := ls.loadSchemas(context.Background()); err != nil {
		return nil, fmt.Errorf("failed to load schemas: %w", err)
	}

	// Регистрируем встроенные схемы
	if err := ls.registerBuiltinSchemas(); err != nil {
		return nil, fmt.Errorf("failed to register builtin schemas: %w", err)
	}

	if !ls.mst.HasCollection(SchemaCollection) {
		// Создаем коллекцию для схем, если ее нет
		if err := ls.mst.CreateCollection(SchemaCollection, map[string]string{
			"description": "Collection for lexicon schemas",
		}); err != nil {
			return nil, fmt.Errorf("failed to create schema collection: %w", err)
		}
	}

	// Создаем коллекцию для записей, если ее нет
	if !ls.mst.HasCollection(RecordCollection) {
		if err := ls.mst.CreateCollection(RecordCollection, map[string]string{
			"description": "Collection for lexicon records",
		}); err != nil {
			return nil, fmt.Errorf("failed to create record collection: %w", err)
		}
	}

	return ls, nil
}

// === УПРАВЛЕНИЕ СХЕМАМИ ===

// RegisterSchema регистрирует новую схему лексикона
func (ls *LexiconService) RegisterSchema(ctx context.Context, def *LexiconDefinition) error {

	// Валидируем схему
	if err := ls.validator.ValidateSchema(def); err != nil {
		return fmt.Errorf("invalid schema: %w", err)
	}

	def.CreatedAt = time.Now()
	def.UpdatedAt = def.CreatedAt

	// Сохраняем схему в MST
	schemaKey := datastore.NewKey(fmt.Sprintf("%s%s", LexiconPrefix, def.ID))
	schemaData, err := json.Marshal(def)
	if err != nil {
		return err
	}

	if err := ls.mst.PutInCollection(ctx, SchemaCollection, schemaKey, schemaData); err != nil {
		return err
	}

	// Регистрируем в реестре
	if err := ls.registry.Register(def); err != nil {
		return err
	}

	// Индексируем схему
	if ls.indexer != nil {
		entry := &i.IndexEntry{
			Key:         schemaKey.String(),
			ContentType: "application/json",
			Size:        int64(len(schemaData)),
			CreatedAt:   def.CreatedAt,
			UpdatedAt:   def.UpdatedAt,
			DataHash:    fmt.Sprintf("%x", schemaData),
			Metadata: map[string]interface{}{
				"type":        "lexicon_schema",
				"lexicon_id":  string(def.ID),
				"version":     def.Version,
				"name":        def.Name,
				"description": def.Description,
				"content":     def.Name + " " + def.Description,
			},
		}
		go ls.indexer.IndexEntry(entry)
		go ls.indexer.AddTags(schemaKey.String(), []string{"lexicon", "schema", "definition"})
	}

	return nil
}

// GetSchema получает схему по ID
func (ls *LexiconService) GetSchema(ctx context.Context, id LexiconID) (*LexiconDefinition, error) {
	ls.mu.RLock()
	defer ls.mu.RUnlock()

	// Сначала проверяем реестр
	if def := ls.registry.Get(id); def != nil {
		return def, nil
	}

	// Загружаем из storage
	schemaKey := datastore.NewKey(fmt.Sprintf("%s%s", LexiconPrefix, id))
	data, err := ls.mst.GetFromCollection(ctx, SchemaCollection, schemaKey)
	if err != nil {
		if err == datastore.ErrNotFound {
			return nil, fmt.Errorf("schema not found: %s", id)
		}
		return nil, err
	}

	var def LexiconDefinition
	if err := json.Unmarshal(data, &def); err != nil {
		return nil, err
	}

	// Кешируем в реестре
	ls.registry.Register(&def)

	return &def, nil
}

// ListSchemas возвращает список всех схем
func (ls *LexiconService) ListSchemas(ctx context.Context) ([]*LexiconDefinition, error) {
	ls.mu.RLock()
	defer ls.mu.RUnlock()

	// Получаем все схемы из коллекции
	results, err := ls.mst.QueryCollection(ctx, SchemaCollection, query.Query{
		Prefix: LexiconPrefix,
	})
	if err != nil {
		return nil, err
	}
	defer results.Close()

	var schemas []*LexiconDefinition
	for result := range results.Next() {
		if result.Error != nil {
			continue
		}

		var def LexiconDefinition
		if err := json.Unmarshal(result.Value, &def); err != nil {
			continue
		}

		schemas = append(schemas, &def)
	}

	return schemas, nil
}

// === РАБОТА С ЗАПИСЯМИ ===

// CreateRecord создает новую типизированную запись
func (ls *LexiconService) CreateRecord(ctx context.Context, lexiconID LexiconID, data interface{}, createdBy string) (*TypedRecord, error) {

	// Получаем схему
	schema, err := ls.GetSchema(ctx, lexiconID)
	if err != nil {
		return nil, err
	}

	// Валидируем данные
	if err := ls.validator.ValidateRecord(schema, data); err != nil {
		return nil, err
	}

	// Создаем запись
	record := &TypedRecord{
		ID:        RecordID(generateID()),
		LexiconID: lexiconID,
		Data:      data,
		CreatedAt: time.Now(),
		UpdatedAt: time.Now(),
		CreatedBy: createdBy,
	}

	// Сериализуем
	recordData, err := json.Marshal(record)
	if err != nil {
		return nil, err
	}

	// Вычисляем CID
	record.CID = fmt.Sprintf("%x", recordData[:min(16, len(recordData))])

	// Пересериализуем с CID
	recordData, err = json.Marshal(record)
	if err != nil {
		return nil, err
	}

	// Сохраняем в MST
	recordKey := datastore.NewKey(fmt.Sprintf("%s%s:%s", RecordPrefix, lexiconID, record.ID))
	if err := ls.mst.PutInCollection(ctx, RecordCollection, recordKey, recordData); err != nil {
		return nil, err
	}

	// Индексируем запись
	if ls.indexer != nil {
		go ls.indexRecord(record, recordKey.String(), recordData)
	}

	return record, nil
}

// GetRecord получает запись по ID
func (ls *LexiconService) GetRecord(ctx context.Context, lexiconID LexiconID,
	recordID RecordID) (*TypedRecord, error) {

	ls.mu.RLock()
	defer ls.mu.RUnlock()

	recordKey := datastore.NewKey(fmt.Sprintf("%s%s:%s", RecordPrefix, lexiconID, recordID))
	data, err := ls.mst.GetFromCollection(ctx, RecordCollection, recordKey)
	if err != nil {
		if err == datastore.ErrNotFound {
			return nil, fmt.Errorf("record not found: %s/%s", lexiconID, recordID)
		}
		return nil, err
	}

	var record TypedRecord
	if err := json.Unmarshal(data, &record); err != nil {
		return nil, err
	}

	return &record, nil
}

// UpdateRecord обновляет существующую запись
func (ls *LexiconService) UpdateRecord(ctx context.Context, lexiconID LexiconID,
	recordID RecordID, data interface{}, updatedBy string) (*TypedRecord, error) {

	// Получаем существующую запись
	existing, err := ls.GetRecord(ctx, lexiconID, recordID)
	if err != nil {
		return nil, err
	}

	// Получаем схему и валидируем
	schema, err := ls.GetSchema(ctx, lexiconID)
	if err != nil {
		return nil, err
	}

	if err := ls.validator.ValidateRecord(schema, data); err != nil {
		return nil, err
	}

	// Обновляем запись
	existing.Data = data
	existing.UpdatedAt = time.Now()
	if updatedBy != "" {
		existing.CreatedBy = updatedBy // В реальности нужно поле UpdatedBy
	}

	// Сохраняем
	recordData, err := json.Marshal(existing)
	if err != nil {
		return nil, err
	}

	recordKey := datastore.NewKey(fmt.Sprintf("%s%s:%s", RecordPrefix, lexiconID, recordID))
	if err := ls.mst.PutInCollection(ctx, RecordCollection, recordKey, recordData); err != nil {
		return nil, err
	}

	// Переиндексируем
	if ls.indexer != nil {
		go ls.indexRecord(existing, recordKey.String(), recordData)
	}

	return existing, nil
}

// DeleteRecord удаляет запись
func (ls *LexiconService) DeleteRecord(ctx context.Context, lexiconID LexiconID,
	recordID RecordID) error {

	recordKey := datastore.NewKey(fmt.Sprintf("%s%s:%s", RecordPrefix, lexiconID, recordID))

	// Удаляем из MST
	if err := ls.mst.DeleteFromCollection(ctx, RecordCollection, recordKey); err != nil {
		return err
	}

	// Удаляем из индекса
	if ls.indexer != nil {
		go ls.indexer.RemoveEntry(recordKey.String())
	}

	return nil
}

// QueryRecords выполняет поиск записей
func (ls *LexiconService) QueryRecords(ctx context.Context, filter *QueryFilter) (*QueryResult, error) {
	ls.mu.RLock()
	defer ls.mu.RUnlock()

	// Строим запрос
	var queryPrefix string
	if filter.LexiconID != "" {
		queryPrefix = fmt.Sprintf("%s%s:", RecordPrefix, filter.LexiconID)
	} else {
		queryPrefix = RecordPrefix
	}

	// Получаем данные из MST
	results, err := ls.mst.QueryCollection(ctx, RecordCollection, query.Query{
		Prefix: queryPrefix,
		Limit:  filter.Limit,
		Offset: filter.Offset,
	})
	if err != nil {
		return nil, err
	}
	defer results.Close()

	var records []*TypedRecord
	for result := range results.Next() {
		if result.Error != nil {
			continue
		}

		var record TypedRecord
		if err := json.Unmarshal(result.Value, &record); err != nil {
			continue
		}

		// Применяем фильтры
		if ls.matchesFilter(&record, filter) {
			records = append(records, &record)
		}
	}

	return &QueryResult{
		Records: records,
		Total:   len(records),
		HasMore: len(records) == filter.Limit,
	}, nil
}

// === ВСПОМОГАТЕЛЬНЫЕ МЕТОДЫ ===

func (ls *LexiconService) indexRecord(record *TypedRecord, key string, data []byte) {
	// Извлекаем текстовое содержимое для полнотекстового поиска
	content := ls.extractTextContent(record.Data)

	entry := &i.IndexEntry{
		Key:         key,
		ContentType: "application/json",
		Size:        int64(len(data)),
		CreatedAt:   record.CreatedAt,
		UpdatedAt:   record.UpdatedAt,
		DataHash:    record.CID,
		Metadata: map[string]interface{}{
			"type":        "lexicon_record",
			"lexicon_id":  string(record.LexiconID),
			"record_id":   string(record.ID),
			"created_by":  record.CreatedBy,
			"content":     content,
			"record_data": record.Data,
		},
	}

	ls.indexer.IndexEntry(entry)

	// Добавляем теги
	tags := []string{"lexicon", "record", string(record.LexiconID)}
	if record.CreatedBy != "" {
		tags = append(tags, "user:"+record.CreatedBy)
	}
	ls.indexer.AddTags(key, tags)
}

func (ls *LexiconService) extractTextContent(data interface{}) string {
	dataBytes, _ := json.Marshal(data)

	// Простая экстракция текста из JSON
	var result strings.Builder
	var jsonData map[string]interface{}

	if json.Unmarshal(dataBytes, &jsonData) == nil {
		ls.extractTextFromMap(jsonData, &result)
	}

	return result.String()
}

func (ls *LexiconService) extractTextFromMap(data map[string]interface{}, result *strings.Builder) {
	for _, value := range data {
		switch v := value.(type) {
		case string:
			if result.Len() > 0 {
				result.WriteString(" ")
			}
			result.WriteString(v)
		case map[string]interface{}:
			ls.extractTextFromMap(v, result)
		case []interface{}:
			for _, item := range v {
				if itemMap, ok := item.(map[string]interface{}); ok {
					ls.extractTextFromMap(itemMap, result)
				} else if itemStr, ok := item.(string); ok {
					if result.Len() > 0 {
						result.WriteString(" ")
					}
					result.WriteString(itemStr)
				}
			}
		}
	}
}

func (ls *LexiconService) matchesFilter(record *TypedRecord, filter *QueryFilter) bool {
	// Фильтр по автору
	if filter.CreatedBy != "" && record.CreatedBy != filter.CreatedBy {
		return false
	}

	// Фильтр по времени
	if filter.Since != nil && record.CreatedAt.Before(*filter.Since) {
		return false
	}

	if filter.Until != nil && record.CreatedAt.After(*filter.Until) {
		return false
	}

	// Фильтр по полям (простая реализация)
	if len(filter.Fields) > 0 {
		recordBytes, _ := json.Marshal(record.Data)
		var recordData map[string]interface{}
		if json.Unmarshal(recordBytes, &recordData) == nil {
			for field, expectedValue := range filter.Fields {
				if recordData[field] != expectedValue {
					return false
				}
			}
		}
	}

	return true
}

func (ls *LexiconService) loadSchemas(ctx context.Context) error {
	// Попытка загрузить схемы из коллекции
	results, err := ls.mst.QueryCollection(ctx, SchemaCollection, query.Query{
		Prefix: LexiconPrefix,
	})
	if err != nil {
		// Коллекция может не существовать - это нормально для первого запуска
		return nil
	}
	defer results.Close()

	for result := range results.Next() {
		if result.Error != nil {
			continue
		}

		var def LexiconDefinition
		if err := json.Unmarshal(result.Value, &def); err != nil {
			continue
		}

		ls.registry.Register(&def)
	}

	return nil
}

func (ls *LexiconService) registerBuiltinSchemas() error {
	// Регистрируем базовые встроенные схемы
	builtins := []*LexiconDefinition{
		{
			ID:          "app.uds.note",
			Version:     "1.0.0",
			Name:        "Text Note",
			Description: "Simple text note",
			Schema: map[string]interface{}{
				"type": "object",
				"properties": map[string]interface{}{
					"title": map[string]interface{}{
						"type":      "string",
						"maxLength": 200,
					},
					"content": map[string]interface{}{
						"type":      "string",
						"maxLength": 10000,
					},
					"tags": map[string]interface{}{
						"type": "array",
						"items": map[string]interface{}{
							"type": "string",
						},
					},
				},
				"required": []string{"content"},
			},
			Indexes: []IndexDefinition{
				{
					Name:   "content_fts",
					Fields: []string{"title", "content"},
					Type:   "fts",
				},
				{
					Name:   "tags_index",
					Fields: []string{"tags"},
					Type:   "btree",
				},
			},
		},
		{
			ID:          "app.uds.media",
			Version:     "1.0.0",
			Name:        "Media Asset",
			Description: "Media file reference",
			Schema: map[string]interface{}{
				"type": "object",
				"properties": map[string]interface{}{
					"filename": map[string]interface{}{
						"type": "string",
					},
					"mimeType": map[string]interface{}{
						"type": "string",
					},
					"size": map[string]interface{}{
						"type": "integer",
					},
					"cid": map[string]interface{}{
						"type": "string",
					},
					"description": map[string]interface{}{
						"type":      "string",
						"maxLength": 1000,
					},
					"metadata": map[string]interface{}{
						"type": "object",
					},
				},
				"required": []string{"filename", "mimeType", "cid"},
			},
			Indexes: []IndexDefinition{
				{
					Name:   "mimetype_index",
					Fields: []string{"mimeType"},
					Type:   "btree",
				},
				{
					Name:   "filename_fts",
					Fields: []string{"filename", "description"},
					Type:   "fts",
				},
			},
		},
	}

	for _, def := range builtins {
		ls.registry.Register(def)
	}

	return nil
}

func generateID() string {
	bytes := make([]byte, 16)
	rand.Read(bytes)
	return hex.EncodeToString(bytes)
}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}
