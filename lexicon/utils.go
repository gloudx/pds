package lexicon

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"time"
)

// SchemaImporter утилита для импорта схем из файлов
type SchemaImporter struct {
	lexiconService *LexiconService
}

// NewSchemaImporter создает новый импортер схем
func NewSchemaImporter(ls *LexiconService) *SchemaImporter {
	return &SchemaImporter{
		lexiconService: ls,
	}
}

// ImportFromDirectory импортирует все схемы из директории
func (si *SchemaImporter) ImportFromDirectory(ctx context.Context, dirPath string) error {
	return filepath.Walk(dirPath, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}

		// Обрабатываем только JSON файлы
		if !strings.HasSuffix(strings.ToLower(info.Name()), ".json") {
			return nil
		}

		fmt.Printf("Importing schema from %s...\n", path)

		if err := si.ImportFromFile(ctx, path); err != nil {
			fmt.Printf("Failed to import %s: %v\n", path, err)
			return nil // Продолжаем импорт других файлов
		}

		fmt.Printf("✅ Successfully imported %s\n", path)
		return nil
	})
}

// ImportFromFile импортирует схему из файла
func (si *SchemaImporter) ImportFromFile(ctx context.Context, filePath string) error {
	file, err := os.Open(filePath)
	if err != nil {
		return fmt.Errorf("failed to open file: %w", err)
	}
	defer file.Close()

	return si.ImportFromReader(ctx, file)
}

// ImportFromReader импортирует схему из Reader
func (si *SchemaImporter) ImportFromReader(ctx context.Context, reader io.Reader) error {
	data, err := io.ReadAll(reader)
	if err != nil {
		return fmt.Errorf("failed to read data: %w", err)
	}

	var schema LexiconDefinition
	if err := json.Unmarshal(data, &schema); err != nil {
		return fmt.Errorf("failed to parse schema JSON: %w", err)
	}

	// Устанавливаем временные метки если не заданы
	now := time.Now()
	if schema.CreatedAt.IsZero() {
		schema.CreatedAt = now
	}
	if schema.UpdatedAt.IsZero() {
		schema.UpdatedAt = now
	}

	return si.lexiconService.RegisterSchema(ctx, &schema)
}

// SchemaExporter утилита для экспорта схем в файлы
type SchemaExporter struct {
	lexiconService *LexiconService
}

// NewSchemaExporter создает новый экспортер схем
func NewSchemaExporter(ls *LexiconService) *SchemaExporter {
	return &SchemaExporter{
		lexiconService: ls,
	}
}

// ExportToDirectory экспортирует все схемы в директорию
func (se *SchemaExporter) ExportToDirectory(ctx context.Context, dirPath string) error {
	// Создаем директорию если не существует
	if err := os.MkdirAll(dirPath, 0755); err != nil {
		return fmt.Errorf("failed to create directory: %w", err)
	}

	schemas, err := se.lexiconService.ListSchemas(ctx)
	if err != nil {
		return fmt.Errorf("failed to list schemas: %w", err)
	}

	for _, schema := range schemas {
		fileName := fmt.Sprintf("%s.json", strings.ReplaceAll(string(schema.ID), ".", "_"))
		filePath := filepath.Join(dirPath, fileName)

		if err := se.ExportToFile(ctx, schema.ID, filePath); err != nil {
			fmt.Printf("Failed to export %s: %v\n", schema.ID, err)
			continue
		}

		fmt.Printf("✅ Exported %s to %s\n", schema.ID, filePath)
	}

	return nil
}

// ExportToFile экспортирует схему в файл
func (se *SchemaExporter) ExportToFile(ctx context.Context, lexiconID LexiconID, filePath string) error {
	schema, err := se.lexiconService.GetSchema(ctx, lexiconID)
	if err != nil {
		return fmt.Errorf("failed to get schema: %w", err)
	}

	file, err := os.Create(filePath)
	if err != nil {
		return fmt.Errorf("failed to create file: %w", err)
	}
	defer file.Close()

	return se.ExportToWriter(schema, file)
}

// ExportToWriter экспортирует схему в Writer
func (se *SchemaExporter) ExportToWriter(schema *LexiconDefinition, writer io.Writer) error {
	encoder := json.NewEncoder(writer)
	encoder.SetIndent("", "  ")

	return encoder.Encode(schema)
}

// RecordMigrator утилита для миграции записей между версиями схем
type RecordMigrator struct {
	lexiconService *LexiconService
}

// NewRecordMigrator создает новый мигратор записей
func NewRecordMigrator(ls *LexiconService) *RecordMigrator {
	return &RecordMigrator{
		lexiconService: ls,
	}
}

// MigrateRecord мигрирует запись к новой версии схемы
func (rm *RecordMigrator) MigrateRecord(ctx context.Context, record *TypedRecord, targetVersion string) (*TypedRecord, error) {
	// Получаем текущую и целевую схемы
	baseID := rm.lexiconService.registry.getBaseID(record.LexiconID)
	currentSchema := rm.lexiconService.registry.GetLatestVersion(baseID)
	targetSchema := rm.lexiconService.registry.GetVersion(baseID, targetVersion)

	if currentSchema == nil || targetSchema == nil {
		return nil, fmt.Errorf("schema not found for migration")
	}

	if currentSchema.Version == targetVersion {
		return record, nil // Уже нужная версия
	}

	// Получаем путь миграции
	migrationPath, err := rm.lexiconService.registry.GetMigrationPath(baseID, currentSchema.Version, targetVersion)
	if err != nil {
		return nil, fmt.Errorf("failed to get migration path: %w", err)
	}

	// Применяем миграции по порядку
	migratedRecord := &TypedRecord{
		ID:        record.ID,
		LexiconID: record.LexiconID,
		Data:      record.Data,
		CreatedAt: record.CreatedAt,
		UpdatedAt: time.Now(),
		CreatedBy: record.CreatedBy,
	}

	for _, step := range migrationPath {
		if err := rm.applyMigrationStep(migratedRecord, step); err != nil {
			return nil, fmt.Errorf("failed to apply migration step %s: %w", step.Version, err)
		}
	}

	// Обновляем ID лексикона к целевой версии
	migratedRecord.LexiconID = targetSchema.ID

	return migratedRecord, nil
}

// applyMigrationStep применяет один шаг миграции
func (rm *RecordMigrator) applyMigrationStep(record *TypedRecord, targetSchema *LexiconDefinition) error {
	// Упрощенная логика миграции - в реальности нужны более сложные трансформации

	// Валидируем данные против новой схемы
	if err := rm.lexiconService.validator.ValidateRecord(targetSchema, record.Data); err != nil {
		// Пытаемся автоматически исправить данные
		if fixedData := rm.autoFixData(record.Data, targetSchema); fixedData != nil {
			record.Data = fixedData
		} else {
			return fmt.Errorf("data validation failed and cannot be auto-fixed: %w", err)
		}
	}

	return nil
}

// autoFixData пытается автоматически исправить данные для соответствия схеме
func (rm *RecordMigrator) autoFixData(data interface{}, schema *LexiconDefinition) interface{} {
	dataMap, ok := data.(map[string]interface{})
	if !ok {
		return nil
	}

	fixed := make(map[string]interface{})

	// Копируем существующие поля
	for key, value := range dataMap {
		fixed[key] = value
	}

	// Добавляем значения по умолчанию для новых обязательных полей
	if required, ok := schema.Schema["required"].([]interface{}); ok {
		if props, ok := schema.Schema["properties"].(map[string]interface{}); ok {
			for _, reqField := range required {
				if fieldName, ok := reqField.(string); ok {
					if _, exists := fixed[fieldName]; !exists {
						// Добавляем значение по умолчанию
						if defaultValue := rm.getDefaultValue(props[fieldName]); defaultValue != nil {
							fixed[fieldName] = defaultValue
						}
					}
				}
			}
		}
	}

	return fixed
}

// getDefaultValue возвращает значение по умолчанию для типа поля
func (rm *RecordMigrator) getDefaultValue(fieldSchema interface{}) interface{} {
	fieldMap, ok := fieldSchema.(map[string]interface{})
	if !ok {
		return nil
	}

	fieldType, ok := fieldMap["type"].(string)
	if !ok {
		return nil
	}

	switch fieldType {
	case "string":
		return ""
	case "integer", "number":
		return 0
	case "boolean":
		return false
	case "array":
		return []interface{}{}
	case "object":
		return map[string]interface{}{}
	default:
		return nil
	}
}

// SchemaValidator расширенный валидатор схем
type SchemaValidator struct {
	validator *Validator
}

// NewSchemaValidator создает новый валидатор
func NewSchemaValidator() *SchemaValidator {
	return &SchemaValidator{
		validator: NewValidator(),
	}
}

// ValidateSchemaCompatibility проверяет совместимость двух схем
func (sv *SchemaValidator) ValidateSchemaCompatibility(oldSchema, newSchema *LexiconDefinition) error {
	// Проверяем что ID одинаковые
	if oldSchema.ID != newSchema.ID {
		return fmt.Errorf("schema IDs must match")
	}

	// Проверяем версии
	if oldSchema.Version == newSchema.Version {
		return fmt.Errorf("schema versions must be different")
	}

	// Проверяем обратную совместимость
	return sv.checkBackwardCompatibility(oldSchema, newSchema)
}

// checkBackwardCompatibility проверяет обратную совместимость схем
func (sv *SchemaValidator) checkBackwardCompatibility(oldSchema, newSchema *LexiconDefinition) error {
	oldProps := sv.getSchemaProperties(oldSchema)
	newProps := sv.getSchemaProperties(newSchema)
	oldRequired := sv.getRequiredFields(oldSchema)
	newRequired := sv.getRequiredFields(newSchema)

	// Проверяем, что все старые обязательные поля остались
	for _, field := range oldRequired {
		found := false
		for _, newField := range newRequired {
			if field == newField {
				found = true
				break
			}
		}
		if !found {
			return fmt.Errorf("required field '%s' was removed - breaking change", field)
		}
	}

	// Проверяем, что типы существующих полей не изменились
	for fieldName, oldFieldSchema := range oldProps {
		if newFieldSchema, exists := newProps[fieldName]; exists {
			if !sv.areFieldTypesCompatible(oldFieldSchema, newFieldSchema) {
				return fmt.Errorf("field '%s' type changed - breaking change", fieldName)
			}
		}
	}

	return nil
}

// Вспомогательные методы

func (sv *SchemaValidator) getSchemaProperties(schema *LexiconDefinition) map[string]interface{} {
	if props, ok := schema.Schema["properties"].(map[string]interface{}); ok {
		return props
	}
	return make(map[string]interface{})
}

func (sv *SchemaValidator) getRequiredFields(schema *LexiconDefinition) []string {
	var required []string
	if reqFields, ok := schema.Schema["required"].([]interface{}); ok {
		for _, field := range reqFields {
			if fieldName, ok := field.(string); ok {
				required = append(required, fieldName)
			}
		}
	}
	return required
}

func (sv *SchemaValidator) areFieldTypesCompatible(oldField, newField interface{}) bool {
	oldFieldMap, ok1 := oldField.(map[string]interface{})
	newFieldMap, ok2 := newField.(map[string]interface{})

	if !ok1 || !ok2 {
		return false
	}

	oldType, ok1 := oldFieldMap["type"].(string)
	newType, ok2 := newFieldMap["type"].(string)

	if !ok1 || !ok2 {
		return false
	}

	// Строгая проверка - типы должны совпадать
	// В более продвинутой реализации можно добавить логику совместимых типов
	return oldType == newType
}

// LexiconHelper набор утилитарных функций
type LexiconHelper struct{}

// NewLexiconHelper создает новый хелпер
func NewLexiconHelper() *LexiconHelper {
	return &LexiconHelper{}
}

// GenerateLexiconID генерирует ID лексикона по соглашению
func (lh *LexiconHelper) GenerateLexiconID(namespace, domain, recordType string) LexiconID {
	return LexiconID(fmt.Sprintf("%s.%s.%s", namespace, domain, recordType))
}

// ParseLexiconID парсит ID лексикона на компоненты
func (lh *LexiconHelper) ParseLexiconID(id LexiconID) (namespace, domain, recordType string, err error) {
	parts := strings.Split(string(id), ".")
	if len(parts) < 3 {
		return "", "", "", fmt.Errorf("invalid lexicon ID format: %s", id)
	}

	return parts[0], parts[1], strings.Join(parts[2:], "."), nil
}

// CreateBasicTextSchema создает базовую схему для текстовых данных
func (lh *LexiconHelper) CreateBasicTextSchema(id LexiconID, name, description string, maxLength int) *LexiconDefinition {
	return &LexiconDefinition{
		ID:          id,
		Version:     "1.0.0",
		Name:        name,
		Description: description,
		Schema: map[string]interface{}{
			"type": "object",
			"properties": map[string]interface{}{
				"content": map[string]interface{}{
					"type":      "string",
					"maxLength": maxLength,
				},
				"title": map[string]interface{}{
					"type":      "string",
					"maxLength": 200,
				},
			},
			"required": []interface{}{"content"},
		},
		Indexes: []IndexDefinition{
			{
				Name:   "content_search",
				Fields: []string{"title", "content"},
				Type:   "fts",
			},
		},
		CreatedAt: time.Now(),
		UpdatedAt: time.Now(),
	}
}

// CreateMediaSchema создает схему для медиа-файлов
func (lh *LexiconHelper) CreateMediaSchema(id LexiconID, name, description string) *LexiconDefinition {
	return &LexiconDefinition{
		ID:          id,
		Version:     "1.0.0",
		Name:        name,
		Description: description,
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
					"type":    "integer",
					"minimum": 0,
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
			"required": []interface{}{"filename", "mimeType", "cid"},
		},
		Indexes: []IndexDefinition{
			{
				Name:   "mimetype_index",
				Fields: []string{"mimeType"},
				Type:   "btree",
			},
			{
				Name:   "filename_search",
				Fields: []string{"filename", "description"},
				Type:   "fts",
			},
		},
		CreatedAt: time.Now(),
		UpdatedAt: time.Now(),
	}
}

// ValidateRecordID проверяет валидность ID записи
func (lh *LexiconHelper) ValidateRecordID(id RecordID) error {
	if len(string(id)) == 0 {
		return fmt.Errorf("record ID cannot be empty")
	}

	if len(string(id)) > 100 {
		return fmt.Errorf("record ID too long")
	}

	// Дополнительные проверки формата можно добавить здесь

	return nil
}

// SanitizeRecordData очищает и нормализует данные записи
func (lh *LexiconHelper) SanitizeRecordData(data interface{}) interface{} {
	switch v := data.(type) {
	case map[string]interface{}:
		sanitized := make(map[string]interface{})
		for key, value := range v {
			// Рекурсивно обрабатываем вложенные структуры
			sanitized[key] = lh.SanitizeRecordData(value)
		}
		return sanitized

	case []interface{}:
		sanitized := make([]interface{}, len(v))
		for i, value := range v {
			sanitized[i] = lh.SanitizeRecordData(value)
		}
		return sanitized

	case string:
		// Убираем лишние пробелы
		return strings.TrimSpace(v)

	default:
		return v
	}
}
