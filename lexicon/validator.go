// lexicon/validator.go
package lexicon

import (
	"encoding/json"
	"fmt"
	"reflect"
	"regexp"
	"strings"
)

// Validator валидирует схемы и данные согласно JSON Schema
type Validator struct {
	// Можно добавить кеш скомпилированных схем
	compiledSchemas map[LexiconID]*CompiledSchema
}

// CompiledSchema предварительно обработанная схема для быстрой валидации
type CompiledSchema struct {
	Definition       *LexiconDefinition
	RequiredFields   map[string]bool
	FieldTypes       map[string]string
	FieldConstraints map[string]map[string]interface{}
	Patterns         map[string]*regexp.Regexp
}

// NewValidator создает новый валидатор
func NewValidator() *Validator {
	return &Validator{
		compiledSchemas: make(map[LexiconID]*CompiledSchema),
	}
}

// ValidateSchema валидирует определение схемы лексикона
func (v *Validator) ValidateSchema(def *LexiconDefinition) error {
	var errors ValidationErrors

	// Проверяем базовые поля
	if def.ID == "" {
		errors = append(errors, ValidationError{
			Field:   "id",
			Message: "lexicon ID is required",
		})
	} else if !v.isValidLexiconID(string(def.ID)) {
		errors = append(errors, ValidationError{
			Field:   "id",
			Message: "invalid lexicon ID format (should be like 'app.domain.type')",
			Value:   def.ID,
		})
	}

	if def.Version == "" {
		errors = append(errors, ValidationError{
			Field:   "version",
			Message: "version is required",
		})
	} else if !v.isValidVersion(def.Version) {
		errors = append(errors, ValidationError{
			Field:   "version",
			Message: "invalid version format (should be semantic versioning)",
			Value:   def.Version,
		})
	}

	if def.Name == "" {
		errors = append(errors, ValidationError{
			Field:   "name",
			Message: "name is required",
		})
	}

	// Валидируем JSON Schema
	if def.Schema == nil || len(def.Schema) == 0 {
		errors = append(errors, ValidationError{
			Field:   "schema",
			Message: "schema definition is required",
		})
	} else {
		if err := v.validateJSONSchema(def.Schema); err != nil {
			errors = append(errors, ValidationError{
				Field:   "schema",
				Message: "invalid JSON schema: " + err.Error(),
				Value:   def.Schema,
			})
		}
	}

	// Валидируем индексы
	for i, idx := range def.Indexes {
		if err := v.validateIndex(&idx, def.Schema); err != nil {
			errors = append(errors, ValidationError{
				Field:   fmt.Sprintf("indexes[%d]", i),
				Message: err.Error(),
				Value:   idx,
			})
		}
	}

	if len(errors) > 0 {
		return errors
	}

	return nil
}

// ValidateRecord валидирует данные записи согласно схеме
func (v *Validator) ValidateRecord(schema *LexiconDefinition, data interface{}) error {
	// Компилируем схему если еще не скомпилирована
	compiled, err := v.getCompiledSchema(schema)
	if err != nil {
		return err
	}

	return v.validateRecordData(compiled, data, "")
}

// validateRecordData рекурсивно валидирует данные
func (v *Validator) validateRecordData(schema *CompiledSchema, data interface{}, path string) error {
	var errors ValidationErrors

	// Конвертируем data в map для анализа
	var dataMap map[string]interface{}

	switch d := data.(type) {
	case map[string]interface{}:
		dataMap = d
	case string:
		// Пытаемся парсить JSON строку
		if err := json.Unmarshal([]byte(d), &dataMap); err != nil {
			return ValidationError{
				Field:   path,
				Message: "invalid JSON data",
				Value:   d,
			}
		}
	default:
		// Пытаемся через JSON marshaling
		dataBytes, err := json.Marshal(data)
		if err != nil {
			return ValidationError{
				Field:   path,
				Message: "cannot serialize data to JSON",
				Value:   data,
			}
		}
		if err := json.Unmarshal(dataBytes, &dataMap); err != nil {
			return ValidationError{
				Field:   path,
				Message: "invalid data structure",
				Value:   data,
			}
		}
	}

	// Проверяем обязательные поля
	for field := range schema.RequiredFields {
		if _, exists := dataMap[field]; !exists {
			errors = append(errors, ValidationError{
				Field:   v.joinPath(path, field),
				Message: "required field is missing",
			})
		}
	}

	// Валидируем каждое поле
	for field, value := range dataMap {
		fieldPath := v.joinPath(path, field)

		if err := v.validateField(schema, field, value, fieldPath); err != nil {
			if validationErrors, ok := err.(ValidationErrors); ok {
				errors = append(errors, validationErrors...)
			} else if validationError, ok := err.(ValidationError); ok {
				errors = append(errors, validationError)
			} else {
				errors = append(errors, ValidationError{
					Field:   fieldPath,
					Message: err.Error(),
					Value:   value,
				})
			}
		}
	}

	if len(errors) > 0 {
		return errors
	}

	return nil
}

// validateField валидирует одно поле
func (v *Validator) validateField(schema *CompiledSchema, fieldName string, value interface{}, path string) error {
	// Получаем ожидаемый тип поля
	expectedType, exists := schema.FieldTypes[fieldName]
	if !exists {
		// Поле не определено в схеме - можем либо разрешить, либо запретить
		return ValidationError{
			Field:   path,
			Message: "unknown field",
			Value:   value,
		}
	}

	// Проверяем тип
	if !v.isCorrectType(value, expectedType) {
		return ValidationError{
			Field:   path,
			Message: fmt.Sprintf("expected type %s, got %s", expectedType, reflect.TypeOf(value).String()),
			Value:   value,
		}
	}

	// Проверяем ограничения поля
	if constraints, exists := schema.FieldConstraints[fieldName]; exists {
		if err := v.validateConstraints(value, constraints, path); err != nil {
			return err
		}
	}

	// Проверяем паттерны
	if pattern, exists := schema.Patterns[fieldName]; exists {
		if str, ok := value.(string); ok {
			if !pattern.MatchString(str) {
				return ValidationError{
					Field:   path,
					Message: "value does not match required pattern",
					Value:   value,
				}
			}
		}
	}

	return nil
}

// validateConstraints проверяет ограничения поля
func (v *Validator) validateConstraints(value interface{}, constraints map[string]interface{}, path string) error {
	for constraint, constraintValue := range constraints {
		switch constraint {
		case "maxLength":
			if str, ok := value.(string); ok {
				if maxLen, ok := constraintValue.(float64); ok {
					if len(str) > int(maxLen) {
						return ValidationError{
							Field:   path,
							Message: fmt.Sprintf("string length %d exceeds maximum %d", len(str), int(maxLen)),
							Value:   value,
						}
					}
				}
			}

		case "minLength":
			if str, ok := value.(string); ok {
				if minLen, ok := constraintValue.(float64); ok {
					if len(str) < int(minLen) {
						return ValidationError{
							Field:   path,
							Message: fmt.Sprintf("string length %d is below minimum %d", len(str), int(minLen)),
							Value:   value,
						}
					}
				}
			}

		case "maximum":
			if num, ok := v.toFloat64(value); ok {
				if max, ok := constraintValue.(float64); ok {
					if num > max {
						return ValidationError{
							Field:   path,
							Message: fmt.Sprintf("value %g exceeds maximum %g", num, max),
							Value:   value,
						}
					}
				}
			}

		case "minimum":
			if num, ok := v.toFloat64(value); ok {
				if min, ok := constraintValue.(float64); ok {
					if num < min {
						return ValidationError{
							Field:   path,
							Message: fmt.Sprintf("value %g is below minimum %g", num, min),
							Value:   value,
						}
					}
				}
			}

		case "enum":
			if enumValues, ok := constraintValue.([]interface{}); ok {
				found := false
				for _, enumValue := range enumValues {
					if reflect.DeepEqual(value, enumValue) {
						found = true
						break
					}
				}
				if !found {
					return ValidationError{
						Field:   path,
						Message: "value is not in allowed enum values",
						Value:   value,
					}
				}
			}
		}
	}

	return nil
}

// getCompiledSchema компилирует схему для быстрой валидации
func (v *Validator) getCompiledSchema(def *LexiconDefinition) (*CompiledSchema, error) {
	if compiled, exists := v.compiledSchemas[def.ID]; exists {
		return compiled, nil
	}

	compiled, err := v.compileSchema(def)
	if err != nil {
		return nil, err
	}

	v.compiledSchemas[def.ID] = compiled
	return compiled, nil
}

// compileSchema компилирует схему лексикона
func (v *Validator) compileSchema(def *LexiconDefinition) (*CompiledSchema, error) {
	compiled := &CompiledSchema{
		Definition:       def,
		RequiredFields:   make(map[string]bool),
		FieldTypes:       make(map[string]string),
		FieldConstraints: make(map[string]map[string]interface{}),
		Patterns:         make(map[string]*regexp.Regexp),
	}

	// Парсим JSON Schema
	schema := def.Schema

	// Получаем properties
	if props, ok := schema["properties"].(map[string]interface{}); ok {
		for fieldName, fieldSchema := range props {
			if fieldSchemaMap, ok := fieldSchema.(map[string]interface{}); ok {
				// Извлекаем тип
				if fieldType, ok := fieldSchemaMap["type"].(string); ok {
					compiled.FieldTypes[fieldName] = fieldType
				}

				// Извлекаем ограничения
				constraints := make(map[string]interface{})
				for key, value := range fieldSchemaMap {
					switch key {
					case "maxLength", "minLength", "maximum", "minimum", "enum":
						constraints[key] = value
					case "pattern":
						if patternStr, ok := value.(string); ok {
							if regex, err := regexp.Compile(patternStr); err == nil {
								compiled.Patterns[fieldName] = regex
							}
						}
					}
				}
				if len(constraints) > 0 {
					compiled.FieldConstraints[fieldName] = constraints
				}
			}
		}
	}

	// Получаем required поля
	if required, ok := schema["required"].([]interface{}); ok {
		for _, field := range required {
			if fieldName, ok := field.(string); ok {
				compiled.RequiredFields[fieldName] = true
			}
		}
	}

	return compiled, nil
}

// Вспомогательные методы

func (v *Validator) isValidLexiconID(id string) bool {
	// Простая проверка формата app.domain.type
	parts := strings.Split(id, ".")
	return len(parts) >= 3 &&
		regexp.MustCompile(`^[a-z][a-z0-9]*$`).MatchString(parts[0]) &&
		regexp.MustCompile(`^[a-z][a-z0-9\-]*[a-z0-9]$`).MatchString(parts[1])
}

func (v *Validator) isValidVersion(version string) bool {
	// Простая проверка semantic versioning
	pattern := regexp.MustCompile(`^\d+\.\d+\.\d+(-[a-zA-Z0-9\-]+)?$`)
	return pattern.MatchString(version)
}

func (v *Validator) validateJSONSchema(schema map[string]interface{}) error {
	// Базовая валидация JSON Schema
	if schemaType, ok := schema["type"]; ok {
		if typeStr, ok := schemaType.(string); ok {
			validTypes := []string{"object", "array", "string", "number", "integer", "boolean", "null"}
			found := false
			for _, validType := range validTypes {
				if typeStr == validType {
					found = true
					break
				}
			}
			if !found {
				return fmt.Errorf("invalid schema type: %s", typeStr)
			}
		}
	}

	return nil
}

func (v *Validator) validateIndex(idx *IndexDefinition, schema map[string]interface{}) error {
	if idx.Name == "" {
		return fmt.Errorf("index name is required")
	}

	if len(idx.Fields) == 0 {
		return fmt.Errorf("index must have at least one field")
	}

	validTypes := []string{"btree", "hash", "fts"}
	found := false
	for _, validType := range validTypes {
		if idx.Type == validType {
			found = true
			break
		}
	}
	if !found {
		return fmt.Errorf("invalid index type: %s", idx.Type)
	}

	// Проверяем что все поля индекса существуют в схеме
	if props, ok := schema["properties"].(map[string]interface{}); ok {
		for _, field := range idx.Fields {
			if _, exists := props[field]; !exists {
				return fmt.Errorf("index field '%s' not found in schema", field)
			}
		}
	}

	return nil
}

func (v *Validator) isCorrectType(value interface{}, expectedType string) bool {
	switch expectedType {
	case "string":
		_, ok := value.(string)
		return ok
	case "number":
		switch value.(type) {
		case float64, float32, int, int32, int64:
			return true
		default:
			return false
		}
	case "integer":
		switch value.(type) {
		case int, int32, int64:
			return true
		case float64:
			// Проверяем что это целое число
			if f, ok := value.(float64); ok {
				return f == float64(int64(f))
			}
		default:
			return false
		}
	case "boolean":
		_, ok := value.(bool)
		return ok
	case "array":
		v := reflect.ValueOf(value)
		return v.Kind() == reflect.Slice || v.Kind() == reflect.Array
	case "object":
		_, ok := value.(map[string]interface{})
		return ok
	case "null":
		return value == nil
	default:
		return false
	}
	return false
}

func (v *Validator) toFloat64(value interface{}) (float64, bool) {
	switch v := value.(type) {
	case float64:
		return v, true
	case float32:
		return float64(v), true
	case int:
		return float64(v), true
	case int32:
		return float64(v), true
	case int64:
		return float64(v), true
	default:
		return 0, false
	}
}

func (v *Validator) joinPath(base, field string) string {
	if base == "" {
		return field
	}
	return base + "." + field
}
