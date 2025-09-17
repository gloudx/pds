package lexicon

import (
	"fmt"
	"time"
)

// LexiconID уникальный идентификатор лексикона
type LexiconID string

// RecordID уникальный идентификатор записи
type RecordID string

// LexiconDefinition определение схемы лексикона
type LexiconDefinition struct {
	ID          LexiconID              `json:"id"`          // app.bsky.feed.post
	Version     string                 `json:"version"`     // 1.0.0
	Name        string                 `json:"name"`        // Human readable name
	Description string                 `json:"description"` // Description
	Schema      map[string]interface{} `json:"schema"`      // JSON Schema
	Indexes     []IndexDefinition      `json:"indexes"`     // Search indexes
	CreatedAt   time.Time              `json:"createdAt"`
	UpdatedAt   time.Time              `json:"updatedAt"`
}

// IndexDefinition определение индекса для лексикона
type IndexDefinition struct {
	Name     string                 `json:"name"`   // Index name
	Fields   []string               `json:"fields"` // Fields to index
	Type     string                 `json:"type"`   // "btree", "hash", "fts"
	Unique   bool                   `json:"unique"` // Unique constraint
	Sparse   bool                   `json:"sparse"` // Sparse index
	Metadata map[string]interface{} `json:"metadata,omitempty"`
}

// TypedRecord типизированная запись
type TypedRecord struct {
	ID        RecordID    `json:"id"`    // Уникальный ID записи
	LexiconID LexiconID   `json:"$type"` // Тип записи (лексикон)
	Data      interface{} `json:"data"`  // Данные записи
	CreatedAt time.Time   `json:"createdAt"`
	UpdatedAt time.Time   `json:"updatedAt"`
	CreatedBy string      `json:"createdBy,omitempty"` // DID создателя
	CID       string      `json:"cid,omitempty"`       // Content ID
}

// ValidationError ошибка валидации
type ValidationError struct {
	Field   string      `json:"field"`
	Message string      `json:"message"`
	Value   interface{} `json:"value"`
}

func (ve ValidationError) Error() string {
	return "Validation error in field '" + ve.Field + "': " + ve.Message
}

// ValidationErrors множественные ошибки валидации
type ValidationErrors []ValidationError

func (ve ValidationErrors) Error() string {
	if len(ve) == 0 {
		return ""
	}
	if len(ve) == 1 {
		return ve[0].Error()
	}

	for _, err := range ve {
		fmt.Println(err.Error())
	}

	return "Multiple validation errors"
}

// QueryFilter фильтр для поиска записей
type QueryFilter struct {
	LexiconID LexiconID              `json:"lexiconId,omitempty"`
	Fields    map[string]interface{} `json:"fields,omitempty"`
	CreatedBy string                 `json:"createdBy,omitempty"`
	Since     *time.Time             `json:"since,omitempty"`
	Until     *time.Time             `json:"until,omitempty"`
	Limit     int                    `json:"limit,omitempty"`
	Offset    int                    `json:"offset,omitempty"`
}

// QueryResult результат поиска
type QueryResult struct {
	Records    []*TypedRecord `json:"records"`
	Total      int            `json:"total"`
	HasMore    bool           `json:"hasMore"`
	NextCursor string         `json:"nextCursor,omitempty"`
}
