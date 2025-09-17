// lexicon/registry.go
package lexicon

import (
	"fmt"
	"sort"
	"strings"
	"sync"
)

// Registry реестр схем лексиконов с поддержкой версионирования
type Registry struct {
	schemas     map[LexiconID]*LexiconDefinition // Текущие версии схем
	allVersions map[string][]*LexiconDefinition  // Все версии по base ID
	mu          sync.RWMutex
}

// NewRegistry создает новый реестр схем
func NewRegistry() *Registry {
	return &Registry{
		schemas:     make(map[LexiconID]*LexiconDefinition),
		allVersions: make(map[string][]*LexiconDefinition),
	}
}

// Register регистрирует схему в реестре
func (r *Registry) Register(def *LexiconDefinition) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	// Валидируем что схема не null
	if def == nil {
		return fmt.Errorf("schema definition cannot be nil")
	}

	// Регистрируем как текущую версию
	r.schemas[def.ID] = def

	// Добавляем в список всех версий
	baseID := r.getBaseID(def.ID)
	if r.allVersions[baseID] == nil {
		r.allVersions[baseID] = make([]*LexiconDefinition, 0)
	}

	// Проверяем, нет ли уже такой версии
	for _, existing := range r.allVersions[baseID] {
		if existing.Version == def.Version {
			// Заменяем существующую версию
			existing.Schema = def.Schema
			existing.Indexes = def.Indexes
			existing.UpdatedAt = def.UpdatedAt
			existing.Description = def.Description
			existing.Name = def.Name
			return nil
		}
	}

	// Добавляем новую версию
	r.allVersions[baseID] = append(r.allVersions[baseID], def)

	// Сортируем версии (последняя версия должна быть в конце)
	sort.Slice(r.allVersions[baseID], func(i, j int) bool {
		return r.compareVersions(r.allVersions[baseID][i].Version, r.allVersions[baseID][j].Version) < 0
	})

	return nil
}

// Get получает схему по ID (последнюю версию)
func (r *Registry) Get(id LexiconID) *LexiconDefinition {
	r.mu.RLock()
	defer r.mu.RUnlock()

	if def, exists := r.schemas[id]; exists {
		// Возвращаем копию для безопасности
		return r.copyDefinition(def)
	}

	return nil
}

// GetVersion получает конкретную версию схемы
func (r *Registry) GetVersion(baseID, version string) *LexiconDefinition {
	r.mu.RLock()
	defer r.mu.RUnlock()

	if versions, exists := r.allVersions[baseID]; exists {
		for _, def := range versions {
			if def.Version == version {
				return r.copyDefinition(def)
			}
		}
	}

	return nil
}

// GetLatestVersion получает последнюю версию схемы по base ID
func (r *Registry) GetLatestVersion(baseID string) *LexiconDefinition {
	r.mu.RLock()
	defer r.mu.RUnlock()

	if versions, exists := r.allVersions[baseID]; exists && len(versions) > 0 {
		// Последняя версия в отсортированном массиве
		return r.copyDefinition(versions[len(versions)-1])
	}

	return nil
}

// GetAllVersions получает все версии схемы
func (r *Registry) GetAllVersions(baseID string) []*LexiconDefinition {
	r.mu.RLock()
	defer r.mu.RUnlock()

	if versions, exists := r.allVersions[baseID]; exists {
		result := make([]*LexiconDefinition, len(versions))
		for i, def := range versions {
			result[i] = r.copyDefinition(def)
		}
		return result
	}

	return nil
}

// ListAll возвращает все зарегистрированные схемы
func (r *Registry) ListAll() []*LexiconDefinition {
	r.mu.RLock()
	defer r.mu.RUnlock()

	result := make([]*LexiconDefinition, 0, len(r.schemas))
	for _, def := range r.schemas {
		result = append(result, r.copyDefinition(def))
	}

	// Сортируем по ID для предсказуемого порядка
	sort.Slice(result, func(i, j int) bool {
		return string(result[i].ID) < string(result[j].ID)
	})

	return result
}

// ListByNamespace возвращает схемы определенного namespace
func (r *Registry) ListByNamespace(namespace string) []*LexiconDefinition {
	r.mu.RLock()
	defer r.mu.RUnlock()

	var result []*LexiconDefinition
	for _, def := range r.schemas {
		if r.getNamespace(def.ID) == namespace {
			result = append(result, r.copyDefinition(def))
		}
	}

	sort.Slice(result, func(i, j int) bool {
		return string(result[i].ID) < string(result[j].ID)
	})

	return result
}

// Unregister удаляет схему из реестра
func (r *Registry) Unregister(id LexiconID) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	_, exists := r.schemas[id]
	if !exists {
		return fmt.Errorf("schema %s not found", id)
	}

	delete(r.schemas, id)

	// Удаляем из allVersions
	baseID := r.getBaseID(id)
	if versions, exists := r.allVersions[baseID]; exists {
		for i, version := range versions {
			if version.ID == id {
				// Удаляем элемент из слайса
				r.allVersions[baseID] = append(versions[:i], versions[i+1:]...)
				break
			}
		}

		// Если это была последняя версия, удаляем весь baseID
		if len(r.allVersions[baseID]) == 0 {
			delete(r.allVersions, baseID)
		}
	}

	return nil
}

// IsCompatible проверяет совместимость версий схем
func (r *Registry) IsCompatible(fromVersion, toVersion string) bool {
	// Упрощенная логика совместимости
	// В реальности нужна более сложная логика на основе semantic versioning

	fromParts := strings.Split(fromVersion, ".")
	toParts := strings.Split(toVersion, ".")

	if len(fromParts) < 3 || len(toParts) < 3 {
		return false
	}

	// Совместимы если major версия одинаковая
	return fromParts[0] == toParts[0]
}

// GetMigrationPath получает путь миграции между версиями
func (r *Registry) GetMigrationPath(baseID, fromVersion, toVersion string) ([]*LexiconDefinition, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()

	versions, exists := r.allVersions[baseID]
	if !exists {
		return nil, fmt.Errorf("schema %s not found", baseID)
	}

	var fromIndex, toIndex int = -1, -1

	for i, def := range versions {
		if def.Version == fromVersion {
			fromIndex = i
		}
		if def.Version == toVersion {
			toIndex = i
		}
	}

	if fromIndex == -1 {
		return nil, fmt.Errorf("version %s not found", fromVersion)
	}
	if toIndex == -1 {
		return nil, fmt.Errorf("version %s not found", toVersion)
	}

	if fromIndex == toIndex {
		return []*LexiconDefinition{}, nil // Нет миграции
	}

	// Возвращаем путь между версиями
	var path []*LexiconDefinition
	if fromIndex < toIndex {
		// Миграция вперед
		for i := fromIndex + 1; i <= toIndex; i++ {
			path = append(path, r.copyDefinition(versions[i]))
		}
	} else {
		// Миграция назад (rollback)
		for i := fromIndex - 1; i >= toIndex; i-- {
			path = append(path, r.copyDefinition(versions[i]))
		}
	}

	return path, nil
}

// GetStats возвращает статистику реестра
func (r *Registry) GetStats() map[string]interface{} {
	r.mu.RLock()
	defer r.mu.RUnlock()

	stats := make(map[string]interface{})

	stats["total_schemas"] = len(r.schemas)
	stats["total_base_schemas"] = len(r.allVersions)

	// Статистика по namespace
	namespaces := make(map[string]int)
	for _, def := range r.schemas {
		ns := r.getNamespace(def.ID)
		namespaces[ns]++
	}
	stats["namespaces"] = namespaces

	// Статистика версий
	var totalVersions int
	for _, versions := range r.allVersions {
		totalVersions += len(versions)
	}
	stats["total_versions"] = totalVersions

	return stats
}

// Вспомогательные методы

// getBaseID извлекает базовый ID без версии
// Например: app.bsky.feed.post -> app.bsky.feed.post
func (r *Registry) getBaseID(id LexiconID) string {
	// В данной реализации ID уже является base ID
	// В более сложной системе может быть app.bsky.feed.post@1.0.0
	return string(id)
}

// getNamespace извлекает namespace из ID
// Например: app.bsky.feed.post -> app.bsky
func (r *Registry) getNamespace(id LexiconID) string {
	parts := strings.Split(string(id), ".")
	if len(parts) < 2 {
		return "default"
	}
	return strings.Join(parts[:2], ".")
}

// compareVersions сравнивает две версии (упрощенно)
// Возвращает -1, 0, 1 аналогично strings.Compare
func (r *Registry) compareVersions(v1, v2 string) int {
	parts1 := strings.Split(v1, ".")
	parts2 := strings.Split(v2, ".")

	maxLen := len(parts1)
	if len(parts2) > maxLen {
		maxLen = len(parts2)
	}

	for i := 0; i < maxLen; i++ {
		var p1, p2 string
		if i < len(parts1) {
			p1 = parts1[i]
		} else {
			p1 = "0"
		}
		if i < len(parts2) {
			p2 = parts2[i]
		} else {
			p2 = "0"
		}

		// Простое строковое сравнение
		// В реальности нужно парсить числа
		if p1 < p2 {
			return -1
		}
		if p1 > p2 {
			return 1
		}
	}

	return 0
}

// copyDefinition создает копию определения схемы
func (r *Registry) copyDefinition(def *LexiconDefinition) *LexiconDefinition {
	if def == nil {
		return nil
	}

	// Глубокое копирование
	copy := &LexiconDefinition{
		ID:          def.ID,
		Version:     def.Version,
		Name:        def.Name,
		Description: def.Description,
		CreatedAt:   def.CreatedAt,
		UpdatedAt:   def.UpdatedAt,
	}

	// Копируем Schema
	if def.Schema != nil {
		copy.Schema = r.deepCopyMap(def.Schema)
	}

	// Копируем Indexes
	if def.Indexes != nil {
		copy.Indexes = make([]IndexDefinition, len(def.Indexes))
		for i, idx := range def.Indexes {
			copy.Indexes[i] = IndexDefinition{
				Name:   idx.Name,
				Type:   idx.Type,
				Unique: idx.Unique,
				Sparse: idx.Sparse,
			}

			// Копируем Fields
			if idx.Fields != nil {
				copy.Indexes[i].Fields = make([]string, len(idx.Fields))
				for j, field := range idx.Fields {
					copy.Indexes[i].Fields[j] = field
				}
			}

			// Копируем Metadata
			if idx.Metadata != nil {
				copy.Indexes[i].Metadata = r.deepCopyMap(idx.Metadata)
			}
		}
	}

	return copy
}

// deepCopyMap глубокое копирование map[string]interface{}
func (r *Registry) deepCopyMap(original map[string]interface{}) map[string]interface{} {
	if original == nil {
		return nil
	}

	copy := make(map[string]interface{})
	for key, value := range original {
		switch v := value.(type) {
		case map[string]interface{}:
			copy[key] = r.deepCopyMap(v)
		case []interface{}:
			copy[key] = r.deepCopySlice(v)
		default:
			copy[key] = v
		}
	}
	return copy
}

// deepCopySlice глубокое копирование []interface{}
func (r *Registry) deepCopySlice(original []interface{}) []interface{} {
	if original == nil {
		return nil
	}

	copy := make([]interface{}, len(original))
	for i, value := range original {
		switch v := value.(type) {
		case map[string]interface{}:
			copy[i] = r.deepCopyMap(v)
		case []interface{}:
			copy[i] = r.deepCopySlice(v)
		default:
			copy[i] = v
		}
	}
	return copy
}
