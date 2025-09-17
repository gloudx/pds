package oplog

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"fmt"
	s "pds/datastore"
	"sort"
	"sync"
	"time"

	"github.com/ipfs/go-datastore"
	"github.com/ipfs/go-datastore/query"
)

const (
	operationLogPrefix = "_oplog:"
	operationLogIndex  = "_oplog_index"
)

// OperationType тип операции
type OperationType string

const (
	OpTypePut              OperationType = "PUT"
	OpTypeDelete           OperationType = "DELETE"
	OpTypeCreateCollection OperationType = "CREATE_COLLECTION"
	OpTypeDeleteCollection OperationType = "DELETE_COLLECTION"
	OpTypeBatchCommit      OperationType = "BATCH_COMMIT"
)

// OperationLogEntry запись в логе операций
type OperationLogEntry struct {
	ID            string                 `json:"id"`                   // Уникальный ID записи
	HLC           *HybridLogicalClock    `json:"hlc"`                  // Hybrid Logical Clock
	TransactionID string                 `json:"transaction_id"`       // ID транзакции
	Operation     OperationType          `json:"operation"`            // Тип операции
	Key           string                 `json:"key"`                  // Ключ операции
	Value         []byte                 `json:"value,omitempty"`      // Значение (для PUT)
	Collection    string                 `json:"collection,omitempty"` // Коллекция
	Metadata      map[string]interface{} `json:"metadata,omitempty"`   // Дополнительные метаданные
	CreatedAt     time.Time              `json:"created_at"`           // Физическое время создания
}

// OperationLog интерфейс для работы с логом операций
type OperationLog interface {
	// Записать операцию в лог
	LogOperation(ctx context.Context, entry *OperationLogEntry) error
	// Получить операции по диапазону HLC
	GetOperations(ctx context.Context, fromHLC, toHLC *HybridLogicalClock) ([]*OperationLogEntry, error)
	// Получить операции по транзакции
	GetOperationsByTransaction(ctx context.Context, txnID string) ([]*OperationLogEntry, error)
	// Получить последние N операций
	GetRecentOperations(ctx context.Context, limit int) ([]*OperationLogEntry, error)
	// Получить операции по коллекции
	GetOperationsByCollection(ctx context.Context, collection string, limit int) ([]*OperationLogEntry, error)
	// Компактификация лога (удаление старых записей)
	Compact(ctx context.Context, beforeHLC *HybridLogicalClock) error
}

// StorageOperationLog реализация OperationLog на основе Storage
type StorageOperationLog struct {
	storage s.Datastore
	hlc     *HybridLogicalClock
	mu      sync.RWMutex
}

// NewStorageOperationLog создает новый operation log
func NewStorageOperationLog(storage s.Datastore, nodeID string) *StorageOperationLog {
	return &StorageOperationLog{
		storage: storage,
		hlc:     NewHLClock(nodeID),
	}
}

// generateTransactionID генерирует уникальный ID транзакции
func GenerateTransactionID() string {
	bytes := make([]byte, 16)
	rand.Read(bytes)
	return hex.EncodeToString(bytes)
}

// generateOperationID генерирует уникальный ID операции
func GenerateOperationID() string {
	bytes := make([]byte, 8)
	rand.Read(bytes)
	return hex.EncodeToString(bytes)
}

// LogOperation записывает операцию в лог
func (sol *StorageOperationLog) LogOperation(ctx context.Context, entry *OperationLogEntry) error {

	sol.mu.Lock()
	defer sol.mu.Unlock()

	// Обновляем HLC для новой операции
	entry.HLC = sol.hlc.Tick()
	entry.CreatedAt = time.Now()

	if entry.ID == "" {
		entry.ID = GenerateOperationID()
	}

	if entry.TransactionID == "" {
		entry.TransactionID = GenerateTransactionID()
	}

	// Сериализуем запись
	data, err := json.Marshal(entry)
	if err != nil {
		return fmt.Errorf("failed to marshal operation entry: %w", err)
	}

	// Создаем ключ для хранения: _oplog:{HLC_timestamp}:{logical}:{node}:{operation_id}
	key := fmt.Sprintf("%s%d:%d:%s:%s",
		operationLogPrefix,
		entry.HLC.physicalTime,
		entry.HLC.logicalTime,
		entry.HLC.nodeID,
		entry.ID,
	)

	// Сохраняем операцию
	if err := sol.storage.Put(ctx, datastore.NewKey(key), data); err != nil {
		return fmt.Errorf("failed to store operation: %w", err)
	}

	// Обновляем индекс для быстрого поиска
	if err := sol.updateIndex(ctx, entry); err != nil {
		// Логируем ошибку, но не прерываем операцию
		fmt.Printf("Warning: failed to update operation log index: %v\n", err)
	}

	return nil
}

// GetOperations получает операции в диапазоне HLC
func (sol *StorageOperationLog) GetOperations(ctx context.Context, fromHLC, toHLC *HybridLogicalClock) ([]*OperationLogEntry, error) {

	sol.mu.RLock()
	defer sol.mu.RUnlock()

	// Запрос всех операций в логе
	results, err := sol.storage.Query(ctx, query.Query{
		Prefix: operationLogPrefix,
	})
	if err != nil {
		return nil, fmt.Errorf("failed to query operations: %w", err)
	}
	defer results.Close()

	var operations []*OperationLogEntry

	for result := range results.Next() {
		if result.Error != nil {
			continue
		}

		var entry OperationLogEntry
		if err := json.Unmarshal(result.Value, &entry); err != nil {
			continue
		}

		// Фильтруем по диапазону HLC
		if fromHLC != nil && entry.HLC.Compare(fromHLC) < 0 {
			continue
		}
		if toHLC != nil && entry.HLC.Compare(toHLC) > 0 {
			continue
		}

		operations = append(operations, &entry)
	}

	// Сортируем по HLC
	sort.Slice(operations, func(i, j int) bool {
		return operations[i].HLC.Compare(operations[j].HLC) < 0
	})

	return operations, nil
}

// GetOperationsByTransaction получает операции по ID транзакции
func (sol *StorageOperationLog) GetOperationsByTransaction(ctx context.Context, txnID string) ([]*OperationLogEntry, error) {
	sol.mu.RLock()
	defer sol.mu.RUnlock()

	// Пытаемся использовать индекс
	txnIndexKey := fmt.Sprintf("_oplog_index:txn:%s", txnID)
	indexData, err := sol.storage.Get(ctx, datastore.NewKey(txnIndexKey))
	if err == nil {
		var operationKeys []string
		if json.Unmarshal(indexData, &operationKeys) == nil {
			return sol.getOperationsByKeys(ctx, operationKeys)
		}
	}

	// Fallback: поиск по всем операциям
	return sol.searchOperationsByField(ctx, func(entry *OperationLogEntry) bool {
		return entry.TransactionID == txnID
	})
}

// GetRecentOperations получает последние N операций
func (sol *StorageOperationLog) GetRecentOperations(ctx context.Context, limit int) ([]*OperationLogEntry, error) {
	sol.mu.RLock()
	defer sol.mu.RUnlock()

	results, err := sol.storage.Query(ctx, query.Query{
		Prefix: operationLogPrefix,
	})
	if err != nil {
		return nil, err
	}
	defer results.Close()

	var operations []*OperationLogEntry

	for result := range results.Next() {
		if result.Error != nil {
			continue
		}

		var entry OperationLogEntry
		if err := json.Unmarshal(result.Value, &entry); err != nil {
			continue
		}

		operations = append(operations, &entry)
	}

	// Сортируем по HLC (по убыванию)
	sort.Slice(operations, func(i, j int) bool {
		return operations[i].HLC.Compare(operations[j].HLC) > 0
	})

	// Ограничиваем результат
	if len(operations) > limit {
		operations = operations[:limit]
	}

	return operations, nil
}

// GetOperationsByCollection получает операции по коллекции
func (sol *StorageOperationLog) GetOperationsByCollection(ctx context.Context, collection string, limit int) ([]*OperationLogEntry, error) {
	sol.mu.RLock()
	defer sol.mu.RUnlock()

	// Пытаемся использовать индекс
	collIndexKey := fmt.Sprintf("_oplog_index:coll:%s", collection)
	indexData, err := sol.storage.Get(ctx, datastore.NewKey(collIndexKey))
	if err == nil {
		var operationKeys []string
		if json.Unmarshal(indexData, &operationKeys) == nil {
			operations, err := sol.getOperationsByKeys(ctx, operationKeys)
			if err != nil {
				return nil, err
			}

			// Ограничиваем результат
			if len(operations) > limit {
				operations = operations[len(operations)-limit:]
			}

			return operations, nil
		}
	}

	// Fallback: поиск по всем операциям
	operations, err := sol.searchOperationsByField(ctx, func(entry *OperationLogEntry) bool {
		return entry.Collection == collection
	})
	if err != nil {
		return nil, err
	}

	if len(operations) > limit {
		operations = operations[len(operations)-limit:]
	}

	return operations, nil
}

// Compact удаляет старые операции
func (sol *StorageOperationLog) Compact(ctx context.Context, beforeHLC *HybridLogicalClock) error {
	sol.mu.Lock()
	defer sol.mu.Unlock()

	results, err := sol.storage.Query(ctx, query.Query{
		Prefix: operationLogPrefix,
	})
	if err != nil {
		return err
	}
	defer results.Close()

	batch, err := sol.storage.Batch(ctx)
	if err != nil {
		return err
	}

	var deletedCount int

	for result := range results.Next() {
		if result.Error != nil {
			continue
		}

		var entry OperationLogEntry
		if err := json.Unmarshal(result.Value, &entry); err != nil {
			continue
		}

		if entry.HLC.Compare(beforeHLC) < 0 {
			if err := batch.Delete(ctx, datastore.NewKey(result.Key)); err != nil {
				return err
			}
			deletedCount++
		}
	}

	if err := batch.Commit(ctx); err != nil {
		return err
	}

	fmt.Printf("Compacted %d operation log entries\n", deletedCount)
	return nil
}

// Вспомогательные методы

// updateIndex обновляет индексы для быстрого поиска
func (sol *StorageOperationLog) updateIndex(ctx context.Context, entry *OperationLogEntry) error {

	// Индекс по транзакциям: _oplog_index:txn:{txn_id} -> [operation_keys]
	if entry.TransactionID != "" {
		txnIndexKey := fmt.Sprintf("_oplog_index:txn:%s", entry.TransactionID)

		// Получаем текущий список операций транзакции
		var operations []string
		if data, err := sol.storage.Get(ctx, datastore.NewKey(txnIndexKey)); err == nil {
			json.Unmarshal(data, &operations)
		}

		// Добавляем новую операцию
		operationKey := fmt.Sprintf("%s%d:%d:%s:%s",
			operationLogPrefix,
			entry.HLC.physicalTime,
			entry.HLC.logicalTime,
			entry.HLC.nodeID,
			entry.ID,
		)
		operations = append(operations, operationKey)

		// Сохраняем обновленный индекс
		indexData, _ := json.Marshal(operations)
		sol.storage.Put(ctx, datastore.NewKey(txnIndexKey), indexData)
	}

	// Индекс по коллекциям
	if entry.Collection != "" {
		collIndexKey := fmt.Sprintf("_oplog_index:coll:%s", entry.Collection)

		var operations []string
		if data, err := sol.storage.Get(ctx, datastore.NewKey(collIndexKey)); err == nil {
			json.Unmarshal(data, &operations)
		}

		operationKey := fmt.Sprintf("%s%d:%d:%s:%s",
			operationLogPrefix,
			entry.HLC.physicalTime,
			entry.HLC.logicalTime,
			entry.HLC.nodeID,
			entry.ID,
		)
		operations = append(operations, operationKey)

		// Ограничиваем размер индекса (последние 1000 операций)
		if len(operations) > 1000 {
			operations = operations[len(operations)-1000:]
		}

		indexData, _ := json.Marshal(operations)
		sol.storage.Put(ctx, datastore.NewKey(collIndexKey), indexData)
	}

	return nil
}

func (sol *StorageOperationLog) getOperationsByKeys(ctx context.Context, keys []string) ([]*OperationLogEntry, error) {
	var operations []*OperationLogEntry

	for _, key := range keys {
		data, err := sol.storage.Get(ctx, datastore.NewKey(key))
		if err != nil {
			continue // Ключ мог быть удален
		}

		var entry OperationLogEntry
		if err := json.Unmarshal(data, &entry); err != nil {
			continue
		}

		operations = append(operations, &entry)
	}

	// Сортируем по HLC
	sort.Slice(operations, func(i, j int) bool {
		return operations[i].HLC.Compare(operations[j].HLC) < 0
	})

	return operations, nil
}

func (sol *StorageOperationLog) searchOperationsByField(ctx context.Context, filter func(*OperationLogEntry) bool) ([]*OperationLogEntry, error) {
	results, err := sol.storage.Query(ctx, query.Query{
		Prefix: operationLogPrefix,
	})
	if err != nil {
		return nil, err
	}
	defer results.Close()

	var operations []*OperationLogEntry

	for result := range results.Next() {
		if result.Error != nil {
			continue
		}

		var entry OperationLogEntry
		if err := json.Unmarshal(result.Value, &entry); err != nil {
			continue
		}

		if filter(&entry) {
			operations = append(operations, &entry)
		}
	}

	// Сортируем по HLC
	sort.Slice(operations, func(i, j int) bool {
		return operations[i].HLC.Compare(operations[j].HLC) < 0
	})

	return operations, nil
}

// UpdateFromRemote обновляет локальный HLC при получении удаленного события
func (sol *StorageOperationLog) UpdateFromRemote(remoteHLC *HybridLogicalClock) {
	sol.mu.Lock()
	defer sol.mu.Unlock()

	sol.hlc = sol.hlc.Update(remoteHLC)
}

// GetCurrentHLC возвращает текущий HLC
func (sol *StorageOperationLog) GetCurrentHLC() *HybridLogicalClock {
	sol.mu.RLock()
	defer sol.mu.RUnlock()

	return &HybridLogicalClock{
		physicalTime: sol.hlc.physicalTime,
		logicalTime:  sol.hlc.logicalTime,
		nodeID:       sol.hlc.nodeID,
	}
}

func max(a, b int64, others ...int64) int64 {
	result := a
	if b > result {
		result = b
	}
	for _, v := range others {
		if v > result {
			result = v
		}
	}
	return result
}
