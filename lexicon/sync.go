package lexicon

import (
	"context"
	"encoding/json"
	"fmt"
	"pds/oplog"
	"time"

	"github.com/ipfs/go-datastore"
)

// SyncManager управляет синхронизацией лексиконов между узлами
type SyncManager struct {
	lexiconService *LexiconService
	operationLog   oplog.OperationLog
	nodeID         string
}

// SyncEvent событие синхронизации
type SyncEvent struct {
	Type       string      `json:"type"` // "schema_registered", "record_created", "record_updated", "record_deleted"
	LexiconID  LexiconID   `json:"lexiconId"`
	RecordID   RecordID    `json:"recordId,omitempty"`
	Data       interface{} `json:"data,omitempty"`
	NodeID     string      `json:"nodeId"`
	Timestamp  time.Time   `json:"timestamp"`
	SchemaHash string      `json:"schemaHash,omitempty"`
}

// NewSyncManager создает новый менеджер синхронизации
func NewSyncManager(ls *LexiconService, opLog oplog.OperationLog, nodeID string) *SyncManager {
	return &SyncManager{
		lexiconService: ls,
		operationLog:   opLog,
		nodeID:         nodeID,
	}
}

// SyncSchema синхронизирует схему с удаленного узла
func (sm *SyncManager) SyncSchema(ctx context.Context, remoteDef *LexiconDefinition, fromNodeID string) error {
	// Проверяем, есть ли у нас эта схема
	localDef, err := sm.lexiconService.GetSchema(ctx, remoteDef.ID)

	if err != nil {
		// Схемы нет локально - регистрируем
		if err := sm.lexiconService.RegisterSchema(ctx, remoteDef); err != nil {
			return fmt.Errorf("failed to register synced schema: %w", err)
		}

		// Логируем операцию
		sm.logSchemaSync(ctx, "schema_registered", remoteDef, fromNodeID)

		return nil
	}

	// Проверяем версии
	if sm.isNewerVersion(remoteDef.Version, localDef.Version) {
		// Удаленная версия новее - обновляем
		if err := sm.lexiconService.RegisterSchema(ctx, remoteDef); err != nil {
			return fmt.Errorf("failed to update schema from sync: %w", err)
		}

		sm.logSchemaSync(ctx, "schema_updated", remoteDef, fromNodeID)
	}

	return nil
}

// SyncRecord синхронизирует запись с удаленного узла
func (sm *SyncManager) SyncRecord(ctx context.Context, record *TypedRecord, fromNodeID string) error {

	// Проверяем, есть ли у нас схема для этой записи
	_, err := sm.lexiconService.GetSchema(ctx, record.LexiconID)
	if err != nil {
		return fmt.Errorf("schema not found for synced record: %s", record.LexiconID)
	}

	// Проверяем, существует ли запись локально
	existingRecord, err := sm.lexiconService.GetRecord(ctx, record.LexiconID, record.ID)

	if err != nil {
		// Записи нет - создаем новую
		// Но используем существующий ID и метаданные из удаленной записи
		if err := sm.createSyncedRecord(ctx, record); err != nil {
			return fmt.Errorf("failed to create synced record: %w", err)
		}

		sm.logRecordSync(ctx, "record_created", record, fromNodeID)
		return nil
	}

	// Запись существует - сравниваем время обновления
	if record.UpdatedAt.After(existingRecord.UpdatedAt) {
		// Удаленная запись новее - обновляем
		if err := sm.updateSyncedRecord(ctx, record); err != nil {
			return fmt.Errorf("failed to update synced record: %w", err)
		}

		sm.logRecordSync(ctx, "record_updated", record, fromNodeID)
	}

	return nil
}

// DeleteSyncedRecord обрабатывает удаление записи с удаленного узла
func (sm *SyncManager) DeleteSyncedRecord(ctx context.Context, lexiconID LexiconID, recordID RecordID, fromNodeID string) error {
	// Удаляем запись если она существует
	if err := sm.lexiconService.DeleteRecord(ctx, lexiconID, recordID); err != nil {
		// Игнорируем ошибку если запись не найдена
		if err.Error() != fmt.Sprintf("record not found: %s/%s", lexiconID, recordID) {
			return fmt.Errorf("failed to delete synced record: %w", err)
		}
	}

	// Логируем операцию
	sm.logRecordSync(ctx, "record_deleted", &TypedRecord{
		ID:        recordID,
		LexiconID: lexiconID,
	}, fromNodeID)

	return nil
}

// GetSyncEvents получает события синхронизации для отправки удаленному узлу
func (sm *SyncManager) GetSyncEvents(ctx context.Context, since *oplog.HybridLogicalClock) ([]*SyncEvent, error) {
	// Получаем операции из лога
	operations, err := sm.operationLog.GetOperations(ctx, since, nil)
	if err != nil {
		return nil, fmt.Errorf("failed to get operations: %w", err)
	}

	var events []*SyncEvent

	for _, op := range operations {
		// Конвертируем операции в события синхронизации
		event, err := sm.operationToSyncEvent(op)
		if err != nil {
			continue // Пропускаем проблемные операции
		}

		if event != nil {
			events = append(events, event)
		}
	}

	return events, nil
}

// ProcessSyncEvents обрабатывает входящие события синхронизации
func (sm *SyncManager) ProcessSyncEvents(ctx context.Context, events []*SyncEvent) error {
	for _, event := range events {
		if err := sm.processSyncEvent(ctx, event); err != nil {
			// Логируем ошибку, но продолжаем обработку
			fmt.Printf("Failed to process sync event %s: %v\n", event.Type, err)
		}
	}
	return nil
}

// GetSchemaManifest возвращает манифест всех локальных схем
func (sm *SyncManager) GetSchemaManifest(ctx context.Context) (map[LexiconID]string, error) {
	schemas, err := sm.lexiconService.ListSchemas(ctx)
	if err != nil {
		return nil, err
	}

	manifest := make(map[LexiconID]string)

	for _, schema := range schemas {
		// Вычисляем хеш схемы для сравнения
		schemaData, _ := json.Marshal(schema)
		manifest[schema.ID] = fmt.Sprintf("%x", schemaData)
	}

	return manifest, nil
}

// CompareSchemaManifests сравнивает локальный манифест с удаленным
func (sm *SyncManager) CompareSchemaManifests(localManifest, remoteManifest map[LexiconID]string) []LexiconID {
	var diffSchemas []LexiconID

	// Проверяем схемы которые есть удаленно, но нет локально или они отличаются
	for lexiconID, remoteHash := range remoteManifest {
		if localHash, exists := localManifest[lexiconID]; !exists || localHash != remoteHash {
			diffSchemas = append(diffSchemas, lexiconID)
		}
	}

	return diffSchemas
}

// === ВСПОМОГАТЕЛЬНЫЕ МЕТОДЫ ===

func (sm *SyncManager) createSyncedRecord(ctx context.Context, record *TypedRecord) error {
	// Валидируем данные
	schema, err := sm.lexiconService.GetSchema(ctx, record.LexiconID)
	if err != nil {
		return err
	}

	if err := sm.lexiconService.validator.ValidateRecord(schema, record.Data); err != nil {
		return err
	}

	// Создаем запись напрямую в MST (обходим нормальный CreateRecord чтобы сохранить метаданные)
	recordData, err := json.Marshal(record)
	if err != nil {
		return err
	}

	recordKey := datastore.NewKey(fmt.Sprintf("%s%s:%s", RecordPrefix, record.LexiconID, record.ID))
	if err := sm.lexiconService.mst.PutInCollection(ctx, RecordCollection, recordKey, recordData); err != nil {
		return err
	}

	// Индексируем запись
	if sm.lexiconService.indexer != nil {
		go sm.lexiconService.indexRecord(record, recordKey.String(), recordData)
	}

	return nil
}

func (sm *SyncManager) updateSyncedRecord(ctx context.Context, record *TypedRecord) error {
	// Аналогично создаю, но с обновлением существующей записи
	schema, err := sm.lexiconService.GetSchema(ctx, record.LexiconID)
	if err != nil {
		return err
	}

	if err := sm.lexiconService.validator.ValidateRecord(schema, record.Data); err != nil {
		return err
	}

	recordData, err := json.Marshal(record)
	if err != nil {
		return err
	}

	recordKey := datastore.NewKey(fmt.Sprintf("%s%s:%s", RecordPrefix, record.LexiconID, record.ID))
	if err := sm.lexiconService.mst.PutInCollection(ctx, RecordCollection, recordKey, recordData); err != nil {
		return err
	}

	// Переиндексируем
	if sm.lexiconService.indexer != nil {
		go sm.lexiconService.indexRecord(record, recordKey.String(), recordData)
	}

	return nil
}

func (sm *SyncManager) logSchemaSync(ctx context.Context, eventType string, schema *LexiconDefinition, fromNodeID string) {
	schemaData, _ := json.Marshal(schema)

	entry := &oplog.OperationLogEntry{
		TransactionID: oplog.GenerateTransactionID(),
		Operation:     oplog.OperationType(eventType),
		Key:           fmt.Sprintf("schema:%s", schema.ID),
		Value:         schemaData,
		Collection:    SchemaCollection,
		Metadata: map[string]interface{}{
			"type":         "schema_sync",
			"lexicon_id":   string(schema.ID),
			"version":      schema.Version,
			"from_node_id": fromNodeID,
		},
	}

	sm.operationLog.LogOperation(ctx, entry)
}

func (sm *SyncManager) logRecordSync(ctx context.Context, eventType string, record *TypedRecord, fromNodeID string) {
	recordData, _ := json.Marshal(record)

	entry := &oplog.OperationLogEntry{
		TransactionID: oplog.GenerateTransactionID(),
		Operation:     oplog.OperationType(eventType),
		Key:           fmt.Sprintf("record:%s:%s", record.LexiconID, record.ID),
		Value:         recordData,
		Collection:    RecordCollection,
		Metadata: map[string]interface{}{
			"type":         "record_sync",
			"lexicon_id":   string(record.LexiconID),
			"record_id":    string(record.ID),
			"from_node_id": fromNodeID,
		},
	}

	sm.operationLog.LogOperation(ctx, entry)
}

func (sm *SyncManager) operationToSyncEvent(op *oplog.OperationLogEntry) (*SyncEvent, error) {
	// Конвертируем операцию лога в событие синхронизации
	if op.Metadata == nil {
		return nil, nil
	}

	syncType, exists := op.Metadata["type"].(string)
	if !exists || (syncType != "schema_sync" && syncType != "record_sync") {
		return nil, nil
	}

	event := &SyncEvent{
		Type:      string(op.Operation),
		NodeID:    sm.nodeID,
		Timestamp: op.CreatedAt,
	}

	if syncType == "schema_sync" {
		if lexiconIDStr, ok := op.Metadata["lexicon_id"].(string); ok {
			event.LexiconID = LexiconID(lexiconIDStr)
		}

		if len(op.Value) > 0 {
			var schema LexiconDefinition
			if json.Unmarshal(op.Value, &schema) == nil {
				event.Data = &schema
				event.SchemaHash = fmt.Sprintf("%x", op.Value)
			}
		}
	} else if syncType == "record_sync" {
		if lexiconIDStr, ok := op.Metadata["lexicon_id"].(string); ok {
			event.LexiconID = LexiconID(lexiconIDStr)
		}
		if recordIDStr, ok := op.Metadata["record_id"].(string); ok {
			event.RecordID = RecordID(recordIDStr)
		}

		if len(op.Value) > 0 && op.Operation != oplog.OpTypeDelete {
			var record TypedRecord
			if json.Unmarshal(op.Value, &record) == nil {
				event.Data = &record
			}
		}
	}

	return event, nil
}

func (sm *SyncManager) processSyncEvent(ctx context.Context, event *SyncEvent) error {
	switch event.Type {
	case "schema_registered", "schema_updated":
		if schema, ok := event.Data.(*LexiconDefinition); ok {
			return sm.SyncSchema(ctx, schema, event.NodeID)
		}

	case "record_created", "record_updated":
		if record, ok := event.Data.(*TypedRecord); ok {
			return sm.SyncRecord(ctx, record, event.NodeID)
		}

	case "record_deleted":
		return sm.DeleteSyncedRecord(ctx, event.LexiconID, event.RecordID, event.NodeID)
	}

	return fmt.Errorf("unknown sync event type: %s", event.Type)
}

func (sm *SyncManager) isNewerVersion(remoteVersion, localVersion string) bool {
	// Упрощенное сравнение версий - в реальности нужен более сложный алгоритм
	return remoteVersion > localVersion
}

// BatchSyncRecords синхронизирует множество записей одной операцией
func (sm *SyncManager) BatchSyncRecords(ctx context.Context, records []*TypedRecord, fromNodeID string) error {
	if len(records) == 0 {
		return nil
	}

	// Группируем по лексиконам
	recordsByLexicon := make(map[LexiconID][]*TypedRecord)
	for _, record := range records {
		recordsByLexicon[record.LexiconID] = append(recordsByLexicon[record.LexiconID], record)
	}

	// Обрабатываем по группам
	for lexiconID, lexiconRecords := range recordsByLexicon {
		// Проверяем схему
		_, err := sm.lexiconService.GetSchema(ctx, lexiconID)
		if err != nil {
			fmt.Printf("Skipping records for unknown lexicon %s: %v\n", lexiconID, err)
			continue
		}

		// Синхронизируем записи этого лексикона
		for _, record := range lexiconRecords {
			if err := sm.SyncRecord(ctx, record, fromNodeID); err != nil {
				fmt.Printf("Failed to sync record %s: %v\n", record.ID, err)
			}
		}
	}

	return nil
}

// GetConflictingRecords находит записи с конфликтами версий
func (sm *SyncManager) GetConflictingRecords(ctx context.Context, remoteRecords []*TypedRecord) ([]*RecordConflict, error) {
	var conflicts []*RecordConflict

	for _, remoteRecord := range remoteRecords {
		localRecord, err := sm.lexiconService.GetRecord(ctx, remoteRecord.LexiconID, remoteRecord.ID)
		if err != nil {
			continue // Запись не существует локально - не конфликт
		}

		// Проверяем конфликт временных меток
		if !localRecord.UpdatedAt.Equal(remoteRecord.UpdatedAt) {
			conflict := &RecordConflict{
				LexiconID:    remoteRecord.LexiconID,
				RecordID:     remoteRecord.ID,
				LocalRecord:  localRecord,
				RemoteRecord: remoteRecord,
				ConflictType: sm.determineConflictType(localRecord, remoteRecord),
			}
			conflicts = append(conflicts, conflict)
		}
	}

	return conflicts, nil
}

// RecordConflict представляет конфликт между локальной и удаленной записью
type RecordConflict struct {
	LexiconID    LexiconID    `json:"lexiconId"`
	RecordID     RecordID     `json:"recordId"`
	LocalRecord  *TypedRecord `json:"localRecord"`
	RemoteRecord *TypedRecord `json:"remoteRecord"`
	ConflictType string       `json:"conflictType"` // "newer_local", "newer_remote", "concurrent"
}

func (sm *SyncManager) determineConflictType(local, remote *TypedRecord) string {
	if local.UpdatedAt.After(remote.UpdatedAt) {
		return "newer_local"
	}
	if remote.UpdatedAt.After(local.UpdatedAt) {
		return "newer_remote"
	}
	return "concurrent"
}
