package collection

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/ipfs/go-datastore"
)

// PutInCollection добавляет элемент в коллекцию
func (cmd *CollectionManager) PutInCollection(ctx context.Context, collectionName string, key datastore.Key, value []byte) error {

	cmd.mu.Lock()
	defer cmd.mu.Unlock()

	collection, exists := cmd.collections[collectionName]
	if !exists {
		return fmt.Errorf("collection %s not found", collectionName)
	}

	k := key.String()

	// Пытаемся использовать транзакцию для атомарности
	if txn, err := cmd.storage.NewTransaction(ctx, false); err == nil {
		defer txn.Discard(ctx)
		return cmd.putInCollectionWithTxn(ctx, txn, collectionName, k, value, collection)
	}

	// Fallback на батч операции
	batch, err := cmd.storage.Batch(ctx)
	if err != nil {
		return err
	}

	return cmd.putInCollectionWithBatch(ctx, batch, collectionName, k, value, collection)
}

func (cmd *CollectionManager) putInCollectionWithTxn(ctx context.Context, txn datastore.Txn, collectionName, k string, value []byte, collection *Collection) error {

	// Сохраняем данные
	dataKey := datastore.NewKey(fmt.Sprintf("%s%s%s%s", collectionPrefix, collectionName, collectionDataKey, k))
	if err := txn.Put(ctx, dataKey, value); err != nil {
		return err
	}

	leafKey := datastore.NewKey(fmt.Sprintf("%s%s%s%s", collectionPrefix, collectionName, collectionLeafKey, k))

	// Проверяем, новый ли это ключ
	_, err := txn.Get(ctx, leafKey)
	isNewKey := err == datastore.ErrNotFound

	// Обновляем/создаем лист
	leafHash := cmd.hashLeaf(k, value)
	if err := txn.Put(ctx, leafKey, leafHash); err != nil {
		return err
	}

	// Пересчитываем дерево коллекции
	// TODO: оптимизировать пересчет дерева, пересчитывая только измененные ветки
	newRoot, err := cmd.buildCollectionTreeFromStorage(ctx, collectionName)
	if err != nil {
		return err
	}

	// Обновляем коллекцию
	if isNewKey {
		collection.ItemsCount++
	}

	collection.UpdatedAt = time.Now()
	collection.RootHash = newRoot

	// Сохраняем обновленную коллекцию
	collectionData, err := json.Marshal(collection)
	if err != nil {
		return err
	}

	collectionKey := datastore.NewKey(fmt.Sprintf("%s%s", collectionPrefix, collectionName))
	if err := txn.Put(ctx, collectionKey, collectionData); err != nil {
		return err
	}

	// Сохраняем root hash
	rootKey := datastore.NewKey(fmt.Sprintf("%s%s%s", collectionPrefix, collectionName, collectionRootKey))
	if err := txn.Put(ctx, rootKey, newRoot); err != nil {
		return err
	}

	return txn.Commit(ctx)
}

func (cmd *CollectionManager) putInCollectionWithBatch(ctx context.Context, batch datastore.Batch, collectionName, k string, value []byte, collection *Collection) error {

	leafKey := datastore.NewKey(fmt.Sprintf("%s%s%s%s", collectionPrefix, collectionName, collectionLeafKey, k))

	// Проверяем, новый ли это ключ
	_, err := cmd.storage.Get(ctx, leafKey)
	isNewKey := err == datastore.ErrNotFound

	// Сохраняем данные
	dataKey := datastore.NewKey(fmt.Sprintf("%s%s%s%s", collectionPrefix, collectionName, collectionDataKey, k))
	if err := batch.Put(ctx, dataKey, value); err != nil {
		return err
	}

	// Обновляем/создаем лист
	leafHash := cmd.hashLeaf(k, value)
	if err := batch.Put(ctx, leafKey, leafHash); err != nil {
		return err
	}

	// Коммитим батч для сохранения листа перед пересчетом дерева
	if err := batch.Commit(ctx); err != nil {
		return err
	}

	// Пересчитываем дерево коллекции
	// TODO: оптимизировать пересчет дерева, пересчитывая только измененные ветки
	newRoot, err := cmd.buildCollectionTreeFromStorage(ctx, collectionName)
	if err != nil {
		return err
	}

	// Создаем новый батч для сохранения метаданных
	newBatch, err := cmd.storage.Batch(ctx)
	if err != nil {
		return err
	}

	// Обновляем коллекцию
	if isNewKey {
		collection.ItemsCount++
	}
	collection.UpdatedAt = time.Now()
	collection.RootHash = newRoot

	// Сохраняем обновленную коллекцию
	collectionData, err := json.Marshal(collection)
	if err != nil {
		return err
	}

	collectionKey := datastore.NewKey(fmt.Sprintf("%s%s", collectionPrefix, collectionName))
	if err := newBatch.Put(ctx, collectionKey, collectionData); err != nil {
		return err
	}

	// Сохраняем root hash
	rootKey := datastore.NewKey(fmt.Sprintf("%s%s%s", collectionPrefix, collectionName, collectionRootKey))
	if err := newBatch.Put(ctx, rootKey, newRoot); err != nil {
		return err
	}

	return newBatch.Commit(ctx)
}
