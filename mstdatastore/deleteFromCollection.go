package mstdatastore

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/ipfs/go-datastore"
)

// DeleteFromCollection удаляет элемент из коллекции
func (cmd *MstDatastore) DeleteFromCollection(ctx context.Context, collectionName string, key datastore.Key) error {
	cmd.mu.Lock()
	defer cmd.mu.Unlock()

	collection, exists := cmd.collections[collectionName]
	if !exists {
		return fmt.Errorf("collection %s not found", collectionName)
	}

	k := key.String()

	// Проверяем существование ключа
	leafKey := datastore.NewKey(fmt.Sprintf("%s%s%s%s", collectionPrefix, collectionName, collectionLeafKey, k))
	_, err := cmd.storage.Get(ctx, leafKey)
	wasExists := err != datastore.ErrNotFound

	batch, err := cmd.storage.Batch(ctx)
	if err != nil {
		return err
	}

	// Удаляем данные
	dataKey := datastore.NewKey(fmt.Sprintf("%s%s%s%s", collectionPrefix, collectionName, collectionDataKey, k))
	if err := batch.Delete(ctx, dataKey); err != nil {
		return err
	}

	// Удаляем лист
	if err := batch.Delete(ctx, leafKey); err != nil {
		return err
	}

	// Коммитим удаление
	if err := batch.Commit(ctx); err != nil {
		return err
	}

	// Пересчитываем дерево
	newRoot, err := cmd.buildCollectionTreeFromStorage(ctx, collectionName)
	if err != nil {
		return err
	}

	// Создаем новый батч для метаданных
	newBatch, err := cmd.storage.Batch(ctx)
	if err != nil {
		return err
	}

	// Обновляем коллекцию
	if wasExists {
		collection.ItemsCount--
	}
	collection.UpdatedAt = time.Now()
	collection.RootHash = newRoot

	// Сохраняем коллекцию
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
