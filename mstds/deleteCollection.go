package mstds

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/ipfs/go-datastore"
	"github.com/ipfs/go-datastore/query"
)

// DeleteCollection удаляет коллекцию и все связанные с ней данные.
func (cmd *MstDs) DeleteCollection(name string) error {
	
	cmd.mu.Lock()
	defer cmd.mu.Unlock()

	if _, exists := cmd.collections[name]; !exists {
		return fmt.Errorf("collection %s not found", name)
	}

	ctx := context.Background()
	prefix := datastore.NewKey(fmt.Sprintf("%s%s:", collectionPrefix, name))

	// Получаем все ключи коллекции
	results, err := cmd.storage.Query(ctx, query.Query{
		Prefix: prefix.String(),
	})
	if err != nil {
		return err
	}
	defer results.Close()

	// Собираем ключи для удаления
	var keysToDelete []datastore.Key
	for result := range results.Next() {
		if result.Error != nil {
			return result.Error
		}
		keysToDelete = append(keysToDelete, datastore.NewKey(result.Key))
	}

	// Используем батч для удаления
	batch, err := cmd.storage.Batch(ctx)
	if err != nil {
		return err
	}

	for _, key := range keysToDelete {
		if err := batch.Delete(ctx, key); err != nil {
			return err
		}
	}

	delete(cmd.collections, name)

	collectionsData, err := json.Marshal(cmd.getCollectionNames())
	if err != nil {
		return err
	}

	if err := batch.Put(ctx, datastore.NewKey(collectionsKey), collectionsData); err != nil {
		return err
	}

	return batch.Commit(ctx)
}
