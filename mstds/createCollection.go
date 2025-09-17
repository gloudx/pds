package mstds

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/ipfs/go-datastore"
)

// CreateCollection создает новую коллекцию с заданным именем и метаданными.
func (cmd *MstDs) CreateCollection(name string, metadata map[string]string) error {

	cmd.mu.Lock()
	defer cmd.mu.Unlock()

	if _, exists := cmd.collections[name]; exists {
		return fmt.Errorf("collection %s already exists", name)
	}

	ctx := context.Background()
	collection := &Collection{
		Name:       name,
		CreatedAt:  time.Now(),
		UpdatedAt:  time.Now(),
		ItemsCount: 0,
		Metadata:   metadata,
	}

	collectionData, err := json.Marshal(collection)
	if err != nil {
		return err
	}

	// Используем транзакцию если доступна
	if txn, err := cmd.storage.NewTransaction(ctx, false); err == nil {
		defer txn.Discard(ctx)

		cmd.collections[name] = collection

		collectionNamesData, err := json.Marshal(cmd.getCollectionNames())
		if err != nil {
			return err
		}

		if err := txn.Put(ctx, datastore.NewKey(collectionsKey), collectionNamesData); err != nil {
			return err
		}

		collectionKey := datastore.NewKey(fmt.Sprintf("%s%s", collectionPrefix, name))
		if err := txn.Put(ctx, collectionKey, collectionData); err != nil {
			return err
		}

		return txn.Commit(ctx)
	}

	// Fallback на обычные операции
	cmd.collections[name] = collection

	collectionsData, err := json.Marshal(cmd.getCollectionNames())
	if err != nil {
		return err
	}

	if err := cmd.storage.Put(ctx, datastore.NewKey(collectionsKey), collectionsData); err != nil {
		return err
	}

	collectionKey := datastore.NewKey(fmt.Sprintf("%s%s", collectionPrefix, name))
	
	return cmd.storage.Put(ctx, collectionKey, collectionData)
}
