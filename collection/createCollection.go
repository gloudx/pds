package collection

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/ipfs/go-datastore"
)

// CreateCollection создает новую коллекцию с заданным именем и метаданными.
func (cm *CollectionManager) CreateCollection(name string, metadata map[string]string) error {
	cm.mu.Lock()
	defer cm.mu.Unlock()
	if _, exists := cm.collections[name]; exists {
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
	cm.collections[name] = collection
	collectionKey := datastore.NewKey(fmt.Sprintf("%s%s", collectionPrefix, name))
	return cm.storage.Put(ctx, collectionKey, collectionData)
}
