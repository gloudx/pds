package mstdatastore

import (
	"fmt"
)

// GetCollectionRootHash возвращает root hash коллекции
func (cmd *MstDatastore) GetCollectionRootHash(collectionName string) ([]byte, error) {
	cmd.mu.RLock()
	defer cmd.mu.RUnlock()

	collection, exists := cmd.collections[collectionName]
	if !exists {
		return nil, fmt.Errorf("collection %s not found", collectionName)
	}

	if len(collection.RootHash) == 0 {
		return nil, nil
	}

	result := make([]byte, len(collection.RootHash))
	copy(result, collection.RootHash)
	return result, nil
}
