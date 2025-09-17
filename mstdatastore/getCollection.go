package mstdatastore

import "fmt"

// GetCollection возвращает информацию о коллекции
func (cmd *MstDatastore) GetCollection(name string) (*Collection, error) {
	cmd.mu.RLock()
	defer cmd.mu.RUnlock()

	collection, exists := cmd.collections[name]
	if !exists {
		return nil, fmt.Errorf("collection %s not found", name)
	}

	collectionCopy := *collection
	return &collectionCopy, nil
}
