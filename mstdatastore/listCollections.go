package mstdatastore

// ListCollections возвращает список всех коллекций
func (cmd *MstDatastore) ListCollections() []*Collection {
	cmd.mu.RLock()
	defer cmd.mu.RUnlock()

	collections := make([]*Collection, 0, len(cmd.collections))
	for _, collection := range cmd.collections {
		collectionCopy := *collection
		collections = append(collections, &collectionCopy)
	}

	return collections
}
