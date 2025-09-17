package mstdatastore

import (
	"context"
	"fmt"
	"sort"

	"github.com/ipfs/go-datastore"
)

// GenerateCollectionProof создает proof читая данные из Storage
func (cmd *MstDatastore) GenerateCollectionProof(ctx context.Context, collectionName string, key datastore.Key) (*CollectionMerkleProof, error) {
	cmd.mu.RLock()
	defer cmd.mu.RUnlock()

	collection, exists := cmd.collections[collectionName]
	if !exists {
		return nil, fmt.Errorf("collection %s not found", collectionName)
	}

	k := key.String()

	// Получаем данные
	dataKey := datastore.NewKey(fmt.Sprintf("%s%s%s%s", collectionPrefix, collectionName, collectionDataKey, k))
	value, err := cmd.storage.Get(ctx, dataKey)
	if err == datastore.ErrNotFound {
		return nil, datastore.ErrNotFound
	}
	if err != nil {
		return nil, err
	}

	// Получаем хеш листа
	leafKey := datastore.NewKey(fmt.Sprintf("%s%s%s%s", collectionPrefix, collectionName, collectionLeafKey, k))
	leafHash, err := cmd.storage.Get(ctx, leafKey)
	if err != nil {
		return nil, err
	}

	// Строим путь до корня
	path, positions, err := cmd.buildCollectionProofPathFromStorage(ctx, collectionName, k)
	if err != nil {
		return nil, err
	}

	proof := &CollectionMerkleProof{
		Collection: collectionName,
		Key:        k,
		Value:      value,
		LeafHash:   leafHash,
		Path:       path,
		Positions:  positions,
		RootHash:   collection.RootHash,
	}

	return proof, nil
}

// buildCollectionProofPathFromStorage строит proof path читая листья из Storage
func (cmd *MstDatastore) buildCollectionProofPathFromStorage(ctx context.Context, collectionName, targetKey string) ([][]byte, []bool, error) {
	// Читаем все листья из Storage (как в buildCollectionTreeFromStorage)
	leafPrefix := datastore.NewKey(fmt.Sprintf("%s%s%s", collectionPrefix, collectionName, collectionLeafKey))

	leafChan, err := cmd.storage.Iterator(ctx, leafPrefix)
	if err != nil {
		return nil, nil, err
	}

	type leafEntry struct {
		key      string
		leafHash []byte
	}

	var leafEntries []leafEntry

	for kv := range leafChan {
		fullKey := kv.Key.String()
		elementKey := fullKey[len(leafPrefix.String()):]

		leafEntries = append(leafEntries, leafEntry{
			key:      elementKey,
			leafHash: kv.Value,
		})
	}

	// Сортируем
	sort.Slice(leafEntries, func(i, j int) bool {
		return leafEntries[i].key < leafEntries[j].key
	})

	// Находим позицию целевого ключа
	targetIndex := -1
	for i, entry := range leafEntries {
		if entry.key == targetKey {
			targetIndex = i
			break
		}
	}

	if targetIndex == -1 {
		return nil, nil, fmt.Errorf("key not found")
	}

	// Строим путь (аналогично оригинальной реализации)
	var path [][]byte
	var positions []bool

	level := make([][]byte, len(leafEntries))
	for i, entry := range leafEntries {
		level[i] = entry.leafHash
	}

	currentIndex := targetIndex

	for len(level) > 1 {
		var siblingHash []byte
		var isRight bool

		if currentIndex%2 == 0 {
			if currentIndex+1 < len(level) {
				siblingHash = level[currentIndex+1]
				isRight = true
			}
		} else {
			siblingHash = level[currentIndex-1]
			isRight = false
		}

		if siblingHash != nil {
			path = append(path, siblingHash)
			positions = append(positions, isRight)
		}

		// Следующий уровень
		nextLevel := make([][]byte, 0, (len(level)+1)/2)
		for i := 0; i < len(level); i += 2 {
			var nodeHash []byte
			if i+1 < len(level) {
				nodeHash = cmd.hashPair(level[i], level[i+1])
			} else {
				nodeHash = level[i]
			}
			nextLevel = append(nextLevel, nodeHash)
		}

		level = nextLevel
		currentIndex = currentIndex / 2
	}

	return path, positions, nil
}
