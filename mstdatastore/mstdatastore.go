package mstdatastore

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	s "pds/datastore"
	"sort"
	"sync"
	"time"

	"github.com/ipfs/go-datastore"
	"lukechampine.com/blake3"
)

const (
	collectionsKey    = "_collections"
	collectionPrefix  = "_collection:"
	collectionRootKey = ":root"
	collectionDataKey = ":data:"
	collectionLeafKey = ":leaf:"
	collectionNodeKey = ":node:"
)

// Collection представляет коллекцию с метаданными
type Collection struct {
	Name       string            `json:"name"`
	CreatedAt  time.Time         `json:"created_at"`
	UpdatedAt  time.Time         `json:"updated_at"`
	ItemsCount int               `json:"items_count"`
	RootHash   []byte            `json:"root_hash,omitempty"`
	Metadata   map[string]string `json:"metadata,omitempty"`
}

// CollectionMerkleProof представляет доказательство включения в коллекцию
type CollectionMerkleProof struct {
	Collection string   `json:"collection"`
	Key        string   `json:"key"`
	Value      []byte   `json:"value"`
	LeafHash   []byte   `json:"leaf_hash"`
	Path       [][]byte `json:"path"`
	Positions  []bool   `json:"positions"`
	RootHash   []byte   `json:"root_hash"`
}

// MerkleNode узел дерева
type MerkleNode struct {
	Hash  []byte `json:"hash"`
	Left  []byte `json:"left,omitempty"`
	Right []byte `json:"right,omitempty"`
}

// KeyValue структура для итератора
type KeyValue struct {
	Key   datastore.Key
	Value []byte
}

var _ datastore.Batching = (*MstDatastore)(nil)

// MstDatastore БЕЗ cmd.leaves в памяти
type MstDatastore struct {
	storage     s.Datastore
	collections map[string]*Collection // только метаданные коллекций
	mu          sync.RWMutex
}

// NewMstDatastore создает новый memory-efficient datastore
func NewMstDatastore(storage s.Datastore) (*MstDatastore, error) {
	cmd := &MstDatastore{
		storage:     storage,
		collections: make(map[string]*Collection),
	}
	// Загружаем только метаданные коллекций (не листья!)
	err := cmd.loadCollections(context.Background())
	return cmd, err
}

// HasCollection проверяет существование коллекции
func (cmd *MstDatastore) HasCollection(name string) bool {
	cmd.mu.RLock()
	defer cmd.mu.RUnlock()
	_, exists := cmd.collections[name]
	return exists
}

// === ВСПОМОГАТЕЛЬНЫЕ МЕТОДЫ ===

func (cmd *MstDatastore) loadCollections(ctx context.Context) error {
	data, err := cmd.storage.Get(ctx, datastore.NewKey(collectionsKey))
	if err == datastore.ErrNotFound {
		return cmd.CreateCollection("_system", map[string]string{
			"description": "Default collection",
		})
	}
	if err != nil {
		return err
	}
	var collectionNames []string
	if err := json.Unmarshal(data, &collectionNames); err != nil {
		return err
	}
	// Загружаем ТОЛЬКО метаданные коллекций (не листья!)
	for _, name := range collectionNames {
		if err := cmd.loadCollectionMetadata(ctx, name); err != nil {
			return err
		}
	}
	return nil
}

// buildCollectionTreeFromStorage строит Merkle Tree читая листья из Storage
func (cmd *MstDatastore) buildCollectionTreeFromStorage(ctx context.Context, collectionName string) ([]byte, error) {

	fmt.Printf("🔍 Строим дерево для коллекции '%s' (читаем из Storage)\n", collectionName)

	// Префикс для поиска всех листьев коллекции
	leafPrefix := datastore.NewKey(fmt.Sprintf("%s%s%s", collectionPrefix, collectionName, collectionLeafKey))
	fmt.Printf("🔍 Ищем листья с префиксом: %s\n", leafPrefix.String())

	// Используем Iterator для получения всех листьев
	leafChan, err := cmd.storage.Iterator(ctx, leafPrefix)
	if err != nil {
		return nil, fmt.Errorf("failed to create iterator: %w", err)
	}

	// Собираем ключи и хеши листьев из Storage
	type leafEntry struct {
		key      string
		leafHash []byte
	}

	var leafEntries []leafEntry

	for kv := range leafChan {
		// Извлекаем ключ элемента из полного ключа
		fullKey := kv.Key.String()
		elementKey := fullKey[len(leafPrefix.String()):]

		leafEntries = append(leafEntries, leafEntry{
			key:      elementKey,
			leafHash: kv.Value,
		})

		fmt.Printf("  📄 Найден лист: %s → %x\n", elementKey, kv.Value[:4])
	}

	// Если нет листьев - возвращаем nil
	if len(leafEntries) == 0 {
		fmt.Println("📭 Коллекция пустая, возвращаем nil")
		return nil, nil
	}

	fmt.Printf("📊 Всего листьев найдено: %d\n", len(leafEntries))

	// Сортируем по ключам (детерминированность!)
	sort.Slice(leafEntries, func(i, j int) bool {
		return leafEntries[i].key < leafEntries[j].key
	})

	// Создаем отсортированный массив хешей
	level := make([][]byte, len(leafEntries))
	for i, entry := range leafEntries {
		level[i] = entry.leafHash
		fmt.Printf("  %d: %s → %x\n", i, entry.key, entry.leafHash[:4])
	}

	// Строим дерево bottom-up
	fmt.Println("🏗️ Строим дерево bottom-up...")
	levelNum := 0

	for len(level) > 1 {
		fmt.Printf("  Уровень %d: %d узлов → ", levelNum, len(level))

		nextLevel := make([][]byte, 0, (len(level)+1)/2)

		for i := 0; i < len(level); i += 2 {
			var nodeHash []byte

			if i+1 < len(level) {
				// Парный узел
				nodeHash = cmd.hashPair(level[i], level[i+1])
				fmt.Printf("[%x+%x→%x] ", level[i][:2], level[i+1][:2], nodeHash[:2])
			} else {
				// Непарный узел
				nodeHash = level[i]
				fmt.Printf("[%x→%x] ", level[i][:2], nodeHash[:2])
			}

			// Сохраняем узел в Storage
			node := &MerkleNode{
				Hash: nodeHash,
				Left: level[i],
			}
			if i+1 < len(level) {
				node.Right = level[i+1]
			}

			nodeData, _ := json.Marshal(node)
			nodeKeyStr := fmt.Sprintf("%s%s%s%x", collectionPrefix, collectionName, collectionNodeKey, nodeHash)
			nodeKey := datastore.NewKey(nodeKeyStr)
			cmd.storage.Put(ctx, nodeKey, nodeData)

			nextLevel = append(nextLevel, nodeHash)
		}

		level = nextLevel
		levelNum++
		fmt.Printf("%d узлов\n", len(level))
	}

	rootHash := level[0]
	fmt.Printf("🎉 Root hash: %x\n", rootHash[:8])

	return rootHash, nil
}

func (cmd *MstDatastore) loadCollectionMetadata(ctx context.Context, name string) error {
	collectionKey := datastore.NewKey(fmt.Sprintf("%s%s", collectionPrefix, name))
	data, err := cmd.storage.Get(ctx, collectionKey)
	if err != nil {
		return err
	}
	var collection Collection
	if err := json.Unmarshal(data, &collection); err != nil {
		return err
	}
	cmd.collections[name] = &collection
	// НЕ загружаем листья в память!
	return nil
}

func (cmd *MstDatastore) getCollectionNames() []string {
	names := make([]string, 0, len(cmd.collections))
	for name := range cmd.collections {
		names = append(names, name)
	}
	sort.Strings(names)
	return names
}

func (cmd *MstDatastore) hashLeaf(key string, value []byte) []byte {
	hasher := blake3.New(32, nil)
	hasher.Write([]byte(key))
	hasher.Write(value)
	return hasher.Sum(nil)
}

func (cmd *MstDatastore) hashPair(left, right []byte) []byte {
	hasher := blake3.New(32, nil)
	hasher.Write(left)
	hasher.Write(right)
	return hasher.Sum(nil)
}

func (cmd *MstDatastore) verifyProofStatic(proof *CollectionMerkleProof, rootHash []byte) bool {
	expectedLeafHash := cmd.hashLeaf(proof.Key, proof.Value)
	if !bytes.Equal(proof.LeafHash, expectedLeafHash) {
		return false
	}

	currentHash := proof.LeafHash
	for i, siblingHash := range proof.Path {
		if proof.Positions[i] {
			currentHash = cmd.hashPair(currentHash, siblingHash)
		} else {
			currentHash = cmd.hashPair(siblingHash, currentHash)
		}
	}

	return bytes.Equal(currentHash, rootHash)
}
