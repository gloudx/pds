package collection

import (
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
	// collectionsKey    = "_collections"
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

// CollectionManager управляет коллекциями
type CollectionManager struct {
	storage     s.Datastore
	collections map[string]*Collection // только метаданные коллекций
	mu          sync.RWMutex
}

// NewCollectionManager создает новый менеджер коллекций
func NewCollectionManager(storage s.Datastore) (*CollectionManager, error) {
	cm := &CollectionManager{
		storage:     storage,
		collections: make(map[string]*Collection),
	}
	err := cm.loadCollections(context.Background())
	return cm, err
}

func (cm *CollectionManager) loadCollections(ctx context.Context) error {
	collections, err := cm.storage.Keys(ctx, datastore.NewKey(collectionPrefix))
	if err != nil {
		return err
	}
	for _, col := range collections {
		if err := cm.loadCollectionMetadata(ctx, col); err != nil {
			return err
		}
	}
	if len(cm.collections) == 0 {
		return cm.CreateCollection("_system", map[string]string{
			"description": "Default collection",
		})
	}
	return nil
}

func (cmd *CollectionManager) loadCollectionMetadata(ctx context.Context, k datastore.Key) error {
	data, err := cmd.storage.Get(ctx, k)
	if err != nil {
		return err
	}
	var collection Collection
	if err := json.Unmarshal(data, &collection); err != nil {
		return err
	}
	name := k.String()[len(collectionPrefix):]
	cmd.collections[name] = &collection
	return nil
}

// buildCollectionTreeFromStorage строит Merkle Tree читая листья из Storage
func (cmd *CollectionManager) buildCollectionTreeFromStorage(ctx context.Context, collectionName string) ([]byte, error) {

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

func (cmd *CollectionManager) hashLeaf(key string, value []byte) []byte {
	hasher := blake3.New(32, nil)
	hasher.Write([]byte(key))
	hasher.Write(value)
	return hasher.Sum(nil)
}

func (cmd *CollectionManager) hashPair(left, right []byte) []byte {
	hasher := blake3.New(32, nil)
	hasher.Write(left)
	hasher.Write(right)
	return hasher.Sum(nil)
}
