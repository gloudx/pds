package mstds

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	s "pds/store"
	"sort"
	"sync"
	"time"

	"github.com/ipfs/go-datastore"
	"github.com/ipfs/go-datastore/query"
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

var _ datastore.Batching = (*MstDs)(nil)

// MstDs БЕЗ cmd.leaves в памяти
type MstDs struct {
	storage     s.Storage
	collections map[string]*Collection // только метаданные коллекций
	mu          sync.RWMutex
}

// NewMstDs создает новый memory-efficient datastore
func NewMstDs(storage s.Storage) (*MstDs, error) {
	cmd := &MstDs{
		storage:     storage,
		collections: make(map[string]*Collection),
	}

	// Загружаем только метаданные коллекций (не листья!)
	err := cmd.loadCollections(context.Background())
	return cmd, err
}

// === УПРАВЛЕНИЕ КОЛЛЕКЦИЯМИ ===

// ListCollections возвращает список всех коллекций
func (cmd *MstDs) ListCollections() []*Collection {
	cmd.mu.RLock()
	defer cmd.mu.RUnlock()

	collections := make([]*Collection, 0, len(cmd.collections))
	for _, collection := range cmd.collections {
		collectionCopy := *collection
		collections = append(collections, &collectionCopy)
	}

	return collections
}

// GetCollection возвращает информацию о коллекции
func (cmd *MstDs) GetCollection(name string) (*Collection, error) {
	cmd.mu.RLock()
	defer cmd.mu.RUnlock()

	collection, exists := cmd.collections[name]
	if !exists {
		return nil, fmt.Errorf("collection %s not found", name)
	}

	collectionCopy := *collection
	return &collectionCopy, nil
}

// === РАБОТА С ДАННЫМИ В КОЛЛЕКЦИЯХ ===

// PutInCollection добавляет элемент в коллекцию
func (cmd *MstDs) PutInCollection(ctx context.Context, collectionName string, key datastore.Key, value []byte) error {
	cmd.mu.Lock()
	defer cmd.mu.Unlock()

	collection, exists := cmd.collections[collectionName]
	if !exists {
		return fmt.Errorf("collection %s not found", collectionName)
	}

	k := key.String()

	// Пытаемся использовать транзакцию для атомарности
	if txn, err := cmd.storage.NewTransaction(ctx, false); err == nil {
		defer txn.Discard(ctx)
		return cmd.putInCollectionWithTxn(ctx, txn, collectionName, k, value, collection)
	}

	// Fallback на батч операции
	batch, err := cmd.storage.Batch(ctx)
	if err != nil {
		return err
	}

	return cmd.putInCollectionWithBatch(ctx, batch, collectionName, k, value, collection)
}

func (cmd *MstDs) putInCollectionWithTxn(ctx context.Context, txn datastore.Txn, collectionName, k string, value []byte, collection *Collection) error {
	// Сохраняем данные
	dataKey := datastore.NewKey(fmt.Sprintf("%s%s%s%s", collectionPrefix, collectionName, collectionDataKey, k))
	if err := txn.Put(ctx, dataKey, value); err != nil {
		return err
	}

	// Проверяем, новый ли это ключ
	leafKey := datastore.NewKey(fmt.Sprintf("%s%s%s%s", collectionPrefix, collectionName, collectionLeafKey, k))
	_, err := txn.Get(ctx, leafKey)
	isNewKey := err == datastore.ErrNotFound

	// Обновляем/создаем лист
	leafHash := cmd.hashLeaf(k, value)
	if err := txn.Put(ctx, leafKey, leafHash); err != nil {
		return err
	}

	// Пересчитываем дерево коллекции
	newRoot, err := cmd.buildCollectionTreeFromStorage(ctx, collectionName)
	if err != nil {
		return err
	}

	// Обновляем коллекцию
	if isNewKey {
		collection.ItemsCount++
	}
	collection.UpdatedAt = time.Now()
	collection.RootHash = newRoot

	// Сохраняем обновленную коллекцию
	collectionData, err := json.Marshal(collection)
	if err != nil {
		return err
	}

	collectionKey := datastore.NewKey(fmt.Sprintf("%s%s", collectionPrefix, collectionName))
	if err := txn.Put(ctx, collectionKey, collectionData); err != nil {
		return err
	}

	// Сохраняем root hash
	rootKey := datastore.NewKey(fmt.Sprintf("%s%s%s", collectionPrefix, collectionName, collectionRootKey))
	if err := txn.Put(ctx, rootKey, newRoot); err != nil {
		return err
	}

	return txn.Commit(ctx)
}

func (cmd *MstDs) putInCollectionWithBatch(ctx context.Context, batch datastore.Batch, collectionName, k string, value []byte, collection *Collection) error {
	// Проверяем, новый ли это ключ
	leafKey := datastore.NewKey(fmt.Sprintf("%s%s%s%s", collectionPrefix, collectionName, collectionLeafKey, k))
	_, err := cmd.storage.Get(ctx, leafKey)
	isNewKey := err == datastore.ErrNotFound

	// Сохраняем данные
	dataKey := datastore.NewKey(fmt.Sprintf("%s%s%s%s", collectionPrefix, collectionName, collectionDataKey, k))
	if err := batch.Put(ctx, dataKey, value); err != nil {
		return err
	}

	// Обновляем/создаем лист
	leafHash := cmd.hashLeaf(k, value)
	if err := batch.Put(ctx, leafKey, leafHash); err != nil {
		return err
	}

	// Коммитим батч для сохранения листа перед пересчетом дерева
	if err := batch.Commit(ctx); err != nil {
		return err
	}

	// Пересчитываем дерево коллекции
	newRoot, err := cmd.buildCollectionTreeFromStorage(ctx, collectionName)
	if err != nil {
		return err
	}

	// Создаем новый батч для сохранения метаданных
	newBatch, err := cmd.storage.Batch(ctx)
	if err != nil {
		return err
	}

	// Обновляем коллекцию
	if isNewKey {
		collection.ItemsCount++
	}
	collection.UpdatedAt = time.Now()
	collection.RootHash = newRoot

	// Сохраняем обновленную коллекцию
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

// GetFromCollection получает элемент из коллекции
func (cmd *MstDs) GetFromCollection(ctx context.Context, collectionName string, key datastore.Key) ([]byte, error) {
	cmd.mu.RLock()
	defer cmd.mu.RUnlock()

	if _, exists := cmd.collections[collectionName]; !exists {
		return nil, fmt.Errorf("collection %s not found", collectionName)
	}

	dataKey := datastore.NewKey(fmt.Sprintf("%s%s%s%s", collectionPrefix, collectionName, collectionDataKey, key.String()))
	return cmd.storage.Get(ctx, dataKey)
}

// DeleteFromCollection удаляет элемент из коллекции
func (cmd *MstDs) DeleteFromCollection(ctx context.Context, collectionName string, key datastore.Key) error {
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

// === ГЛАВНАЯ ФУНКЦИЯ: ПОСТРОЕНИЕ ДЕРЕВА БЕЗ cmd.leaves ===

// buildCollectionTreeFromStorage строит Merkle Tree читая листья из Storage
func (cmd *MstDs) buildCollectionTreeFromStorage(ctx context.Context, collectionName string) ([]byte, error) {
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

// === MERKLE PROOFS БЕЗ cmd.leaves ===

// GenerateCollectionProof создает proof читая данные из Storage
func (cmd *MstDs) GenerateCollectionProof(ctx context.Context, collectionName string, key datastore.Key) (*CollectionMerkleProof, error) {
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
func (cmd *MstDs) buildCollectionProofPathFromStorage(ctx context.Context, collectionName, targetKey string) ([][]byte, []bool, error) {
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

// === ОСТАЛЬНЫЕ МЕТОДЫ ===

// HasInCollection проверяет существование элемента в коллекции
func (cmd *MstDs) HasInCollection(ctx context.Context, collectionName string, key datastore.Key) (bool, error) {
	cmd.mu.RLock()
	defer cmd.mu.RUnlock()

	if _, exists := cmd.collections[collectionName]; !exists {
		return false, fmt.Errorf("collection %s not found", collectionName)
	}

	dataKey := datastore.NewKey(fmt.Sprintf("%s%s%s%s", collectionPrefix, collectionName, collectionDataKey, key.String()))
	_, err := cmd.storage.Get(ctx, dataKey)
	if err == datastore.ErrNotFound {
		return false, nil
	}
	return err == nil, err
}

// QueryCollection выполняет запрос в коллекции
func (cmd *MstDs) QueryCollection(ctx context.Context, collectionName string, q query.Query) (query.Results, error) {
	cmd.mu.RLock()
	defer cmd.mu.RUnlock()

	if _, exists := cmd.collections[collectionName]; !exists {
		return nil, fmt.Errorf("collection %s not found", collectionName)
	}

	prefix := fmt.Sprintf("%s%s%s", collectionPrefix, collectionName, collectionDataKey)

	// Модифицируем запрос для поиска в данных коллекции
	collectionQuery := query.Query{
		Prefix: prefix + q.Prefix,
		Limit:  q.Limit,
		Offset: q.Offset,
	}

	results, err := cmd.storage.Query(ctx, collectionQuery)
	if err != nil {
		return nil, err
	}

	// Адаптируем результаты, убирая префикс коллекции из ключей
	return query.ResultsFromIterator(q, query.Iterator{
		Next: func() (query.Result, bool) {
			result, ok := <-results.Next()
			if !ok {
				return query.Result{}, false
			}

			if result.Error != nil {
				return result, true
			}

			// Убираем префикс коллекции из ключа
			originalKey := result.Key[len(prefix):]
			return query.Result{
				Entry: query.Entry{
					Key:   originalKey,
					Value: result.Value,
					Size:  len(result.Value),
				},
			}, true
		},
		Close: func() error {
			return results.Close()
		},
	}), nil
}

// GetCollectionRootHash возвращает root hash коллекции
func (cmd *MstDs) GetCollectionRootHash(collectionName string) ([]byte, error) {
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

// VerifyCollectionProof проверяет Merkle proof для коллекции
func (cmd *MstDs) VerifyCollectionProof(proof *CollectionMerkleProof) (bool, error) {
	collection, exists := cmd.collections[proof.Collection]
	if !exists {
		return false, fmt.Errorf("collection %s not found", proof.Collection)
	}

	return cmd.verifyProofStatic(proof, collection.RootHash), nil
}

// VerifyCollectionProofStatic статическая верификация proof коллекции
func VerifyCollectionProofStatic(proof *CollectionMerkleProof) bool {
	hasher := blake3.New(32, nil)
	hasher.Write([]byte(proof.Key))
	hasher.Write(proof.Value)
	expectedLeafHash := hasher.Sum(nil)

	if !bytes.Equal(proof.LeafHash, expectedLeafHash) {
		return false
	}

	currentHash := proof.LeafHash
	for i, siblingHash := range proof.Path {
		pairHasher := blake3.New(32, nil)
		if proof.Positions[i] {
			pairHasher.Write(currentHash)
			pairHasher.Write(siblingHash)
		} else {
			pairHasher.Write(siblingHash)
			pairHasher.Write(currentHash)
		}
		currentHash = pairHasher.Sum(nil)
	}

	return bytes.Equal(currentHash, proof.RootHash)
}

// === ВСПОМОГАТЕЛЬНЫЕ МЕТОДЫ ===

func (cmd *MstDs) loadCollections(ctx context.Context) error {
	data, err := cmd.storage.Get(ctx, datastore.NewKey(collectionsKey))
	if err == datastore.ErrNotFound {
		return cmd.CreateCollection("default", map[string]string{
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

func (cmd *MstDs) loadCollectionMetadata(ctx context.Context, name string) error {
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

func (cmd *MstDs) getCollectionNames() []string {
	names := make([]string, 0, len(cmd.collections))
	for name := range cmd.collections {
		names = append(names, name)
	}
	sort.Strings(names)
	return names
}

func (cmd *MstDs) hashLeaf(key string, value []byte) []byte {
	hasher := blake3.New(32, nil)
	hasher.Write([]byte(key))
	hasher.Write(value)
	return hasher.Sum(nil)
}

func (cmd *MstDs) hashPair(left, right []byte) []byte {
	hasher := blake3.New(32, nil)
	hasher.Write(left)
	hasher.Write(right)
	return hasher.Sum(nil)
}

func (cmd *MstDs) verifyProofStatic(proof *CollectionMerkleProof, rootHash []byte) bool {
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

// === go-datastore СОВМЕСТИМОСТЬ ===

func (cmd *MstDs) Put(ctx context.Context, key datastore.Key, value []byte) error {
	return cmd.storage.Put(ctx, key, value)
}

func (cmd *MstDs) Get(ctx context.Context, key datastore.Key) ([]byte, error) {
	return cmd.storage.Get(ctx, key)
}

func (cmd *MstDs) Delete(ctx context.Context, key datastore.Key) error {
	return cmd.storage.Delete(ctx, key)
}

func (cmd *MstDs) Has(ctx context.Context, key datastore.Key) (bool, error) {
	return cmd.storage.Has(ctx, key)
}

func (cmd *MstDs) Query(ctx context.Context, q query.Query) (query.Results, error) {
	return cmd.storage.Query(ctx, q)
}

func (cmd *MstDs) GetSize(ctx context.Context, key datastore.Key) (int, error) {
	return cmd.storage.GetSize(ctx, key)
}

func (cmd *MstDs) Sync(ctx context.Context, prefix datastore.Key) error {
	return cmd.storage.Sync(ctx, prefix)
}

func (cmd *MstDs) Close() error {
	return cmd.storage.Close()
}

func (cmd *MstDs) Batch(ctx context.Context) (datastore.Batch, error) {
	return cmd.storage.Batch(ctx)
}
