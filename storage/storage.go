package storage

import (
	"context"
	"fmt"
	"log"
	"pds/blockstore"
	"pds/collection"
	"pds/datastore"
	"pds/helpers"
	"pds/oplog"

	blocks "github.com/ipfs/go-block-format"
	"github.com/ipfs/go-cid"
	ds "github.com/ipfs/go-datastore"
	badger4 "github.com/ipfs/go-ds-badger4"
	"github.com/ipld/go-ipld-prime"
	"github.com/ipld/go-ipld-prime/linking"
	cidlink "github.com/ipld/go-ipld-prime/linking/cid"
	"github.com/multiformats/go-multicodec"
	"github.com/multiformats/go-multihash"
)

// StorageManager координирует все компоненты UES системы
type StorageManager struct {
	bs     *blockstore.Blockstore        // IPFS блокстор
	cols   *collection.CollectionManager // Менеджер коллекций
	oplog  oplog.OperationLog            // HLC operation log
	config Config                        // Конфигурация
	nodeID string                        // ID узла
}

// Config конфигурация storage менеджера
type Config struct {
	StoragePath string `json:"storagePath"`
	NodeID      string `json:"nodeId"`
}

// NewStorageManager создает и инициализирует менеджер хранения
func NewStorageManager(config Config) (*StorageManager, error) {

	if config.NodeID == "" {
		return nil, fmt.Errorf("node ID is required")
	}

	sm := &StorageManager{
		config: config,
		nodeID: config.NodeID,
	}

	// Инициализируем все компоненты
	if err := sm.initializeComponents(); err != nil {
		return nil, fmt.Errorf("failed to initialize components: %w", err)
	}

	log.Printf("✅ StorageManager initialized with node ID: %s", config.NodeID)

	return sm, nil
}

// initializeComponents инициализирует все компоненты в правильном порядке
func (sm *StorageManager) initializeComponents() error {
	var err error

	// 1. Базовое хранилище (BadgerDB)
	log.Println("📦 Initializing base storage...")
	storage, err := datastore.NewDatastorage(sm.config.StoragePath, datastore.Options{
		Options: badger4.DefaultOptions,
	})
	if err != nil {
		return fmt.Errorf("failed to create storage: %w", err)
	}

	// 2. Collection Manager
	log.Println("🌳 Initializing CollectionManager...")
	sm.cols, err = collection.NewCollectionManager(storage)
	if err != nil {
		return fmt.Errorf("failed to create MST: %w", err)
	}

	// 3. IPFS Blockstore
	log.Println("📁 Initializing blockstore...")
	sm.bs = blockstore.NewBlockstore(storage)

	// 4. Operation Log
	log.Println("📝 Initializing operation log...")
	sm.oplog = oplog.NewStorageOperationLog(storage, sm.nodeID)

	return nil
}

// CollectionManager возвращает менеджер коллекций
func (sm *StorageManager) CollectionManager() *collection.CollectionManager {
	return sm.cols
}

// PutRaw сохраняет сырые данные в блокстор и возвращает их CID
func (sm *StorageManager) PutRaw(ctx context.Context, data []byte) (cid.Cid, error) {
	mh, err := multihash.Sum(data, multihash.BLAKE3, -1)
	if err != nil {
		return cid.Undef, err
	}
	c := cid.NewCidV1(cid.Raw, mh)
	blk, err := blocks.NewBlockWithCid(data, c)
	if err != nil {
		return cid.Undef, err
	}
	if err := sm.bs.Put(ctx, blk); err != nil {
		return cid.Undef, err
	}
	return c, nil
}

// Put сохраняет IPLD узел в блокстор с использованием кодека dag-cbor по умолчанию
func (sm *StorageManager) Put(ctx context.Context, node ipld.Node) (cid.Cid, error) {
	if node == nil {
		return cid.Undef, fmt.Errorf("node cannot be nil")
	}
	return sm.PutWithCodec(ctx, node, multicodec.DagCbor)
}

// PutWithCodec сохраняет IPLD узел в блокстор с указанным кодеком
func (sm *StorageManager) PutWithCodec(ctx context.Context, node ipld.Node, codec multicodec.Code) (cid.Cid, error) {
	if node == nil {
		return cid.Undef, fmt.Errorf("node cannot be nil")
	}
	linkProto := cidlink.LinkPrototype{
		Prefix: cid.Prefix{
			Version:  1,
			Codec:    uint64(codec),
			MhType:   multihash.BLAKE3,
			MhLength: -1,
		},
	}
	link, err := sm.bs.LinkSystem().Store(linking.LinkContext{Ctx: ctx}, linkProto, node)
	if err != nil {
		return cid.Undef, fmt.Errorf("failed to store node: %w", err)
	}
	cidLink, ok := link.(cidlink.Link)
	if !ok {
		return cid.Undef, fmt.Errorf("expected cidlink.Link, got %T", link)
	}
	return cidLink.Cid, nil
}

// PutRecord сохраняет запись в указанную коллекцию
func (sm *StorageManager) PutRecord(ctx context.Context, collection string, data any, author string) error {
	node, err := helpers.ToNode(data)
	if err != nil {
		return fmt.Errorf("failed to convert data to node: %w", err)
	}
	c, err := sm.Put(ctx, node)
	if err != nil {
		return fmt.Errorf("failed to put node: %w", err)
	}
	if err := sm.cols.PutInCollection(ctx, collection, ds.NewKey(c.String()), c.Bytes()); err != nil {
		return fmt.Errorf("failed to put in collection: %w", err)
	}
	return nil
}
