package blockstore

import (
	"context"
	"io"
	s "pds/datastore"
	"sync"

	"github.com/ipfs/boxo/blockservice"
	"github.com/ipfs/boxo/blockstore"
	chunker "github.com/ipfs/boxo/chunker"
	"github.com/ipfs/boxo/files"
	"github.com/ipfs/boxo/ipld/merkledag"

	unixfile "github.com/ipfs/boxo/ipld/unixfs/file"
	imp "github.com/ipfs/boxo/ipld/unixfs/importer"
	ufsio "github.com/ipfs/boxo/ipld/unixfs/io"
	blocks "github.com/ipfs/go-block-format"
	"github.com/ipfs/go-cid"

	format "github.com/ipfs/go-ipld-format"
	"github.com/ipld/go-ipld-prime/linking"
	cidlink "github.com/ipld/go-ipld-prime/linking/cid"
	"github.com/ipld/go-ipld-prime/storage/bsrvadapter"
)

const (
	DefaultChunkSize = 262144 // 256 KiB
	RabinMinSize     = DefaultChunkSize / 2
	RabinMaxSize     = DefaultChunkSize * 2
)

// Blockstore реализует IPFS блокстор с MST бэкендом и индексатором
type Blockstore struct {
	blockstore.Blockstore
	lsys *linking.LinkSystem
	bS   blockservice.BlockService
	dS   format.DAGService
	// Кеш для блоков
	blockCache map[string]blocks.Block
	cacheLimit int
	mu         sync.RWMutex
}

// Проверяем что интерфейсы реализованы правильно
var _ blockstore.Blockstore = (*Blockstore)(nil)
var _ blockstore.Viewer = (*Blockstore)(nil)
var _ io.Closer = (*Blockstore)(nil)

// NewMSTBlockstore создает блокстор с MST бэкендом
func NewBlockstore(ds s.Datastore) *Blockstore {
	bs := &Blockstore{
		Blockstore: blockstore.NewBlockstore(ds),
		blockCache: make(map[string]blocks.Block),
		cacheLimit: 1000,
	}
	bs.bS = blockservice.New(bs.Blockstore, nil)
	bs.dS = merkledag.NewDAGService(bs.bS)
	adapter := &bsrvadapter.Adapter{Wrapped: bs.bS}
	lS := cidlink.DefaultLinkSystem()
	lS.SetWriteStorage(adapter)
	lS.SetReadStorage(adapter)
	bs.lsys = &lS
	return bs
}

// LinkSystem возвращает настроенный IPLD LinkSystem
func (bs *Blockstore) LinkSystem() *linking.LinkSystem {
	return bs.lsys
}

// Put сохраняет блок
func (bs *Blockstore) Put(ctx context.Context, block blocks.Block) error {
	if err := bs.Blockstore.Put(ctx, block); err != nil {
		return err
	}
	bs.cacheBlock(block)
	return nil
}

// Get получает блок
func (bs *Blockstore) Get(ctx context.Context, c cid.Cid) (blocks.Block, error) {
	bs.mu.RLock()
	if block, found := bs.blockCache[c.String()]; found {
		bs.mu.RUnlock()
		return block, nil
	}
	bs.mu.RUnlock()
	block, err := bs.Blockstore.Get(ctx, c)
	if err != nil {
		return nil, err
	}
	bs.cacheBlock(block)
	return block, nil
}

// DeleteBlock удаляет блок
func (bs *Blockstore) DeleteBlock(ctx context.Context, c cid.Cid) error {
	if err := bs.Blockstore.DeleteBlock(ctx, c); err != nil {
		return err
	}
	bs.mu.Lock()
	delete(bs.blockCache, c.String())
	bs.mu.Unlock()
	return nil
}

// AddFile добавляет файл с использованием UnixFS
func (bs *Blockstore) AddFile(ctx context.Context, name string, data io.Reader, useRabin bool) (cid.Cid, error) {
	var spl chunker.Splitter
	if useRabin {
		spl = chunker.NewRabinMinMax(data, RabinMinSize, DefaultChunkSize, RabinMaxSize)
	} else {
		spl = chunker.NewSizeSplitter(data, DefaultChunkSize)
	}
	nd, err := imp.BuildDagFromReader(bs.dS, spl)
	if err != nil {
		return cid.Undef, err
	}
	return nd.Cid(), nil
}

// GetFile получает файл через UnixFS
func (bs *Blockstore) GetFile(ctx context.Context, c cid.Cid) (files.Node, error) {
	nd, err := bs.dS.Get(ctx, c)
	if err != nil {
		return nil, err
	}
	return unixfile.NewUnixfsFile(ctx, bs.dS, nd)
}

// GetReader возвращает Reader для файла (поддерживает большие чанкованные файлы)
func (bs *Blockstore) GetReader(ctx context.Context, c cid.Cid) (io.ReadSeekCloser, error) {
	nd, err := bs.dS.Get(ctx, c)
	if err != nil {
		return nil, err
	}
	return ufsio.NewDagReader(ctx, nd, bs.dS)
}

// View
func (bs *Blockstore) View(ctx context.Context, cid cid.Cid, callback func([]byte) error) error {
	nd, err := bs.Blockstore.Get(ctx, cid)
	if err != nil {
		return err
	}
	data := nd.RawData()
	return callback(data)
}

// Close закрывает блокстор
func (bs *Blockstore) Close() error {
	bs.mu.Lock()
	defer bs.mu.Unlock()

	bs.blockCache = make(map[string]blocks.Block)
	return nil
}

// Внутренние методы

func (bs *Blockstore) cacheBlock(block blocks.Block) {
	bs.mu.Lock()
	defer bs.mu.Unlock()
	// Простая LRU логика
	if len(bs.blockCache) >= bs.cacheLimit {
		// Удаляем 20% кеша
		count := 0
		for k := range bs.blockCache {
			delete(bs.blockCache, k)
			count++
			if count > bs.cacheLimit/5 {
				break
			}
		}
	}
	bs.blockCache[block.Cid().String()] = block
}
