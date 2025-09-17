package blockstore

import (
	"context"
	"fmt"
	"io"
	"path/filepath"
	s "pds/datastore"
	i "pds/indexer"
	"strings"
	"sync"
	"time"

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
	DefaultChunkSize      = 262144 // 256 KiB
	RabinMinSize          = DefaultChunkSize / 2
	RabinMaxSize          = DefaultChunkSize * 2
	DefaultConcurrency    = 10
	DefaultListPagination = 100
)

// Blockstore реализует IPFS блокстор с MST бэкендом и индексатором
type Blockstore struct {
	blockstore.Blockstore
	datastore s.Datastore
	indexer   *i.Indexer
	lsys      *linking.LinkSystem
	bS        blockservice.BlockService
	dS        format.DAGService

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
func NewBlockstore(ds s.Datastore, indx *i.Indexer) *Blockstore {
	bs := &Blockstore{
		Blockstore: blockstore.NewBlockstore(ds),
		datastore:  ds,
		indexer:    indx,
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

// Put сохраняет блок
func (bs *Blockstore) Put(ctx context.Context, block blocks.Block) error {
	if err := bs.Blockstore.Put(ctx, block); err != nil {
		return err
	}
	bs.cacheBlock(block)
	// Индексируем блок если есть индексатор
	if bs.indexer != nil {
		go bs.indexBlock(block)
	}
	return nil
}

// Get получает блок
func (bs *Blockstore) Get(ctx context.Context, c cid.Cid) (blocks.Block, error) {
	// Сначала проверяем кеш
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
	if bs.indexer != nil {
		bs.indexer.RemoveEntry("block:" + c.String())
	}
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

	key := "unixfs:" + nd.Cid().String()

	// Индексируем файл
	if bs.indexer != nil {
		go func() {
			entry := &i.IndexEntry{
				Key:         key,
				ContentType: detectContentType(name),
				Size:        int64(len(nd.RawData())), // Размер метаданных, не самого файла
				CreatedAt:   time.Now(),
				UpdatedAt:   time.Now(),
				DataHash:    nd.Cid().String(),
				Metadata: map[string]interface{}{
					"name":       name,
					"type":       "unixfs_file",
					"layout":     "balanced",
					"raw_leaves": true,
				},
			}
			bs.indexer.IndexEntry(entry)
			bs.indexer.AddTags(key, []string{"unixfs", "file"})
		}()
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

// LinkSystem возвращает настроенный IPLD LinkSystem
func (bs *Blockstore) LinkSystem() *linking.LinkSystem {
	return bs.lsys
}

// GetReader возвращает Reader для файла (поддерживает большие чанкованные файлы)
func (bs *Blockstore) GetReader(ctx context.Context, c cid.Cid) (io.ReadSeekCloser, error) {
	nd, err := bs.dS.Get(ctx, c)
	if err != nil {
		return nil, err
	}
	return ufsio.NewDagReader(ctx, nd, bs.dS)
}

// SearchFiles поиск файлов через индексатор
func (bs *Blockstore) SearchFiles(ctx context.Context, contentType string, limit int) ([]*i.SearchResult, error) {
	if bs.indexer == nil {
		return nil, fmt.Errorf("indexer not available")
	}
	return bs.indexer.SearchByContentType(contentType, limit, 0)
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

func (bs *Blockstore) indexBlock(block blocks.Block) {
	key := "block:" + block.Cid().String()
	entry := &i.IndexEntry{
		Key:         key,
		ContentType: "application/octet-stream", // Блоки это raw data
		Size:        int64(len(block.RawData())),
		CreatedAt:   time.Now(),
		UpdatedAt:   time.Now(),
		DataHash:    block.Cid().String(),
		Metadata: map[string]interface{}{
			"cid":       block.Cid().String(),
			"codec":     block.Cid().Type(),
			"multihash": block.Cid().Hash(),
		},
	}
	bs.indexer.IndexEntry(entry)
	bs.indexer.AddTags(key, []string{"ipfs_block", "raw"})
}

func detectContentType(filename string) string {
	ext := strings.ToLower(filepath.Ext(filename))
	switch ext {
	case ".jpg", ".jpeg":
		return "image/jpeg"
	case ".png":
		return "image/png"
	case ".gif":
		return "image/gif"
	case ".mp4":
		return "video/mp4"
	case ".mp3":
		return "audio/mpeg"
	case ".pdf":
		return "application/pdf"
	case ".txt":
		return "text/plain"
	case ".json":
		return "application/json"
	case ".html":
		return "text/html"
	case ".csv":
		return "text/csv"
	case ".zip":
		return "application/zip"
	case ".tar":
		return "application/x-tar"
	case ".gz":
		return "application/gzip"
	case ".doc", ".docx":
		return "application/msword"
	case ".xls", ".xlsx":
		return "application/vnd.ms-excel"
	case ".ppt", ".pptx":
		return "application/vnd.ms-powerpoint"
	case ".svg":
		return "image/svg+xml"
	case ".wav":
		return "audio/wav"
	case ".flac":
		return "audio/flac"
	case ".ogg":
		return "audio/ogg"
	case ".webm":
		return "video/webm"
	case ".mov":
		return "video/quicktime"
	case ".avi":
		return "video/x-msvideo"
	case ".mkv":
		return "video/x-matroska"
	case ".epub":
		return "application/epub+zip"
	case ".md":
		return "text/markdown"
	case ".rtf":
		return "application/rtf"
	case ".ics":
		return "text/calendar"
	case ".xml":
		return "application/xml"
	case ".yml", ".yaml":
		return "application/x-yaml"
	case ".exe":
		return "application/vnd.microsoft.portable-executable"
	case ".apk":
		return "application/vnd.android.package-archive"
	case ".bin":
		return "application/octet-stream"
	case ".iso":
		return "application/x-iso9660-image"
	case ".dmg":
		return "application/x-apple-diskimage"
	case ".torrent":
		return "application/x-bittorrent"
	case ".log":
		return "text/plain"
	case ".ini", ".cfg", ".conf":
		return "text/plain"
	case ".sql":
		return "application/sql"
	case ".db", ".sqlite", ".sqlite3":
		return "application/vnd.sqlite3"
	case ".psd":
		return "image/vnd.adobe.photoshop"
	case ".ai":
		return "application/postscript"
	case ".eps":
		return "application/postscript"
	case ".ttf":
		return "font/ttf"
	default:
		return "application/octet-stream"
	}
}
