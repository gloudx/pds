package mstdatastore

import (
	"context"
	"fmt"

	"github.com/ipfs/go-datastore/query"
)

// QueryCollection выполняет запрос в коллекции
func (cmd *MstDatastore) QueryCollection(ctx context.Context, collectionName string, q query.Query) (query.Results, error) {
	
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
