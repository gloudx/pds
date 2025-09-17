package helpers

import (
	"fmt"

	"github.com/ipld/go-ipld-prime/datamodel"
)

// NodeToMap преобразует datamodel.Node в map[string]interface{}
func NodeToMap(node datamodel.Node) (map[string]any, error) {

	if node.Kind() != datamodel.Kind_Map {
		return nil, fmt.Errorf("expected map node")
	}

	result := make(map[string]interface{})

	iter := node.MapIterator()

	for !iter.Done() {
		key, value, err := iter.Next()
		if err != nil {
			return nil, err
		}

		keyStr, err := key.AsString()
		if err != nil {
			return nil, err
		}

		result[keyStr], err = NodeToInterface(value)
		if err != nil {
			return nil, err
		}
	}

	return result, nil
}
