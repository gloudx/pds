package helpers

import (
	"fmt"

	"github.com/ipld/go-ipld-prime/datamodel"
)

// NodeToInterface преобразует datamodel.Node в interface{}
func NodeToInterface(node datamodel.Node) (any, error) {

	switch node.Kind() {

	case datamodel.Kind_String:
		return node.AsString()

	case datamodel.Kind_Int:
		return node.AsInt()

	case datamodel.Kind_Float:
		return node.AsFloat()

	case datamodel.Kind_Bool:
		return node.AsBool()

	case datamodel.Kind_Null:
		return nil, nil

	case datamodel.Kind_Map:
		return NodeToMap(node)

	case datamodel.Kind_List:
		iter := node.ListIterator()
		var result []any
		for !iter.Done() {
			_, item, err := iter.Next()
			if err != nil {
				return nil, err
			}
			val, err := NodeToInterface(item)
			if err != nil {
				return nil, err
			}
			result = append(result, val)
		}
		return result, nil

	default:
		return fmt.Sprintf("%v", node), nil
	}
}
