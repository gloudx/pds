package helpers

import (
	"encoding/json"
	"fmt"

	"github.com/ipfs/go-cid"
	"github.com/ipld/go-ipld-prime"
	"github.com/ipld/go-ipld-prime/datamodel"
	cidlink "github.com/ipld/go-ipld-prime/linking/cid"
	"github.com/ipld/go-ipld-prime/node/basicnode"
)

// ToNode преобразует произвольное значение в ipld.Node
func ToNode(data any) (ipld.Node, error) {
	if node, ok := data.(ipld.Node); ok {
		return node, nil
	}
	nb := basicnode.Prototype.Any.NewBuilder()
	if err := ValueToNode(nb, data); err != nil {
		return nil, fmt.Errorf("failed to convert value to node: %w", err)
	}
	return nb.Build(), nil
}

// ValueToNode преобразует произвольное значение в datamodel.NodeAssembler
func ValueToNode(na datamodel.NodeAssembler, v any) (err error) {

	switch val := v.(type) {
	case string:
		err = na.AssignString(val)

	case int64:
		err = na.AssignInt(val)

	case float64:
		err = na.AssignFloat(val)

	case bool:
		err = na.AssignBool(val)

	case nil:
		err = na.AssignNull()

	case cid.Cid:
		err = na.AssignLink(cidlink.Link{Cid: val})

	case map[string]any:
		return MapAnyToNode(na, val)

	case map[string]string:
		return MapStringToNode(na, val)

	case []byte:
		err = na.AssignBytes(val)

	case []any:
		la, err := na.BeginList(int64(len(val)))
		if err != nil {
			return err
		}
		for _, item := range val {
			if err := ValueToNode(la.AssembleValue(), item); err != nil {
				return err
			}
		}
		return la.Finish()

	default:
		var data []byte
		data, err = json.Marshal(val)
		if err != nil {
			return fmt.Errorf("failed to marshal value: %w", err)
		}
		err = na.AssignBytes(data)
	}

	if err != nil {
		return fmt.Errorf("failed to assign value: %w", err)
	}

	return nil
}
