package helpers

import "github.com/ipld/go-ipld-prime/datamodel"

// MapAnyToNode ...
func MapAnyToNode(na datamodel.NodeAssembler, m map[string]any) error {
	ma, err := na.BeginMap(int64(len(m)))
	if err != nil {
		return err
	}
	for k, v := range m {
		if err := ma.AssembleKey().AssignString(k); err != nil {
			return err
		}
		if err := ValueToNode(ma.AssembleValue(), v); err != nil {
			return err
		}
	}
	return ma.Finish()
}

// MapStringToNode ...
func MapStringToNode(na datamodel.NodeAssembler, m map[string]string) error {
	ma, err := na.BeginMap(int64(len(m)))
	if err != nil {
		return err
	}
	for k, v := range m {
		if err := ma.AssembleKey().AssignString(k); err != nil {
			return err
		}
		if err := ValueToNode(ma.AssembleValue(), v); err != nil {
			return err
		}
	}
	return ma.Finish()
}
