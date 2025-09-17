package mstdatastore

import (
	"bytes"
	"fmt"

	"lukechampine.com/blake3"
)

// VerifyCollectionProof проверяет Merkle proof для коллекции
func (cmd *MstDatastore) VerifyCollectionProof(proof *CollectionMerkleProof) (bool, error) {
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
