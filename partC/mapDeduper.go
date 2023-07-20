package partC

import (
	"github.com/gagliardetto/solana-go/rpc"
	"github.com/iamburbo/tensor-injester/util"
)

// The Deduper processes incoming data and dedupes it
// while limiting the data it holds in memory.
type MapDeduper struct {
	om *util.OrderedMap
}

func NewMapDeduper(cap int) *MapDeduper {
	return &MapDeduper{
		om: util.NewOrderedMap(cap),
	}
}

func (d *MapDeduper) Dedupe(signatures *ConsensusMap) {
	// Dedupe incoming signatures
	allSignatures := signatures.GetAllSignatures()

	for _, sig := range allSignatures {
		if !d.om.Has(sig) {
			d.om.Put(sig)
		} else {
			signatures.Delete(sig)
		}
	}
}

// Dedupes a slice, used mostly for loading a snapshot into the deduper
func (d *MapDeduper) DedupeSlice(sigs []*rpc.TransactionSignature) []*rpc.TransactionSignature {
	// Dedupe incoming signatures
	j := 0
	for _, sig := range sigs {
		if !d.om.Has(sig) {
			d.om.Put(sig)
			sigs[j] = sig
			j++
		}
	}
	sigs = sigs[:j]
	return sigs
}
