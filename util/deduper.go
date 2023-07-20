package util

import (
	"github.com/gagliardetto/solana-go/rpc"
)

// The Deduper processes incoming data and dedupes it
// while limiting the data it holds in memory.
type Deduper struct {
	om *OrderedMap
}

func NewDeduper(cap int) *Deduper {
	return &Deduper{
		om: NewOrderedMap(cap),
	}
}

func (d *Deduper) Dedupe(sigs []*rpc.TransactionSignature) []*rpc.TransactionSignature {
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
