package partC

import "github.com/gagliardetto/solana-go/rpc"

type entry struct {
	signature *rpc.TransactionSignature
	count     int
}

type ConsensusMap struct {
	m                  map[string]*entry
	consensusThreshold int
}

func NewConsensusMap(consensusThreshold int) *ConsensusMap {
	return &ConsensusMap{
		m:                  make(map[string]*entry),
		consensusThreshold: consensusThreshold,
	}
}

func (c *ConsensusMap) Len() int {
	return len(c.m)
}

func (c *ConsensusMap) InjestSignature(sig *rpc.TransactionSignature) {
	_, ok := c.m[sig.Signature.String()]
	if ok {
		// Signature already seen, increment count
		c.m[sig.Signature.String()].count++
	} else {
		// Encountered new sig, add to map
		c.m[sig.Signature.String()] = &entry{
			signature: sig,
			count:     1,
		}
	}
}

func (c *ConsensusMap) Delete(sig *rpc.TransactionSignature) {
	delete(c.m, sig.Signature.String())
}

// Returns a slice of signatures that have been seen at least ConsensusThreshold times
func (c *ConsensusMap) GetConsensusSignatures() []*rpc.TransactionSignature {
	signatures := make([]*rpc.TransactionSignature, 0)
	for _, entry := range c.m {
		if entry.count >= c.consensusThreshold {
			signatures = append(signatures, entry.signature)
		}
	}
	return signatures
}

// Returns every signature encountered regardless of consensus
func (c *ConsensusMap) GetAllSignatures() []*rpc.TransactionSignature {
	signatures := make([]*rpc.TransactionSignature, 0)
	for _, entry := range c.m {
		signatures = append(signatures, entry.signature)
	}
	return signatures
}
