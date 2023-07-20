package util

import (
	"sort"
	"strings"
	"time"

	"github.com/gagliardetto/solana-go"
	"github.com/gagliardetto/solana-go/rpc"
)

type InjesterMode int

const (
	MODE_STANDARD InjesterMode = iota
	MODE_HISTORY
)

func (m InjesterMode) String() string {
	switch m {
	case MODE_STANDARD:
		return "Standard"
	case MODE_HISTORY:
		return "History"
	default:
		return "Unknown"
	}
}

func ParseMode(str string) InjesterMode {
	lower := strings.ToLower(str)

	switch lower {
	case "standard":
		return MODE_STANDARD
	case "history":
		return MODE_HISTORY
	default:
		return MODE_STANDARD
	}
}

func GetEarliestSignatureFromBatch(signatures []*rpc.TransactionSignature) *rpc.TransactionSignature {
	var earliest *rpc.TransactionSignature = nil
	for _, signature := range signatures {
		if earliest == nil {
			earliest = signature
		} else if earliest.BlockTime.Time().Unix() > signature.BlockTime.Time().Unix() {
			earliest = signature
		}
	}
	return earliest
}

func GetLatestSignatureFromBatch(signatures []*rpc.TransactionSignature) *rpc.TransactionSignature {
	var latest *rpc.TransactionSignature = nil
	for _, signature := range signatures {
		if latest == nil {
			latest = signature
		} else if signature.BlockTime.Time().After(latest.BlockTime.Time()) {
			latest = signature
		}
	}
	return latest
}

// Ensures all signatures are in chronological order
func SortSignatures(signatures []*rpc.TransactionSignature) {
	sort.Slice(signatures, func(i, j int) bool {
		return signatures[j].BlockTime.Time().After(signatures[i].BlockTime.Time())
	})
}

// Ensures all signatures are in reverse-chronological order
func SortSignaturesReverse(signatures []*rpc.TransactionSignature) {
	sort.Slice(signatures, func(i, j int) bool {
		return signatures[i].BlockTime.Time().After(signatures[j].BlockTime.Time())
	})
}

// Removes signatures that are outside of the min/max blockTime
func TrimExpiredSignatures(signatures []*rpc.TransactionSignature, minTime, maxTime *solana.UnixTimeSeconds) []*rpc.TransactionSignature {
	j := 0
	for i := 0; i < len(signatures); i++ {
		sig := signatures[i]

		trim := false
		if minTime != nil && sig.BlockTime.Time().Unix() < minTime.Time().Unix() {
			trim = true
		} else if maxTime != nil && sig.BlockTime.Time().Unix() > maxTime.Time().Unix() {
			trim = true
		}

		if !trim {
			signatures[j] = sig
			j++
		}
	}
	signatures = signatures[:j]
	return signatures
}

// Checks if every signature in a batch has the same Blocktime. Used for detecting an edge case
func BatchHasAllIdenticalBlocktimes(blocktime time.Time, signatures []*rpc.TransactionSignature) bool {
	if len(signatures) == 0 {
		return false
	}

	identicalBlockTime := true
	for _, sig := range signatures {
		if !sig.BlockTime.Time().Equal(blocktime) {
			identicalBlockTime = false
			break
		}
	}

	return identicalBlockTime
}

func SignaturesOnBlocktimeLimits(targetBlocktime time.Time, previousBlocktime time.Time, signatures []*rpc.TransactionSignature) bool {
	if len(signatures) == 0 {
		return false
	}

	identicalBlockTime := true
	for _, sig := range signatures {
		if !sig.BlockTime.Time().Equal(targetBlocktime) && !sig.BlockTime.Time().Equal(previousBlocktime) {
			identicalBlockTime = false
			break
		}
	}

	return identicalBlockTime
}
