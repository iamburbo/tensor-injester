package fetch

import (
	"context"

	"github.com/gagliardetto/solana-go"
	"github.com/gagliardetto/solana-go/rpc"
	"github.com/iamburbo/tensor-injester/util"
)

// A StandardFetchJob signifies an RPC query
// for a list of signatures in Standard mode
type StandardFetchJob struct {
	// The signature to fetch 'until'
	TargetSignature solana.Signature
	TargetBlocktime solana.UnixTimeSeconds

	// Earliest signature from previous fetch, or nil if this is the first fetch
	PreviousFetchSignature *solana.Signature
	PreviousFetchBlocktime *solana.UnixTimeSeconds

	// Not directly used in this fetcher, but required for the job manager
	// to keep track of job batches
	NextTargetSignature *solana.Signature
	NextTargetBlocktime *solana.UnixTimeSeconds
}

func (j *StandardFetchJob) fetchSignatures(ctx context.Context, fetcher *RetryableFetcher) ([]*rpc.TransactionSignature, error) {
	if j.PreviousFetchSignature == nil {
		// This is the first time fetching any signatures for this job.
		// Fetch signatures from the beginning of time until the target signature.
		opts := &rpc.GetSignaturesForAddressOpts{
			Until: j.TargetSignature,
		}
		return fetcher.GetSignaturesForAddress(ctx, opts)
	} else {
		// This is a subsequent fetch.
		// Fetch signatures from the previous fetch signature until the target signature.
		opts := &rpc.GetSignaturesForAddressOpts{
			Until:  j.TargetSignature,
			Before: *j.PreviousFetchSignature,
		}
		return fetcher.GetSignaturesForAddress(ctx, opts)
	}
}

func (j *StandardFetchJob) Execute(ctx context.Context, fetcher *RetryableFetcher) ([]*rpc.TransactionSignature, error) {
	// Retreive new signatures based on params of the job
	signatures, err := j.fetchSignatures(ctx, fetcher)
	if err != nil {
		return nil, err
	}

	// Remove signatures outside of time window for this fetch
	signatures = util.TrimExpiredSignatures(signatures, &j.TargetBlocktime, j.PreviousFetchBlocktime)
	return signatures, nil
}

type HistoryFetchJob struct {
	PreviousFetchSignature *solana.Signature
	PreviousFetchBlocktime solana.UnixTimeSeconds
}

func (j *HistoryFetchJob) Execute(ctx context.Context, fetcher *RetryableFetcher) ([]*rpc.TransactionSignature, error) {
	// Fetch signatures from the beginning of time until the previous fetch signature
	opts := &rpc.GetSignaturesForAddressOpts{
		Before: *j.PreviousFetchSignature,
	}
	return fetcher.GetSignaturesForAddress(ctx, opts)
}
