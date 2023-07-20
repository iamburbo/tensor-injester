package fetch

import (
	"context"
	"time"

	"github.com/gagliardetto/solana-go"
	"github.com/gagliardetto/solana-go/rpc"
	"github.com/rs/zerolog/log"
	"golang.org/x/sync/semaphore"
)

type RetryableFetcherConfig struct {
	Commitment         rpc.CommitmentType
	Address            solana.PublicKey
	FetchLimit         int
	ZeroRequestRetries int
	RetryDelay         time.Duration

	// Optional semaphore
	Sem *semaphore.Weighted
}

type RetryableFetcher struct {
	rpcClient *rpc.Client
	config    *RetryableFetcherConfig
	sem       *semaphore.Weighted
}

func NewRetryableFetcher(rpcClient *rpc.Client, config *RetryableFetcherConfig) *RetryableFetcher {
	return &RetryableFetcher{
		rpcClient: rpcClient,
		config:    config,
		sem:       config.Sem,
	}
}

func (f *RetryableFetcher) GetSignaturesForAddress(ctx context.Context,
	opts *rpc.GetSignaturesForAddressOpts) ([]*rpc.TransactionSignature, error) {

	opts.Commitment = f.config.Commitment
	if opts.Limit == nil {
		opts.Limit = &f.config.FetchLimit
	}

	// Fetch signatures, retrying when 0 signatures are returned
	// due to small chance of RPC server returning 0 signatures as a mistake.
	// note: This does not retry on errors
	attempt := 0
	for attempt < f.config.ZeroRequestRetries {
		if f.sem != nil {
			// Assume worst case scenario of receiving maximum number of signatures
			f.sem.Acquire(ctx, int64(f.config.FetchLimit))
		}

		signatures, err := f.rpcClient.GetSignaturesForAddressWithOpts(
			ctx,
			f.config.Address,
			opts)

		if err != nil {
			return nil, err
		}

		// Release unused semaphore slots based on actual response size
		if f.sem != nil {
			f.sem.Release(int64(f.config.FetchLimit - len(signatures)))
			log.Trace().Msgf("fetcher: released %d unused semaphore slots", f.config.FetchLimit-len(signatures))
		}

		if len(signatures) > 0 {
			// Successful request. return
			return signatures, nil
		} else {
			// No signatures returned, wait and try again
			attempt++
			time.Sleep(f.config.RetryDelay)
		}
	}

	return make([]*rpc.TransactionSignature, 0), nil
}
