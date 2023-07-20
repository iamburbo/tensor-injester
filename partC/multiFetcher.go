package partC

import (
	"context"
	"sync"

	"github.com/gagliardetto/solana-go/rpc"
	"github.com/iamburbo/tensor-injester/fetch"
	"github.com/rs/zerolog/log"
)

type MultiFetcher struct {
	consensusThreshold int
	fetchers           []*fetch.RetryableFetcher
	wg                 *sync.WaitGroup

	consensusMap *ConsensusMap
}

func NewMultiFetcher(rpcClient *rpc.Client, config *fetch.RetryableFetcherConfig, numFetchers, consensusThreshold int) *MultiFetcher {
	fetchers := make([]*fetch.RetryableFetcher, 0, numFetchers)
	for i := 0; i < numFetchers; i++ {
		fetchers = append(fetchers, fetch.NewRetryableFetcher(rpcClient, config))
	}

	return &MultiFetcher{
		consensusThreshold: consensusThreshold,
		fetchers:           fetchers,
		wg:                 &sync.WaitGroup{},
	}
}

// Concurrently fetch from all fetchers and consolidate results
func (m *MultiFetcher) FetchSignatures(ctx context.Context,
	execFunc func(ctx context.Context, fetcher *fetch.RetryableFetcher) ([]*rpc.TransactionSignature, error)) (*ConsensusMap, error) {
	fetcherCtx, cancel := context.WithCancel(ctx)

	m.consensusMap = NewConsensusMap(m.consensusThreshold)

	// Channel to collect results from all fetchers
	sigChan := make(chan []*rpc.TransactionSignature, len(m.fetchers))

	// Dispatch the same job for each fetcher
	for _, fetcher := range m.fetchers {
		m.wg.Add(1)

		go func(fetcher *fetch.RetryableFetcher) {
			defer m.wg.Done()

			signatures, err := execFunc(fetcherCtx, fetcher)
			if err != nil {
				select {
				case <-ctx.Done():
					// Don't log errors from context cancellation
					return
				default:
					log.Error().Err(err).Msg("error executing fetch job")
					cancel()
					return
				}
			}

			sigChan <- signatures
		}(fetcher)
	}

	// Wait for results from the fetch job
	m.wg.Wait()
	cancel()
	close(sigChan)

	// Process all signatures from each fetcher goroutine
	for {
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case signatures, ok := <-sigChan:
			if !ok {
				// Channel closed, everything has been processed
				if ctx.Err() != nil {
					return nil, ctx.Err()
				}
				return m.consensusMap, nil
			}

			// Add signatures to consensus map to be counted
			for _, sig := range signatures {
				m.consensusMap.InjestSignature(sig)
			}
		}
	}
}
