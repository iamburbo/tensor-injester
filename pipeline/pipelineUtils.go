package pipeline

import (
	"context"

	"github.com/gagliardetto/solana-go"
	"github.com/gagliardetto/solana-go/rpc"
	"github.com/iamburbo/tensor-injester/fetch"
	"github.com/rs/zerolog/log"
)

type PipelineConfig struct {
	Filepath       string
	RpcClient      *rpc.Client
	Address        solana.PublicKey
	FetchLimit     int
	WriteSizeLimit int
}

// For use when no signatures have been written yet, fetch the first batch
// of signatures and write them to the file.
func FetchInitialSignature(ctx context.Context,
	fetcher *fetch.RetryableFetcher) *rpc.TransactionSignature {
	// No existing initial signature, fetch first batch and go from there
	var signatures []*rpc.TransactionSignature = make([]*rpc.TransactionSignature, 0)
	var err error = nil

	// Fetch until a signature is found
	for err != nil || len(signatures) != 1 {
		select {
		case <-ctx.Done():
			return nil
		default:
			limit := 1
			signatures, err = fetcher.GetSignaturesForAddress(ctx, &rpc.GetSignaturesForAddressOpts{
				Limit: &limit,
			})
			if err != nil {
				log.Warn().Err(err).Msg("error fetching initial signature")
			}
		}
	}

	return signatures[0]
}
