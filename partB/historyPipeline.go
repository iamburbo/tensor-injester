package partB

import (
	"context"
	"fmt"
	"sync"

	"github.com/gagliardetto/solana-go/rpc"
	"github.com/iamburbo/tensor-injester/cache"
	"github.com/iamburbo/tensor-injester/fetch"
	"github.com/iamburbo/tensor-injester/fileWriter"
	"github.com/iamburbo/tensor-injester/manager"
	"github.com/iamburbo/tensor-injester/pipeline"
	"github.com/iamburbo/tensor-injester/util"
	"github.com/rs/zerolog/log"
	"golang.org/x/sync/semaphore"
)

type HistoryPipeline struct {
	writer     *fileWriter.FileWriter
	jobCache   *cache.JobCache
	jobManager *manager.HistoryJobManager
	fetcher    *fetch.RetryableFetcher
	deduper    *util.Deduper
	wg         *sync.WaitGroup
	sem        *semaphore.Weighted
}

func NewHistoryPipeline(ctx context.Context, config *pipeline.PipelineConfig, wg *sync.WaitGroup) (*HistoryPipeline, error) {
	// Initialize semaphore to limit memory usage to 1000 signatures at a time
	sem := semaphore.NewWeighted(1000)

	// Initialize Signature RPC Fetcher
	fetcherConfig := &fetch.RetryableFetcherConfig{
		Commitment:         rpc.CommitmentFinalized,
		Address:            config.Address,
		RetryDelay:         50,
		FetchLimit:         config.FetchLimit,
		ZeroRequestRetries: 2,
		Sem:                sem,
	}
	fetcher := fetch.NewRetryableFetcher(config.RpcClient, fetcherConfig)

	// Initialize output file and job cache
	fileWriter, err := fileWriter.NewFileWriter(config.Filepath+".csv", config.WriteSizeLimit, 100, wg, sem)
	if err != nil {
		return nil, fmt.Errorf("error creating file writer: %v", err)
	}

	deduper := util.NewDeduper(1_000)

	jobCache := cache.NewJobCache(config.Filepath)

	pipe := &HistoryPipeline{
		writer:   fileWriter,
		fetcher:  fetcher,
		jobCache: jobCache,
		deduper:  deduper,
		wg:       wg,
		sem:      sem,
	}
	job := pipe.InitializeJob(ctx)
	jobManager, err := manager.NewHistoryJobManager(job, jobCache)
	if err != nil {
		return nil, fmt.Errorf("error creating job manager: %v", err)
	}
	pipe.jobManager = jobManager

	return pipe, nil
}

func (p *HistoryPipeline) InitializeJob(ctx context.Context) *fetch.HistoryFetchJob {
	// NOTE: Loading from history may lead to a few duplicates in file.
	// To prevent this, the last 1,000 signatures could be loaded into the deduper
	// however this was not implemented for this exercise
	job, err := p.jobCache.LoadHistoryJob()
	if err == nil {
		log.Info().Msgf("loaded job from cache, fetching backwards from signature %s", job.PreviousFetchSignature.String())
		return job
	}

	// No cached job found, fetch initial signature from chain
	signature := pipeline.FetchInitialSignature(ctx, p.fetcher)
	p.writer.WriteSignatures([]*rpc.TransactionSignature{signature})
	log.Info().Msgf("starting from signature %s", signature.Signature.String())

	// Add to deduper
	p.deduper.Dedupe([]*rpc.TransactionSignature{signature})

	return &fetch.HistoryFetchJob{
		PreviousFetchSignature: &signature.Signature,
		PreviousFetchBlocktime: *signature.BlockTime,
	}
}

func (p *HistoryPipeline) Run(ctx context.Context) {
	p.wg.Add(1)
	defer p.wg.Done()

	// Continuously fetch signatures and write to file.
	// This context is separate from the pipeline context so it can be cancelled
	// after the pipeline is completely done with execution.
	writerCtx, cancelWriter := context.WithCancel(context.Background())
	defer cancelWriter()
	go p.writer.Run(writerCtx)

	// Main pipeline logic loop
	for {
		select {
		case <-ctx.Done():
			return
		default:
			job := p.jobManager.GetJob()
			signatures, err := job.Execute(ctx, p.fetcher)
			if err != nil {
				// Log only if the context has not been cancelled
				select {
				case <-ctx.Done():
				default:
					log.Warn().Err(err).Msg("error executing standard job")
				}
				// If we get an error, we should retry the job
				continue
			}
			log.Debug().Msgf("Fetched %d signatures", len(signatures))

			// A response with no signatures means
			// that all signatures have been fetched up until the target sig.
			// It is time to update the target sig and start a new job.
			if len(signatures) == 0 {
				return
			}

			// Edge case: If 'before' returns signatures that
			// have the same timestamp, the request may always return signatures,
			// preventing job completion detection.
			// If the timestamps are the same for every sig, and all were deduped,
			// then we have reached the target signature.
			edgeCaseComplete := util.BatchHasAllIdenticalBlocktimes(job.PreviousFetchBlocktime.Time(), signatures)

			// Remove duplicates
			preDedupe := len(signatures)
			signatures = p.deduper.Dedupe(signatures)
			log.Debug().Msgf("Deduped %d signatures", preDedupe-len(signatures))

			// Free signatures that were deduped
			p.sem.Release(int64(preDedupe - len(signatures)))

			if len(signatures) > 0 {
				util.SortSignaturesReverse(signatures)
				p.writer.WriteSignatures(signatures)
				err := p.jobManager.NextJob(signatures)
				if err != nil {
					log.Fatal().Err(err).Msg("error updating job")
				}
			} else {
				if edgeCaseComplete {
					return
				}
			}
		}
	}
}
