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

type StandardPipeline struct {
	config *pipeline.PipelineConfig
	wg     *sync.WaitGroup

	fetcher  *fetch.RetryableFetcher
	deduper  *util.Deduper
	writer   *fileWriter.FileWriter
	jobCache *cache.JobCache

	jobManager *manager.StandardJobManager
	sem        *semaphore.Weighted
}

// A pipeline that only maintains 2000 signatures in memory at a time.
// 1,000 signatures are kept in the deduper at all times, leaving 1,000 only for new requests.
// Since an RPC fetch could be up to 1,000 signatures, new RPC requests are only made once
// all signatures from the previous request are comitted to file.
func NewStandardPipeline(ctx context.Context, config *pipeline.PipelineConfig, wg *sync.WaitGroup) (*StandardPipeline, error) {
	// Initialize semaphore to limit memory usage to 1000 signatures at a time
	sem := semaphore.NewWeighted(1000)

	// Initialize Signature RPC Fetcher
	fetcherConfig := &fetch.RetryableFetcherConfig{
		Commitment:         rpc.CommitmentFinalized,
		Address:            config.Address,
		RetryDelay:         50,
		FetchLimit:         config.FetchLimit,
		ZeroRequestRetries: 3,
		Sem:                sem,
	}
	fetcher := fetch.NewRetryableFetcher(config.RpcClient, fetcherConfig)

	// Intialize Signature Deduper that stores the last 1000 signatures
	// to prevent duplicates from being written to the file.
	deduper := util.NewDeduper(1000)

	// Initialize output file writer
	fileWriter, err := fileWriter.NewFileWriter(config.Filepath+".csv", config.WriteSizeLimit, 20, wg, sem)
	if err != nil {
		return nil, fmt.Errorf("error creating file writer: %v", err)
	}

	jobCache := cache.NewJobCache(config.Filepath)

	pipe := &StandardPipeline{
		config:   config,
		wg:       wg,
		fetcher:  fetcher,
		deduper:  deduper,
		writer:   fileWriter,
		jobCache: jobCache,
		sem:      sem,
	}

	// Initialize first standard fetch job
	job := pipe.InitializeJob(ctx)
	pipe.jobManager = manager.NewStandardJobManager(job, pipe.jobCache)

	return pipe, nil
}

// Creates first fetch job by first checking cache then fetching initial signature from Solana if necessary
func (p *StandardPipeline) InitializeJob(ctx context.Context) *fetch.StandardFetchJob {
	job, err := p.jobCache.LoadStandardJob()
	if err == nil {
		log.Info().Msgf("loaded job from cache, starting from signature %s", job.TargetSignature.String())
		return job
	}

	// No cached job found, fetch initial signature from chain
	signature := pipeline.FetchInitialSignature(ctx, p.fetcher)
	p.writer.WriteSignatures([]*rpc.TransactionSignature{signature})
	log.Info().Msgf("starting from signature %s", signature.Signature.String())

	return &fetch.StandardFetchJob{
		TargetSignature:        signature.Signature,
		TargetBlocktime:        *signature.BlockTime,
		PreviousFetchSignature: nil,
		PreviousFetchBlocktime: nil,
	}
}

func (p *StandardPipeline) Run(ctx context.Context) {
	defer p.wg.Done()
	p.wg.Add(1)

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
			// Execute RPC call
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
				// Edge case: Context cancellation causes a 0 signature response
				// leading to false positive. Ensure context is valid before writing signatures.
				if ctx.Err() == nil {
					p.jobManager.FinishJob()
				}

				continue
			}

			// Create next job but do not update the current job in case of edge cases
			// Use only consensus signatures to build the next job
			nextJob := p.jobManager.BuildNextJob(signatures)

			// Edge case: If 'until' and 'before' return signatures that
			// have the same timestamp, the request may always return signatures,
			// preventing our job completion detection.
			// If the timestamps are the same for every sig, and all were deduped,
			// then we have reached the target signature.
			completeEdgeCase := false
			if job.PreviousFetchSignature != nil {
				completeEdgeCase = util.SignaturesOnBlocktimeLimits(job.TargetBlocktime.Time(), job.PreviousFetchBlocktime.Time(), signatures)
			} else {
				completeEdgeCase = util.BatchHasAllIdenticalBlocktimes(job.TargetBlocktime.Time(), signatures)
			}

			// Remove duplicates
			preDedupe := len(signatures)
			signatures = p.deduper.Dedupe(signatures)
			log.Debug().Msgf("Deduped %d signatures", preDedupe-len(signatures))

			// Free signatures that were deduped
			p.sem.Release(int64(preDedupe - len(signatures)))

			// Process newly seen signatures
			if len(signatures) > 0 {
				log.Debug().Msg("updating standard job")
				p.jobManager.SetNextJob(nextJob)

				// Signatures don't need to be sorted, so can be written immediately
				p.writer.WriteSignatures(signatures)
			} else {
				// Handle edge case where all signatures have been deduped
				// and the signatures are at the same blocktime as either 'until' or 'before'
				if completeEdgeCase && preDedupe != p.config.FetchLimit {
					log.Debug().Msg("Identical timestamps detected, completing job")
					p.jobManager.FinishJob()
					continue
				}
			}
		}
	}
}
