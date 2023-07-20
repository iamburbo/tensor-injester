package partC

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
)

type StandardPipeline struct {
	config *pipeline.PipelineConfig
	wg     *sync.WaitGroup

	fetcher  *MultiFetcher
	deduper  *MapDeduper
	writer   *fileWriter.FileWriter
	jobCache *cache.JobCache

	jobManager *manager.StandardJobManager

	// Variables used during pipeline runtime
	snapshot      []*rpc.TransactionSignature
	snapshotCache *cache.SnapshotCache
}

func NewStandardPipeline(ctx context.Context, config *pipeline.PipelineConfig, wg *sync.WaitGroup) (*StandardPipeline, error) {
	// Initialize Signature RPC Fetcher
	fetcherConfig := &fetch.RetryableFetcherConfig{
		Commitment:         rpc.CommitmentFinalized,
		Address:            config.Address,
		RetryDelay:         200,
		FetchLimit:         config.FetchLimit,
		ZeroRequestRetries: 3,
	}

	// Create MultiFetcher with 3 rpc fetchers and a consensus threshold of 2
	fetcher := NewMultiFetcher(config.RpcClient, fetcherConfig, 3, 2)

	// Intialize Signature Deduper that stores the last 10,000 signatures
	// to prevent duplicates from being written to the file.
	deduper := NewMapDeduper(30_000)

	// Initialize output file and job cache
	fileWriter, err := fileWriter.NewFileWriter(config.Filepath+".csv", config.WriteSizeLimit, 100, wg, nil)
	if err != nil {
		return nil, fmt.Errorf("error creating file writer: %v", err)
	}

	jobCache := cache.NewJobCache(config.Filepath)

	// Load previously saved snapshot if possible
	cachedSnapshot, err := cache.ReadSnapshotCache(config.Filepath + "_standard")
	if err != nil {
		return nil, fmt.Errorf("error reading snapshot cache: %v", err)
	}

	var snapshot []*rpc.TransactionSignature = nil
	if cachedSnapshot != nil {
		snapshot = cachedSnapshot
		util.SortSignatures(snapshot)
		deduper.DedupeSlice(snapshot) // Load snapshot into deduper for future requests
		log.Info().Msgf("Loaded %d signatures from snapshot", len(snapshot))
	} else {
		snapshot = make([]*rpc.TransactionSignature, 0)
	}

	snapshotCache, err := cache.NewSnapshotCache(config.Filepath + "_standard")
	if err != nil {
		return nil, fmt.Errorf("error creating snapshot cache: %v", err)
	}

	pipe := &StandardPipeline{
		config:        config,
		wg:            wg,
		fetcher:       fetcher,
		deduper:       deduper,
		writer:        fileWriter,
		jobCache:      jobCache,
		snapshot:      snapshot,
		snapshotCache: snapshotCache,
	}

	// Initialize first standard fetch job
	tempFetcher := fetch.NewRetryableFetcher(config.RpcClient, fetcherConfig)
	job := pipe.InitializeJob(ctx, tempFetcher)
	pipe.jobManager = manager.NewStandardJobManager(job, jobCache)

	return pipe, nil
}

// Creates first fetch job by first checking cache then fetching initial signature from Solana if necessary
func (p *StandardPipeline) InitializeJob(ctx context.Context, fetcher *fetch.RetryableFetcher) *fetch.StandardFetchJob {
	job, err := p.jobCache.LoadStandardJob()
	if err == nil {
		log.Info().Msgf("loaded job from cache, starting from signature %s", job.TargetSignature.String())
		return job
	}

	// No cached job found, fetch initial signature from chain
	signature := pipeline.FetchInitialSignature(ctx, fetcher)
	p.writer.WriteSignatures([]*rpc.TransactionSignature{signature})
	log.Info().Msgf("starting from signature %s", signature.Signature.String())

	return &fetch.StandardFetchJob{
		TargetSignature:        signature.Signature,
		TargetBlocktime:        *signature.BlockTime,
		PreviousFetchSignature: nil,
		PreviousFetchBlocktime: nil,
	}
}

func (p *StandardPipeline) FinishJob() {
	if p.jobManager.GetJob().PreviousFetchSignature == nil {
		log.Debug().Msg("no next target signature, re-running job")
		return
	}

	// Create next job and save to job cache. If this fails then the program will crash
	p.jobManager.FinishJob()

	// Clear snapshot before writing to file to prevent duplicates
	// in case this operation crashes. Since the next job is already cached,
	// crashing here means that this snapshot will be written when the next one
	// is complete.
	err := p.snapshotCache.Delete()
	if err != nil {
		log.Fatal().Err(err).Msg("error deleting snapshot cache")
	}

	// Sort and write snapshot to file
	util.SortSignatures(p.snapshot)
	p.writer.WriteSignatures(p.snapshot)

	// Reset
	p.snapshot = make([]*rpc.TransactionSignature, 0)
	log.Debug().
		Str("new_target", p.jobManager.GetJob().TargetSignature.String()).
		Str("new_target_blocktime", p.jobManager.GetJob().TargetBlocktime.String()).
		Msg("completing standard job")

	newSnapshotCache, err := cache.NewSnapshotCache(p.config.Filepath + "_standard")
	if err != nil {
		log.Fatal().Err(err).Msg("error creating snapshot cache")
	}
	p.snapshotCache = newSnapshotCache
}

// Add newly processed and sorted signatures to memory
// to later be added to file
func (p *StandardPipeline) AddToSnapshot(signatures []*rpc.TransactionSignature) {
	p.snapshot = append(p.snapshot, signatures...)
	p.snapshotCache.Add(signatures)
}

// Main pipeline loop
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
			signatures, err := p.fetcher.FetchSignatures(ctx, job.Execute)
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
			log.Debug().Msgf("Fetched %d signatures", signatures.Len())

			// A response with no signatures means
			// that all signatures have been fetched up until the target sig.
			// It is time to update the target sig and start a new job.
			if signatures.Len() == 0 {
				// Edge case: Context cancellation causes a 0 signature response
				// leading to false positive. Ensure context is valid before writing signatures.
				if ctx.Err() == nil {
					p.FinishJob()
				}

				continue
			}

			// Create next job but do not update the current job in case of edge cases
			// Use only consensus signatures to build the next job
			nextJob := p.jobManager.BuildNextJob(signatures.GetConsensusSignatures())

			// Edge case: If 'until' and 'before' return signatures that
			// have the same timestamp, the request may always return signatures,
			// preventing our job completion detection.
			// If the timestamps are the same for every sig, and all were deduped,
			// then we have reached the target signature.
			completeEdgeCase := false
			if job.PreviousFetchSignature != nil {
				completeEdgeCase = util.SignaturesOnBlocktimeLimits(job.TargetBlocktime.Time(), job.PreviousFetchBlocktime.Time(), signatures.GetAllSignatures())
			} else {
				completeEdgeCase = util.BatchHasAllIdenticalBlocktimes(job.TargetBlocktime.Time(), signatures.GetAllSignatures())
			}

			// Remove duplicates
			preDedupe := signatures.Len()
			p.deduper.Dedupe(signatures)
			log.Debug().Msgf("Deduped %d signatures", preDedupe-signatures.Len())

			// Process newly seen signatures
			if signatures.Len() > 0 {
				log.Debug().Msg("updating standard job")
				p.jobManager.SetNextJob(nextJob)

				// Include every signature in the snapshot, regardless of consensus
				// This decision heavily depends on the use case/context,
				// such as the reasons why a signature would only be included
				// in one RPC. If there are malicious actors actively trying to add
				// bad signatures to manipulate data, it would be better to leave them out.
				p.AddToSnapshot(signatures.GetAllSignatures())
			} else {
				// Handle edge case where all signatures have been deduped
				if completeEdgeCase && preDedupe != p.config.FetchLimit {
					log.Debug().Msg("Identical timestamps detected, completing job")
					p.FinishJob()
					continue
				}
			}
		}
	}
}
