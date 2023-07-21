package manager

import (
	"github.com/gagliardetto/solana-go/rpc"
	"github.com/iamburbo/tensor-injester/cache"
	"github.com/iamburbo/tensor-injester/fetch"
	"github.com/iamburbo/tensor-injester/util"
	"github.com/rs/zerolog/log"
)

type StandardJobManager struct {
	job      *fetch.StandardFetchJob
	jobCache *cache.JobCache
}

func NewStandardJobManager(initialJob *fetch.StandardFetchJob, jobCache *cache.JobCache) *StandardJobManager {
	err := jobCache.CacheStandardJob(initialJob)
	if err != nil {
		log.Fatal().Err(err).Msg("error caching standard job before job manager creation")
	}

	return &StandardJobManager{
		job:      initialJob,
		jobCache: jobCache,
	}
}

func (m *StandardJobManager) GetJob() *fetch.StandardFetchJob {
	return m.job
}

// Creates an updated job based on the previous job and its resulting signatures
func (m *StandardJobManager) BuildNextJob(signatures []*rpc.TransactionSignature) *fetch.StandardFetchJob {
	earliestEntry := util.GetEarliestSignatureFromBatch(signatures)
	updatedJob := &fetch.StandardFetchJob{
		TargetSignature:        m.job.TargetSignature,
		TargetBlocktime:        m.job.TargetBlocktime,
		PreviousFetchSignature: &earliestEntry.Signature,
		PreviousFetchBlocktime: earliestEntry.BlockTime,
		NextTargetSignature:    m.job.NextTargetSignature,
		NextTargetBlocktime:    m.job.NextTargetBlocktime,
	}

	if m.job.NextTargetSignature == nil {
		latestEntry := util.GetLatestSignatureFromBatch(signatures)
		updatedJob.NextTargetSignature = &latestEntry.Signature
		updatedJob.NextTargetBlocktime = latestEntry.BlockTime
	}

	return updatedJob
}

// Update the next job to be run
func (m *StandardJobManager) SetNextJob(job *fetch.StandardFetchJob) {
	log.Trace().Interface("job", job).Interface("previousJob", m.job).Msg("setting next job")
	err := m.jobCache.CacheStandardJob(job)
	if err != nil {
		log.Fatal().Err(err).Msg("failed to cache job")
	}

	m.job = job
}

// When a job has reached its target signature and completes its batch,
// The next job should fetch until the latest signature from this batch of jobs
func (m *StandardJobManager) FinishJob() {
	// Edge case: A job considered "complete" after only one request.
	// This means no new signatures were found in this job batch,
	// and the job should be re-run.
	if m.job.NextTargetSignature == nil {
		log.Debug().Msg("no next target signature, re-running job")
		return
	}

	newJob := &fetch.StandardFetchJob{
		TargetSignature:        *m.job.NextTargetSignature,
		TargetBlocktime:        *m.job.NextTargetBlocktime,
		PreviousFetchSignature: nil,
		PreviousFetchBlocktime: nil,
		NextTargetSignature:    nil,
		NextTargetBlocktime:    nil,
	}

	m.SetNextJob(newJob)
}

type HistoryJobManager struct {
	job      *fetch.HistoryFetchJob
	jobCache *cache.JobCache
}

func NewHistoryJobManager(initialJob *fetch.HistoryFetchJob, jobCache *cache.JobCache) (*HistoryJobManager, error) {
	err := jobCache.CacheHistoryJob(initialJob)
	if err != nil {
		return nil, err
	}

	return &HistoryJobManager{
		job:      initialJob,
		jobCache: jobCache,
	}, nil
}

func (m *HistoryJobManager) GetJob() *fetch.HistoryFetchJob {
	return m.job
}

func (m *HistoryJobManager) NextJob(signatures []*rpc.TransactionSignature) error {
	earliest := util.GetEarliestSignatureFromBatch(signatures)

	m.job = &fetch.HistoryFetchJob{
		PreviousFetchSignature: &earliest.Signature,
		PreviousFetchBlocktime: *earliest.BlockTime,
	}

	err := m.jobCache.CacheHistoryJob(m.job)
	if err != nil {
		return err
	}

	return nil
}
