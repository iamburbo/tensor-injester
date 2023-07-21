package cache

import (
	"encoding/json"
	"fmt"
	"os"

	"github.com/gagliardetto/solana-go"
	"github.com/iamburbo/tensor-injester/fetch"
)

const STANDARD_JOB_SUFFIX = "_standard_cache.json"
const HISTORY_JOB_SUFFIX = "_history_cache.json"

type JobCache struct {
	FilePrefix string
}

func NewJobCache(filePrefix string) *JobCache {
	return &JobCache{
		FilePrefix: filePrefix,
	}
}

type CachedStandardJob struct {
	TargetSignature string `json:"targetSignature"`
	TargetBlocktime int64  `json:"targetBlocktime"`

	PreviousFetchSignature *string `json:"previousFetchSignature,omitempty"`
	PreviousFetchBlocktime *int64  `json:"previousFetchBlocktime,omitempty"`

	NextTargetSignature *string `json:"nextTarget,omitempty"`
	NextTargetBlocktime *int64  `json:"nextTargetBlocktime,omitempty"`
}

func (j *CachedStandardJob) Convert() (*fetch.StandardFetchJob, error) {
	targetSignature, err := solana.SignatureFromBase58(j.TargetSignature)
	if err != nil {
		return nil, fmt.Errorf("failed to convert TargetSignature: %w", err)
	}

	job := &fetch.StandardFetchJob{
		TargetSignature: targetSignature,
		TargetBlocktime: solana.UnixTimeSeconds(j.TargetBlocktime),
	}

	if j.PreviousFetchSignature != nil {
		previousFetchSignature, err := solana.SignatureFromBase58(*j.PreviousFetchSignature)
		if err != nil {
			return nil, fmt.Errorf("failed to convert PreviousFetchSignature: %w", err)
		}

		previousFetchBlocktime := solana.UnixTimeSeconds(*j.PreviousFetchBlocktime)

		job.PreviousFetchSignature = &previousFetchSignature
		job.PreviousFetchBlocktime = &previousFetchBlocktime
	}

	if j.NextTargetSignature != nil {
		nextTargetSignature, err := solana.SignatureFromBase58(*j.NextTargetSignature)
		if err != nil {
			return nil, fmt.Errorf("failed to convert NextTargetSignature: %w", err)
		}

		nextTargetBlocktime := solana.UnixTimeSeconds(*j.NextTargetBlocktime)

		job.NextTargetSignature = &nextTargetSignature
		job.NextTargetBlocktime = &nextTargetBlocktime
	}

	return job, nil
}

// Save standard job details to file to be used
// for recovering from crashes.
func (c *JobCache) CacheStandardJob(j *fetch.StandardFetchJob) error {
	// Marshall job to JSON
	cachedJob := &CachedStandardJob{
		TargetSignature: j.TargetSignature.String(),
		TargetBlocktime: j.TargetBlocktime.Time().Unix(),
	}
	if j.PreviousFetchSignature != nil {
		previousFetchSignature := j.PreviousFetchSignature.String()
		previousFetchBlocktime := j.PreviousFetchBlocktime.Time().Unix()
		cachedJob.PreviousFetchSignature = &previousFetchSignature
		cachedJob.PreviousFetchBlocktime = &previousFetchBlocktime
	}
	if j.NextTargetSignature != nil {
		nextTargetSignature := j.NextTargetSignature.String()
		nextTargetBlocktime := j.NextTargetBlocktime.Time().Unix()
		cachedJob.NextTargetSignature = &nextTargetSignature
		cachedJob.NextTargetBlocktime = &nextTargetBlocktime
	}

	marshalled, err := json.Marshal(cachedJob)
	if err != nil {
		return fmt.Errorf("failed to marshal job: %w", err)
	}

	// Save to cache file
	filename := c.FilePrefix + STANDARD_JOB_SUFFIX
	err = os.WriteFile(filename, marshalled, 0644)
	if err != nil {
		return fmt.Errorf("failed to save job cache: %w", err)
	}

	return nil
}

// Attempt to load a standard job from cache.
// If successful, this means a previous job was not completed
// and the program crashed.
func (c *JobCache) LoadStandardJob() (*fetch.StandardFetchJob, error) {
	filename := c.FilePrefix + STANDARD_JOB_SUFFIX

	data, err := os.ReadFile(filename)
	if err != nil {
		return nil, fmt.Errorf("failed to load job cache: %w", err)
	}

	var cachedJob CachedStandardJob
	err = json.Unmarshal(data, &cachedJob)
	if err != nil {
		return nil, fmt.Errorf("failed to unmarshal job cache: %w", err)
	}

	converted, err := cachedJob.Convert()
	if err != nil {
		return nil, fmt.Errorf("failed to convert cached job: %w", err)
	}

	return converted, nil
}

type CachedHistoryJob struct {
	PreviousFetchSignature string `json:"previousFetchSignature,omitempty"`
	PreviousFetchBlocktime int64  `json:"previousFetchBlocktime,omitempty"`
}

func (j *CachedHistoryJob) Convert() (*fetch.HistoryFetchJob, error) {
	previousFetchSignature, err := solana.SignatureFromBase58(j.PreviousFetchSignature)
	if err != nil {
		return nil, fmt.Errorf("failed to convert PreviousFetchSignature: %w", err)
	}

	previousFetchBlocktime := solana.UnixTimeSeconds(j.PreviousFetchBlocktime)

	return &fetch.HistoryFetchJob{
		PreviousFetchSignature: &previousFetchSignature,
		PreviousFetchBlocktime: previousFetchBlocktime,
	}, nil
}

func (c *JobCache) CacheHistoryJob(job *fetch.HistoryFetchJob) error {
	cachedJob := &CachedHistoryJob{
		PreviousFetchSignature: job.PreviousFetchSignature.String(),
		PreviousFetchBlocktime: job.PreviousFetchBlocktime.Time().Unix(),
	}

	marshalled, err := json.Marshal(cachedJob)
	if err != nil {
		return fmt.Errorf("failed to marshal job: %w", err)
	}

	filename := c.FilePrefix + HISTORY_JOB_SUFFIX
	err = os.WriteFile(filename, marshalled, 0644)
	if err != nil {
		return fmt.Errorf("failed to save job cache: %w", err)
	}

	return nil
}

func (c *JobCache) LoadHistoryJob() (*fetch.HistoryFetchJob, error) {
	filename := c.FilePrefix + HISTORY_JOB_SUFFIX

	data, err := os.ReadFile(filename)
	if err != nil {
		return nil, fmt.Errorf("failed to load job cache: %w", err)
	}

	var cachedJob CachedHistoryJob
	err = json.Unmarshal(data, &cachedJob)
	if err != nil {
		return nil, fmt.Errorf("failed to unmarshal job cache: %w", err)
	}

	converted, err := cachedJob.Convert()
	if err != nil {
		return nil, fmt.Errorf("failed to convert cached job: %w", err)
	}

	return converted, nil
}
