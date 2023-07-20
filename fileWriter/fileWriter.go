package fileWriter

import (
	"context"
	"encoding/csv"
	"os"
	"strconv"
	"sync"

	"github.com/gagliardetto/solana-go/rpc"
	"github.com/rs/zerolog/log"
	"golang.org/x/sync/semaphore"
)

type FileWriter struct {
	Filename     string
	sigWriteSize int
	file         *os.File
	csvWriter    *csv.Writer

	writeChan chan []*rpc.TransactionSignature
	wg        *sync.WaitGroup
	writeWg   *sync.WaitGroup

	// Optional semaphore when keeping track of
	// memory usage is important.
	sem *semaphore.Weighted
}

func NewFileWriter(filename string, sigWriteSize, writeChanSize int, wg *sync.WaitGroup, sem *semaphore.Weighted) (*FileWriter, error) {
	file, err := os.OpenFile(filename, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		return nil, err
	}

	writer := csv.NewWriter(file)

	// Write header only if file is empty
	stat, err := file.Stat()
	if err != nil {
		return nil, err
	}
	if stat.Size() == 0 {
		writer.Write([]string{"sig", "blockTime"})
	}

	return &FileWriter{
		Filename:     filename,
		sigWriteSize: sigWriteSize,
		file:         file,
		csvWriter:    writer,
		writeChan:    make(chan []*rpc.TransactionSignature, writeChanSize),
		wg:           wg,
		writeWg:      &sync.WaitGroup{},
		sem:          sem,
	}, nil
}

func (f *FileWriter) Flush() {
	f.csvWriter.Flush()
}

func (f *FileWriter) Close() {
	f.csvWriter.Flush()
	f.file.Close()
}

func (f *FileWriter) WriteSignature(sig *rpc.TransactionSignature) {
	blocktimeStr := strconv.Itoa(int(sig.BlockTime.Time().Unix()))

	err := f.csvWriter.Write([]string{sig.Signature.String(), blocktimeStr})
	if err != nil {
		log.Fatal().Msgf("Error writing to file: %v", err)
	}
}

// Divides a batch of signatures into batches of size sigWriteSize and sends them to the writer
// NOTE: The writer does not stop writing until the input channel is cleared. If its context is done,
// this function should not be called anywhere
func (f *FileWriter) WriteSignatures(signatures []*rpc.TransactionSignature) {
	log.Debug().Msgf("Writing %d signatures to file", len(signatures))
	f.writeWg.Add(1)
	defer f.writeWg.Done()

	for i := 0; i < len(signatures); i += f.sigWriteSize {
		end := i + f.sigWriteSize
		if end > len(signatures) {
			end = len(signatures)
		}
		writeBatch := signatures[i:end]
		f.writeChan <- writeBatch
	}
}

// Goroutine for writing to file
func (f *FileWriter) Run(ctx context.Context) {
	f.wg.Add(1)
	defer f.wg.Done()

	for {
		// If context is done, continue writing until the channel is drained
		if ctx.Err() != nil {
			log.Debug().Msg("context done, draining write channel")

			// Once all writes have been inserted into the channel, close it.
			go func() {
				f.writeWg.Wait()
				close(f.writeChan)
			}()

			for sigs := range f.writeChan {
				f.process(sigs)
			}

			// Close the channel and file after draining the channel
			f.file.Close()
			log.Debug().Msg("drained write channel")
			return
		}

		select {
		case <-ctx.Done():
			// Continue to the top of the loop to start draining the channel
			continue
		case sigs := <-f.writeChan:
			f.process(sigs)
		}
	}
}

func (f *FileWriter) process(sigs []*rpc.TransactionSignature) {
	// Sanity check
	if len(sigs) > f.sigWriteSize {
		log.Fatal().Msgf("Attempting to write %d signatures, but max write size is %d", len(sigs), f.sigWriteSize)
	}

	for _, sig := range sigs {
		f.WriteSignature(sig)
	}

	// Flush often so data is written to disk in a timely manner
	f.csvWriter.Flush()

	// If a semaphore has been provided,
	// release a slot for each signature written.
	if f.sem != nil {
		f.sem.Release(int64(len(sigs)))
		log.Trace().Msgf("fileWriter: released %d semaphore slots", len(sigs))
	}
}
