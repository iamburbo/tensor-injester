package cache

import (
	"encoding/csv"
	"io"
	"os"
	"strconv"

	"github.com/gagliardetto/solana-go"
	"github.com/gagliardetto/solana-go/rpc"
	"github.com/rs/zerolog/log"
)

// Parts A and C write signatures in ascending order,
// so they must be saved in a snapshot file until an entire
// batch is fetched
type SnapshotCache struct {
	file     *os.File
	writer   *csv.Writer
	filename string
}

const SNAPSHOT_FILE_SUFFIX = "_snapshot.csv"

func ReadSnapshotCache(filePrefix string) ([]*rpc.TransactionSignature, error) {
	filename := filePrefix + SNAPSHOT_FILE_SUFFIX
	file, err := os.Open(filename) // Open the file for reading
	if err != nil {
		// If the file does not exist, just return nil
		if os.IsNotExist(err) {
			return nil, nil
		}
		return nil, err
	}
	defer file.Close()

	reader := csv.NewReader(file)

	signatures := make([]*rpc.TransactionSignature, 0)
	for {
		line, err := reader.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			log.Fatal().Err(err).Msg("error reading snapshot cache")
		}

		sig := solana.MustSignatureFromBase58(line[0])
		blocktimeInt, err := strconv.Atoi(line[1])
		if err != nil {
			log.Fatal().Err(err).Msg("error parsing snapshot cache")
		}
		blocktime := solana.UnixTimeSeconds(blocktimeInt)

		signatures = append(signatures, &rpc.TransactionSignature{
			Signature: sig,
			BlockTime: &blocktime,
		})
	}

	return signatures, nil
}

func NewSnapshotCache(filePrefix string) (*SnapshotCache, error) {
	filename := filePrefix + SNAPSHOT_FILE_SUFFIX
	file, err := os.OpenFile(filename, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		return nil, err
	}

	writer := csv.NewWriter(file)

	return &SnapshotCache{
		file:     file,
		writer:   writer,
		filename: filename,
	}, nil
}

// Appends signatures to bottom of cache.
// Signatures should be sorted before being added.
func (c *SnapshotCache) Add(signatures []*rpc.TransactionSignature) {
	for _, sig := range signatures {
		blocktimeStr := strconv.Itoa(int(sig.BlockTime.Time().Unix()))

		err := c.writer.Write([]string{sig.Signature.String(), blocktimeStr})
		if err != nil {
			log.Fatal().Msgf("Error writing to file: %v", err)
		}
	}
	c.writer.Flush()
}

func (c *SnapshotCache) Delete() error {
	c.writer.Flush()
	err := c.file.Close()
	if err != nil {
		return err
	}

	err = os.Remove(c.filename)
	if err != nil {
		return err
	}
	return nil
}
