// Copyright 2023 Rivian Automotive, Inc.
// Licensed under the Apache License, Version 2.0 (the “License”);
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an “AS IS” BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
package main

import (
	"bufio"
	"context"
	"encoding/binary"
	"flag"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/rivian/delta-go"
	"github.com/rivian/delta-go/lock/nillock"
	"github.com/rivian/delta-go/state/localstate"
	"github.com/rivian/delta-go/storage"
	"github.com/rivian/delta-go/storage/s3store"
	log "github.com/sirupsen/logrus"
	parquetS3 "github.com/xitongsys/parquet-go-source/s3v2"
	"github.com/xitongsys/parquet-go/reader"
	"golang.org/x/exp/constraints"
)

// Insert RowType and PartitionType structs here (compilation will otherwise fail)

var (
	bucketName      string
	objectPrefix    string
	scriptDir       string
	inputPath       string
	loggingPath     string
	resultsPath     string
	batchSize       int
	minBatchNum     int
	maxBatchNum     int
	dryRun          bool
	writeLogEntries bool
)

func init() {
	flag.StringVar(&bucketName, "bucket", "vehicle-telemetry-fleet-prod", "The `name` of the S3 bucket to list objects from.")
	flag.StringVar(&objectPrefix, "prefix", "tables/v1/vehicle_fleet_delta_go_clone/", "The optional `object prefix` of the S3 Object keys to list.")
	flag.StringVar(&scriptDir, "script-directory", "/Users/rahulmadnawat/delta-go-logs/fleet-prod-backfill", "The `script directory` in which to keep script files.")
	flag.StringVar(&inputPath, "input-path", "files.txt", "The `input path` from which to read script results.")
	flag.StringVar(&loggingPath, "logging-path", "logs.txt", "The `logging path` to which to store script logs.")
	flag.StringVar(&resultsPath, "results-path", "log_entry.txt", "The `results path` to which to store script results.")
	flag.IntVar(&batchSize, "batch-size", 20000, "The `batch size` used to incrementally process untracked files.")
	flag.IntVar(&minBatchNum, "min-batch-number", 1, "The `minimum batch number` to commit.")
	flag.IntVar(&maxBatchNum, "max-batch-number", math.MaxInt64, "The `maximum batch number` to commit.")
	flag.BoolVar(&dryRun, "dry-run", true, "To avoid committing transactions, enable `dry run`.")
	flag.BoolVar(&writeLogEntries, "write-log-entries", true, "To save log entries on disk, enable `write log entries`.")
}

func main() {
	CommitLogEntries()
}

func GetPathsFromActions(actions []delta.Action) ([]string, []delta.Action) {
	var paths []string
	var actionsWithPaths []delta.Action

	for _, action := range actions {
		switch action.(type) {
		case *delta.Add[FlatRecord, TestPartitionType]:
			add, _ := action.(*delta.Add[FlatRecord, TestPartitionType])
			paths = append(paths, add.Path)
			actionsWithPaths = append(actionsWithPaths, action)
		case *delta.Remove:
			remove, _ := action.(*delta.Remove)
			paths = append(paths, remove.Path)
			actionsWithPaths = append(actionsWithPaths, action)
		case *delta.CommitInfo:
			continue
		}
	}

	return paths, actionsWithPaths
}

func CleanUpLogEntries() {
	uncommittedFiles, err := readLines(filepath.Join(scriptDir, inputPath))
	if err != nil {
		log.Fatalf("failed reading results %v", err)
	}

	numBatches := 1 + (len(uncommittedFiles)-1)/batchSize

	for entryNum := 1; entryNum <= numBatches; entryNum++ {
		err := os.Remove(strings.Replace(filepath.Join(scriptDir, resultsPath), ".", fmt.Sprintf("_%d.", entryNum+1), 1))
		if err != nil {
			log.WithFields(log.Fields{"entry number": entryNum}).Fatalf("failed to delete file for entry %d of %d %v", entryNum, numBatches, err)
		}
	}
}

func CommitLogEntries() {
	flag.Parse()
	if len(bucketName) == 0 {
		flag.PrintDefaults()
		log.Fatal("invalid parameters, bucket name required")
	}

	loggingPath = "logs_commit.txt"
	f, err := os.OpenFile(filepath.Join(scriptDir, loggingPath), os.O_WRONLY|os.O_CREATE, 0755)
	if err != nil {
		log.Fatalf("failed creating file: %v", err)
	}
	log.SetOutput(f)

	objectPrefix = strings.TrimSuffix(objectPrefix, "/")

	cfg, err := config.LoadDefaultConfig(context.TODO())
	if err != nil {
		log.Fatalf("failed to load SDK configuration %v", err)
	}

	client := s3.NewFromConfig(cfg)

	store, err := s3store.New(client, storage.NewPath(fmt.Sprintf("s3://%s/%s", bucketName, objectPrefix)))
	if err != nil {
		log.Fatalf("failed to set up S3 store %v", err)
	}

	storeState := localstate.New(-1) // the version will be pulled from the remote state, so this initialization shouldn't matter
	lock := nillock.New()

	table := delta.NewDeltaTable[FlatRecord, TestPartitionType](store, lock, storeState)

	uncommittedFiles, err := readLines(filepath.Join(scriptDir, inputPath))
	if err != nil {
		log.Fatalf("failed reading results %v", err)
	}

	numBatches := min(1+(len(uncommittedFiles)-1)/batchSize, maxBatchNum)

	var transaction *delta.DeltaTransaction[FlatRecord, TestPartitionType]

	operation := delta.Write{Mode: delta.Append, PartitionBy: []string{"date"}}
	appMetaData := make(map[string]any)
	appMetaData["isBlindAppend"] = true

	for batchNum := max(1, minBatchNum); batchNum <= numBatches; batchNum++ {
		transaction = table.CreateTransaction(delta.NewDeltaTransactionOptions())

		logEntry, err := os.ReadFile(strings.Replace(filepath.Join(scriptDir, resultsPath), ".", fmt.Sprintf("_%d.", batchNum+1), 1))
		if err != nil {
			log.WithFields(log.Fields{"batch number": batchNum}).Fatalf("failed to read file for entry %d of %d %v", batchNum, numBatches, err)
		}

		actions, err := delta.ActionsFromLogEntries[FlatRecord, TestPartitionType](logEntry)
		if err != nil {
			log.WithFields(log.Fields{"batch number": batchNum}).Fatalf("failed to get actions %v", err)
		}

		transaction.AddActions(actions)

		if !dryRun {
			version, err := transaction.Commit(operation, appMetaData)
			if err != nil {
				log.WithFields(log.Fields{"batch number": batchNum}).Fatalf("failed to commit version %d, %v", version, err)
			}

			log.WithFields(log.Fields{"batch number": batchNum}).Infof("Committed version %d", version)
		}
	}
}

func WriteLogEntries(logEntries [][]byte) {
	for entryNum, logEntry := range logEntries {
		err := os.WriteFile(strings.Replace(filepath.Join(scriptDir, resultsPath), ".", fmt.Sprintf("_%d.", entryNum+1), 1), logEntry, 0644)
		if err != nil {
			log.Fatalf("failed to write entry %d of %d to file %v", entryNum+1, len(logEntries), err)
		}
	}
}

func CreateLogEntries(uncommittedFiles []string) [][]byte {
	flag.Parse()
	if len(bucketName) == 0 {
		flag.PrintDefaults()
		log.Fatal("invalid parameters, bucket name required")
	}

	os.MkdirAll(scriptDir, os.ModePerm)

	f, err := os.OpenFile(filepath.Join(scriptDir, loggingPath), os.O_WRONLY|os.O_CREATE, 0755)
	if err != nil {
		log.Fatalf("failed creating file: %v", err)
	}
	log.SetOutput(f)

	objectPrefix = strings.TrimSuffix(objectPrefix, "/")

	cfg, err := config.LoadDefaultConfig(context.TODO())
	if err != nil {
		log.Fatalf("failed to load SDK configuration %v", err)
	}

	client := s3.NewFromConfig(cfg)

	store, err := s3store.New(client, storage.NewPath(fmt.Sprintf("s3://%s/%s", bucketName, objectPrefix)))
	if err != nil {
		log.Fatalf("failed to set up S3 store %v", err)
	}

	if len(uncommittedFiles) == 0 {
		uncommittedFiles, err = readLines(filepath.Join(scriptDir, inputPath))
		if err != nil {
			log.Fatalf("failed reading results %v", err)
		}
	}

	numBatches := 1 + (len(uncommittedFiles)-1)/batchSize

	var logEntries [][]byte

	var wg = &sync.WaitGroup{}
	for batchNum := 0; batchNum < numBatches; batchNum++ {
		wg.Add(1)
		go func(batchNum int) {
			var actions []delta.Action

			for fileNum := 0; fileNum < min(batchSize, len(uncommittedFiles)-batchNum*batchSize); fileNum++ {
				r, _ := regexp.Compile("date=(.+?)/.+")
				date := r.FindStringSubmatch(uncommittedFiles[batchNum*batchSize+fileNum])[1]
				partitionValues := map[string]string{"date": date}

				pr, err := parquetS3.NewS3FileReaderWithClient(context.TODO(), client, bucketName, objectPrefix+"/"+uncommittedFiles[batchNum*batchSize+fileNum])
				if err != nil {
					log.WithFields(log.Fields{"file": uncommittedFiles[batchNum*batchSize+fileNum]}).Fatalf("failed to create reader %v", err)
					continue
				}
				reader, err := reader.NewParquetReader(pr, nil, 1)
				if err != nil {
					log.WithFields(log.Fields{"file": uncommittedFiles[batchNum*batchSize+fileNum]}).Fatalf("failed to read file %v", err)
					continue
				}

				cb := reader.ColumnBuffers

				// TODO: Figure out if these min and max values are limited to a single page or the whole file
				// Should we collects stats for all the columns?
				minValues := map[string]any{"commit_id": string(cb["Parquet_go_root\x01Tcm_commit_id"].ChunkHeader.MetaData.Statistics.MinValue),
					"dbc_path":                   string(cb["Parquet_go_root\x01Dbc_path"].ChunkHeader.MetaData.Statistics.MinValue),
					"id":                         string(cb["Parquet_go_root\x01Id"].ChunkHeader.MetaData.Statistics.MinValue),
					"source_path":                string(cb["Parquet_go_root\x01Source_path"].ChunkHeader.MetaData.Statistics.MinValue),
					"source_processed_timestamp": time.UnixMicro(int64(binary.LittleEndian.Uint64(cb["Parquet_go_root\x01Source_processed_timestamp"].ChunkHeader.MetaData.Statistics.MinValue))).In(time.UTC).Format(time.RFC3339Nano),
					"source_uploaded_timestamp":  time.UnixMicro(int64(binary.LittleEndian.Uint64(cb["Parquet_go_root\x01Source_uploaded_timestamp"].ChunkHeader.MetaData.Statistics.MinValue))).In(time.UTC).Format(time.RFC3339Nano),
					"sw_version":                 string(cb["Parquet_go_root\x01Tcm_sw_version"].ChunkHeader.MetaData.Statistics.MinValue),
					"timestamp":                  time.UnixMicro(int64(binary.LittleEndian.Uint64(cb["Parquet_go_root\x01Timestamp"].ChunkHeader.MetaData.Statistics.MinValue))).In(time.UTC).Format(time.RFC3339Nano)}

				maxValues := map[string]any{"commit_id": string(cb["Parquet_go_root\x01Tcm_commit_id"].ChunkHeader.MetaData.Statistics.MaxValue),
					"dbc_path":                   string(cb["Parquet_go_root\x01Dbc_path"].ChunkHeader.MetaData.Statistics.MaxValue),
					"id":                         string(cb["Parquet_go_root\x01Id"].ChunkHeader.MetaData.Statistics.MaxValue),
					"source_path":                string(cb["Parquet_go_root\x01Source_path"].ChunkHeader.MetaData.Statistics.MaxValue),
					"source_processed_timestamp": time.UnixMicro(int64(binary.LittleEndian.Uint64(cb["Parquet_go_root\x01Source_processed_timestamp"].ChunkHeader.MetaData.Statistics.MaxValue))).In(time.UTC).Format(time.RFC3339Nano),
					"source_uploaded_timestamp":  time.UnixMicro(int64(binary.LittleEndian.Uint64(cb["Parquet_go_root\x01Source_uploaded_timestamp"].ChunkHeader.MetaData.Statistics.MaxValue))).In(time.UTC).Format(time.RFC3339Nano),
					"sw_version":                 string(cb["Parquet_go_root\x01Tcm_sw_version"].ChunkHeader.MetaData.Statistics.MinValue),
					"timestamp":                  time.UnixMicro(int64(binary.LittleEndian.Uint64(cb["Parquet_go_root\x01Timestamp"].ChunkHeader.MetaData.Statistics.MaxValue))).In(time.UTC).Format(time.RFC3339Nano)}

				stats := delta.Stats{NumRecords: reader.Footer.NumRows, TightBounds: true, MinValues: minValues, MaxValues: maxValues, NullCount: nil}

				uncommittedFilePreview, err := store.Head(storage.NewPath(uncommittedFiles[batchNum*batchSize+fileNum]))
				if err != nil {
					log.WithFields(log.Fields{"file": uncommittedFiles[batchNum*batchSize+fileNum]}).Fatalf("failed to preview file %v", err)
					continue
				}

				add := delta.Add[FlatRecord, TestPartitionType]{
					Path:             uncommittedFiles[batchNum*batchSize+fileNum],
					Size:             delta.DeltaDataTypeLong(uncommittedFilePreview.Size),
					DataChange:       true,
					ModificationTime: delta.DeltaDataTypeTimestamp(uncommittedFilePreview.LastModified.UnixMilli()),
					Stats:            string(stats.Json()),
					PartitionValues:  partitionValues,
				}

				actions = append(actions, add)

				if fileNum > 0 && fileNum%(min(batchSize, len(uncommittedFiles)-batchNum*batchSize)-1) == 0 {
					logEntry, err := delta.LogEntryFromActions[FlatRecord, TestPartitionType](actions)
					if err != nil {
						log.WithFields(log.Fields{"file number": fileNum}).Fatalf("failed to retrieve log entry %v", err)
					}

					logEntries = append(logEntries, logEntry)

					if writeLogEntries {
						err := os.WriteFile(strings.Replace(filepath.Join(scriptDir, resultsPath), ".", fmt.Sprintf("_%d.", batchNum+1), 1), logEntry, 0644)
						if err != nil {
							log.WithFields(log.Fields{"file number": fileNum}).Fatalf("failed to write entry %d of %d to file %v", batchNum+1, numBatches, err)
						}
					}

					log.WithFields(log.Fields{"file number": fileNum}).Infof("Finished batch %d of %d", batchNum+1, numBatches)
				}

				log.WithFields(log.Fields{"file": uncommittedFiles[batchNum*batchSize+fileNum]}).Infof("Processed file %d of %d in batch %d ", fileNum+1, min(batchSize, len(uncommittedFiles)-batchNum*batchSize), batchNum+1)
			}
			wg.Done()
		}(batchNum)
	}
	wg.Wait()

	return logEntries
}

func readLines(path string) ([]string, error) {
	file, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer file.Close()

	var lines []string
	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		lines = append(lines, scanner.Text())
	}
	return lines, scanner.Err()
}

func min[T constraints.Ordered](a, b T) T {
	if a < b {
		return a
	}
	return b
}

func max[T constraints.Ordered](a, b T) T {
	if a > b {
		return a
	}
	return b
}
