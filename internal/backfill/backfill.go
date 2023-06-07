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
	"io/ioutil"
	"os"
	"regexp"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/rivian/delta-go"
	"github.com/rivian/delta-go/storage"
	"github.com/rivian/delta-go/storage/s3store"
	log "github.com/sirupsen/logrus"
	parquetS3 "github.com/xitongsys/parquet-go-source/s3v2"
	"github.com/xitongsys/parquet-go/reader"
)

var (
	bucketName   string
	objectPrefix string
	inputPath    string
	resultsPath  string
	batchSize    int = 2
)

func init() {
	flag.StringVar(&bucketName, "bucket", "vehicle-telemetry-fleet-prod", "The `name` of the S3 bucket to list objects from.")
	flag.StringVar(&objectPrefix, "prefix", "tables/v1/vehicle_fleet_delta_go_clone/", "The optional `object prefix` of the S3 Object keys to list.")
	flag.StringVar(&inputPath, "input-path", "/Users/rahulmadnawat/delta-go-logs/fleet_prod_backfill_files.txt", "The optional `input path` to read script results.")
	flag.StringVar(&resultsPath, "results-path", "/Users/rahulmadnawat/delta-go-logs/logs_test.txt", "The optional `results path` to store script results.")
}

func main() {
	flag.Parse()
	if len(bucketName) == 0 {
		flag.PrintDefaults()
		log.Fatal("invalid parameters, bucket name required")
	}

	insertUncommittedFiles()
}

func insertUncommittedFiles() {
	// insert logging path

	_, err := os.Create(resultsPath)
	if err != nil {
		log.Fatalf("failed to create results path %v", err)
	}

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

	table := delta.NewDeltaTable(store, nil, nil)

	var transaction *delta.DeltaTransaction

	uncommittedFiles, err := readLines(inputPath)
	if err != nil {
		log.Fatalf("failed reading results %v", err)
	}

	for fileNum, uncommittedFilePath := range uncommittedFiles {
		transaction = table.CreateTransaction(delta.NewDeltaTransactionOptions())
		var actions []delta.Action

		r, _ := regexp.Compile("(date=.+?)/.+")
		date := r.FindStringSubmatch(uncommittedFilePath)[1]
		partitionValues := map[string]string{"date": date}

		pr, err := parquetS3.NewS3FileReaderWithClient(context.TODO(), client, bucketName, objectPrefix+"/"+uncommittedFilePath)
		if err != nil {
			log.WithFields(log.Fields{"file": uncommittedFilePath}).Fatalf("failed to create reader %v", err)
			continue
		}
		reader, err := reader.NewParquetReader(pr, nil, 1)
		if err != nil {
			log.WithFields(log.Fields{"file": uncommittedFilePath}).Fatalf("failed to read file %v", err)
			continue
		}

		cb := reader.ColumnBuffers

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

		stats := delta.Stats{NumRecords: reader.Footer.NumRows, TightBounds: false, MinValues: minValues, MaxValues: maxValues, NullCount: nil}

		uncommittedFilePreview, err := store.Head(storage.NewPath(uncommittedFilePath))
		if err != nil {
			log.WithFields(log.Fields{"file": uncommittedFilePath}).Fatalf("failed to preview file %v", err)
			continue
		}

		add := delta.Add{
			Path:             uncommittedFilePath,
			Size:             delta.DeltaDataTypeLong(uncommittedFilePreview.Size),
			DataChange:       true,
			ModificationTime: delta.DeltaDataTypeTimestamp(uncommittedFilePreview.LastModified.UnixMilli()),
			Stats:            string(stats.Json()),
			PartitionValues:  partitionValues,
		}

		actions = append(actions, add)

		if fileNum > 0 && (fileNum%batchSize == 0 || fileNum == len(uncommittedFiles)-1) {
			logEntry, err := delta.LogEntryFromActions(actions)
			if err != nil {
				log.Fatalf("failed to retrieve log entry %v", err)
			}

			err = ioutil.WriteFile("file.txt", logEntry, 0644)
			if err != nil {
				log.Fatalf("failed to write log entry %v", err)
			}

			transaction.AddActions(actions)
			// commit transaction
		}
	}
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
