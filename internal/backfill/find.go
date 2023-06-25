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
	"encoding/json"
	"flag"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/rivian/delta-go"
	"github.com/rivian/delta-go/storage"
	"github.com/rivian/delta-go/storage/s3store"
	log "github.com/sirupsen/logrus"
	"golang.org/x/exp/constraints"
)

// Insert RowType and PartitionType structs here (compilation will otherwise fail)

type CommitInfo struct {
	Timestamp float64 `json:"timestamp"`
}

var (
	bucketName        string
	objectPrefix      string
	scriptDir         string
	minDate           string
	maxDate           string
	inputPath         string
	loggingPath       string
	resultsPath       string
	batchSize         int
	minBatchNum       int
	maxBatchNum       int
	commitWaitMinutes time.Duration = 20
)

func init() {
	flag.StringVar(&bucketName, "bucket", "vehicle-telemetry-fleet-prod", "The `name` of the S3 bucket to list objects from.")
	flag.StringVar(&objectPrefix, "prefix", "tables/v1/vehicle_fleet/", "The optional `object prefix` of the S3 Object keys to list.")
	flag.StringVar(&scriptDir, "script-directory", "/Users/rahulmadnawat/delta-go-logs/fleet-prod-backfill-non-clone-v3", "The `script directory` in which to keep script files.")
	// TODO: The minimum and maximum dates should be automatically generated and overriden by these flags.
	flag.StringVar(&minDate, "min_date", "", "The optional `minimum date` for the date partitions to cover.")
	flag.StringVar(&maxDate, "max_date", "", "The optional `maximum date` for the date partitions to cover.")
	flag.StringVar(&inputPath, "input-path", "files.txt", "The optional `input path` to read script results.")
	flag.StringVar(&loggingPath, "logging-path", "logs_find.txt", "The optional `logging path` to store script logs.")
	flag.StringVar(&resultsPath, "results-path", "files.txt", "The optional `results path` to store script results.")
	flag.IntVar(&batchSize, "batch-size", 2000, "The `batch size` used to incrementally process untracked files.")
	flag.IntVar(&minBatchNum, "min-batch-number", 1, "The `minimum batch number` to commit.")
	flag.IntVar(&maxBatchNum, "max-batch-number", math.MaxInt64, "The `maximum batch number` to commit.")
}

func main() {
	findUncommittedFiles()
}

func removeSnappyFiles() {
	resultsWithSnappy, err := readLines(filepath.Join(scriptDir, inputPath))
	if err != nil {
		log.Fatalf("failed reading results")
	}

	var resultsWithoutSnappy []string

	for _, uncommittedFile := range resultsWithSnappy {
		if strings.Contains(uncommittedFile, "mfi") {
			resultsWithoutSnappy = append(resultsWithoutSnappy, uncommittedFile)
		}
	}

	err = writeLines(resultsWithoutSnappy, filepath.Join(scriptDir, resultsPath))
	if err != nil {
		log.Fatalf("failed writing results: %s", err)
	}
}

func findUncommittedFiles() {
	flag.Parse()
	if len(bucketName) == 0 {
		flag.PrintDefaults()
		log.Fatal("invalid parameters, bucket name required")
	}

	loc, err := time.LoadLocation("America/Los_Angeles")
	if err != nil {
		log.Fatalf("failed to load location %v", err)
	}

	os.MkdirAll(scriptDir, os.ModePerm)

	loggingPath = "logs_find.txt"
	f, err := os.OpenFile(filepath.Join(scriptDir, loggingPath), os.O_WRONLY|os.O_CREATE, 0755)
	if err != nil {
		log.Fatalf("failed creating file: %v", err)
	}
	log.SetOutput(f)

	commitLogPath := fmt.Sprintf("%s_delta_log/", objectPrefix)
	objectPrefix = strings.TrimSuffix(objectPrefix, "/")

	cfg, err := config.LoadDefaultConfig(context.TODO())
	if err != nil {
		log.Fatalf("failed to load SDK configuration, %v", err)
	}

	client := s3.NewFromConfig(cfg)
	path := storage.NewPath(fmt.Sprintf("s3://%s/%s", bucketName, objectPrefix))

	store, err := s3store.New(client, storage.NewPath(fmt.Sprintf("s3://%s/%s", bucketName, objectPrefix)))
	if err != nil {
		log.Fatalf("failed to set up S3 store %v", err)
	}

	var committedParquetFiles []string

	listResults, _, err := ListAll(client, path, storage.NewPath(commitLogPath), "")
	if err != nil {
		log.Fatalf("failed to list all commit logs %v", err)
	}

	sort.Slice(listResults.Objects, func(i, j int) bool {
		return listResults.Objects[i].LastModified.Before(listResults.Objects[j].LastModified)
	})

	var commitLogs []storage.ObjectMeta

	for _, commitLog := range listResults.Objects {
		isCommit := delta.IsValidCommitUri(storage.NewPath(commitLog.Location.Raw))
		isTempCommit := strings.HasSuffix(commitLog.Location.Raw, ".json.tmp")

		if isCommit || isTempCommit {
			commitLogs = append(commitLogs, commitLog)
		}
	}

	numBatches := 1 + (len(commitLogs)-1)/batchSize
	actionsArray := make([][]delta.Action, numBatches)

	mu := &sync.RWMutex{}
	wg := &sync.WaitGroup{}
	for batchNum := 0; batchNum < numBatches; batchNum++ {
		wg.Add(1)
		go func(batchNum int) {
			for fileNum := 0; fileNum < min(batchSize, len(commitLogs)-batchNum*batchSize); fileNum++ {
				actionsArray[batchNum], err = delta.ReadCommitLog[FlatRecord, TestPartitionType](store, storage.NewPath(strings.TrimPrefix(commitLogs[batchNum*batchSize+fileNum].Location.Raw, objectPrefix)))
				if err != nil {
					log.WithField("key", commitLogs[batchNum*batchSize+fileNum].Location.Raw).Errorf("failed to read commit log %v", err)
					continue
				}

				for _, action := range actionsArray[batchNum] {
					mu.Lock()
					add, ok := action.(*delta.Add[FlatRecord, TestPartitionType])
					if ok {
						committedParquetFiles = append(committedParquetFiles, add.Path)
					}

					remove, ok := action.(*delta.Remove)
					if ok {
						committedParquetFiles = append(committedParquetFiles, remove.Path)
					}
					mu.Unlock()
				}

				log.WithFields(log.Fields{"file": commitLogs[batchNum*batchSize+fileNum].Location.Raw}).Infof("Processed file %d of %d in batch %d", fileNum+1, min(batchSize, len(commitLogs)-batchNum*batchSize), batchNum+1)
			}
			wg.Done()
		}(batchNum)
	}
	wg.Wait()

	committedParquetFiles = removeDuplicates(committedParquetFiles)

	committedParquetFilesByDate := make(map[string][]string)

	for _, committedParquetFile := range committedParquetFiles {
		r, _ := regexp.Compile("(date=.+?)/.+")
		date := r.FindStringSubmatch(committedParquetFile)[1]

		r, _ = regexp.Compile("date=.+?/(.+)")
		key := r.FindStringSubmatch(committedParquetFile)[1]

		committedParquetFilesByDate[date] = append(committedParquetFilesByDate[date], key)
	}

	dates := make([]string, len(committedParquetFilesByDate))

	for date := range committedParquetFilesByDate {
		dates = append(dates, date)
	}

	sort.Strings(dates)

	fmt.Println("Distribution of committed parquet files by date:")
	for _, date := range dates {
		fmt.Println(date, ": ", len(committedParquetFilesByDate[date]))
	}
	fmt.Println()

	_, commonPrefixes, err := ListAll(client, path, storage.NewPath(objectPrefix+"/date="), "/")
	if err != nil {
		log.Fatalf("failed to get common prefixes %v", err)
	}

	var datePartitions []string

	for _, commonPrefix := range commonPrefixes {
		date, ok := getStringInBetweenTwoString(*commonPrefix.Prefix, "date=", "/")
		if !ok {
			log.Fatal("failed to parse date")
		}
		datePartitions = append(datePartitions, date)
	}

	datePartitions = removeDuplicates(datePartitions)

	if len(minDate) > 0 {
		var temp []string

		for _, datePartition := range datePartitions {
			if datePartition >= minDate {
				temp = append(temp, datePartition)
			}
		}

		datePartitions = temp
	}

	if len(maxDate) > 0 {
		var temp []string

		for _, datePartition := range datePartitions {
			if datePartition <= maxDate {
				temp = append(temp, datePartition)
			}
		}

		datePartitions = temp
	}

	var lastCommitTimestamp time.Time

	actions, err := delta.ReadCommitLog[FlatRecord, TestPartitionType](store, storage.NewPath(strings.TrimPrefix(commitLogs[len(commitLogs)-1].Location.Raw, objectPrefix)))
	if err != nil {
		log.WithField("key", commitLogs[len(commitLogs)-1].Location.Raw).Errorf("failed to read commit log %v", err)
	}

	for _, action := range actions {
		commitInfo, ok := action.(*delta.CommitInfo)
		if ok {
			var ci CommitInfo

			marshaledCommitInfo, err := json.Marshal(commitInfo)
			if err != nil {
				log.Fatalf("failed to marshal commit info %v", err)
			}

			err = json.Unmarshal(marshaledCommitInfo, &ci)
			if err != nil {
				log.Fatalf("failed to unmarshal commit info %v", err)
			}

			lastCommitTimestamp = time.Unix(0, int64(ci.Timestamp)*int64(time.Millisecond)).In(loc)
		}
	}

	log.Infof("Last commit timestamp: %s", lastCommitTimestamp.String())

	listResults.Objects = nil
	datePartitionFilesArray := make([]storage.ListResult, len(datePartitions))

	mu2 := &sync.RWMutex{}
	wg2 := &sync.WaitGroup{}
	for datePartitionNum := range datePartitions {
		wg2.Add(1)
		go func(datePartitionNum int) {
			datePartitionFilesArray[datePartitionNum], _, err = ListAll(client, path, storage.NewPath(fmt.Sprintf("%s/date=%s/", objectPrefix, datePartitions[datePartitionNum])), "")
			if err != nil {
				log.Fatalf("failed to list all %s partition files %v", datePartitions[datePartitionNum], err)
			}

			mu2.Lock()
			listResults.Objects = append(listResults.Objects, datePartitionFilesArray[datePartitionNum].Objects...)
			mu2.Unlock()

			log.WithFields(log.Fields{"date partition": datePartitions[datePartitionNum]}).Infof("Processed date partition %d of %d", datePartitionNum+1, len(datePartitions))
			wg2.Done()
		}(datePartitionNum)
	}
	wg2.Wait()

	var allParquetFiles []string

	for _, listResult := range listResults.Objects {
		if (strings.HasSuffix(listResult.Location.Raw, ".parquet") || strings.HasSuffix(listResult.Location.Raw, ".parquet.zstd")) && listResult.LastModified.In(loc).Before(lastCommitTimestamp.Add(-time.Minute*commitWaitMinutes)) {
			allParquetFiles = append(allParquetFiles, strings.TrimPrefix(listResult.Location.Raw, objectPrefix+"/"))
		}
	}

	allParquetFilesByDate := make(map[string][]string)

	for _, parquetFile := range allParquetFiles {
		r, _ := regexp.Compile("(date=.+?)/.+")
		date := r.FindStringSubmatch(parquetFile)[1]

		r, _ = regexp.Compile("date=.+?/(.+)")
		key := r.FindStringSubmatch(parquetFile)[1]

		allParquetFilesByDate[date] = append(allParquetFilesByDate[date], key)
	}

	dates = make([]string, len(allParquetFilesByDate))

	for date := range allParquetFilesByDate {
		dates = append(dates, date)
	}

	sort.Strings(dates)

	fmt.Println("Distribution of parquet files within specified date range by date:")
	for _, date := range dates {
		fmt.Println(date, ": ", len(allParquetFilesByDate[date]))
	}
	fmt.Println()

	fmt.Printf("Number of parquet files within specified date range: %d\n", len(allParquetFiles))

	committedParquetFiles = removeDuplicates(committedParquetFiles)
	fmt.Printf("Number of committed parquet files: %d\n", len(committedParquetFiles))

	uncommittedParquetFiles := findDifference(allParquetFiles, committedParquetFiles)
	fmt.Printf("Number of uncommitted parquet files: %d\n", len(uncommittedParquetFiles))

	err = writeLines(uncommittedParquetFiles, filepath.Join(scriptDir, resultsPath))
	if err != nil {
		log.Fatalf("failed writing results: %s", err)
	}
}

func removeDuplicates(strSlice []string) []string {
	allKeys := make(map[string]bool)
	list := []string{}

	for _, item := range strSlice {
		if _, value := allKeys[item]; !value {
			allKeys[item] = true
			list = append(list, item)
		}
	}

	return list
}

func findDifference(a, b []string) []string {
	mb := make(map[string]struct{}, len(b))

	for _, x := range b {
		mb[x] = struct{}{}
	}

	var diff []string

	for _, x := range a {
		if _, found := mb[x]; !found {
			diff = append(diff, x)
		}
	}

	return diff
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

func writeLines(lines []string, path string) error {
	file, err := os.Create(path)
	if err != nil {
		return err
	}
	defer file.Close()

	w := bufio.NewWriter(file)
	for _, line := range lines {
		fmt.Fprintln(w, line)
	}
	return w.Flush()
}

func getStringInBetweenTwoString(str string, startS string, endS string) (result string, found bool) {
	s := strings.Index(str, startS)
	if s == -1 {
		return result, false
	}
	newS := str[s+len(startS):]
	e := strings.Index(newS, endS)
	if e == -1 {
		return result, false
	}
	result = newS[:e]
	return result, true
}

func ListAll(client *s3.Client, path *storage.Path, prefix *storage.Path, delimiter string) (storage.ListResult, []types.CommonPrefix, error) {
	var listResult storage.ListResult
	// We will need the store path with the leading and trailing /'s for trimming results
	pathWithSeparators := path.Raw
	if !strings.HasPrefix(pathWithSeparators, "/") {
		pathWithSeparators = "/" + pathWithSeparators
	}
	if !strings.HasSuffix(pathWithSeparators, "/") {
		pathWithSeparators = pathWithSeparators + "/"
	}

	bucketName, ok := getStringInBetweenTwoString(pathWithSeparators, "s3://", "/")
	if !ok {
		log.Fatalf("failed to parse date")
	}
	params := &s3.ListObjectsV2Input{
		Bucket: &bucketName,
	}
	if len(delimiter) != 0 {
		params.Delimiter = &delimiter
	}
	if len(prefix.Raw) != 0 {
		params.Prefix = &prefix.Raw
	}

	p := s3.NewListObjectsV2Paginator(client, params)
	var commonPrefixes []types.CommonPrefix

	var i int
	for p.HasMorePages() {
		i++

		page, err := p.NextPage(context.TODO())
		if err != nil {
			log.Fatalf("failed to get page %v, %v", i, err)
		}

		for _, obj := range page.Contents {
			location := strings.TrimPrefix(*obj.Key, pathWithSeparators)
			listResult.Objects = append(listResult.Objects, storage.ObjectMeta{
				Location:     *storage.NewPath(location),
				LastModified: *obj.LastModified,
				Size:         obj.Size,
			})
		}
		commonPrefixes = append(commonPrefixes, page.CommonPrefixes...)
	}

	return listResult, commonPrefixes, nil
}

func min[T constraints.Ordered](a, b T) T {
	if a < b {
		return a
	}
	return b
}
