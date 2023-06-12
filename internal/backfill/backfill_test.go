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
	"context"
	"fmt"
	"testing"

	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/google/go-cmp/cmp"
	"github.com/rivian/delta-go"
	"github.com/rivian/delta-go/storage"
	"github.com/rivian/delta-go/storage/s3store"
)

func TestCompareLogEntries(t *testing.T) {
	bucketName = "vehicle-telemetry-fleet-prod"
	objectPrefix = "tables/v1/vehicle_fleet_delta_go_clone/"
	loggingPath = "logs_test.txt"
	writeLogEntries = false

	cfg, err := config.LoadDefaultConfig(context.TODO())
	if err != nil {
		t.Fatalf("failed to load SDK configuration %v", err)
	}

	client := s3.NewFromConfig(cfg)

	store, err := s3store.New(client, storage.NewPath(fmt.Sprintf("s3://%s/%s", bucketName, objectPrefix)))
	if err != nil {
		t.Fatalf("failed to set up S3 store %v", err)
	}

	remoteLogEntry, err := store.Get(storage.NewPath("_delta_log/00000000000000124307.json"))
	if err != nil {
		t.Fatalf("failed to get remote log entry %v", err)
	}

	remoteActions, err := delta.ActionsFromLogEntries[FlatRecord, TestPartitionType](remoteLogEntry)
	if err != nil {
		t.Fatalf("failed to get remote actions %v", err)
	}

	paths, remoteActionsWithPaths := GetPathsFromActions(remoteActions)

	localLogEntry := CreateLogEntries(paths)[0]

	localActions, err := delta.ActionsFromLogEntries[FlatRecord, TestPartitionType](localLogEntry)
	if err != nil {
		t.Fatalf("failed to get local actions %v", err)
	}

	if len(remoteActionsWithPaths) != len(localActions) {
		t.Fatalf("remote actions length %d does not match local actions length %d %v", len(remoteActions), len(localActions), err)
	}

	for actionNum := range remoteActionsWithPaths {
		remoteAdd, _ := remoteActions[actionNum].(*delta.Add[FlatRecord, TestPartitionType])
		localAdd, _ := localActions[actionNum].(*delta.Add[FlatRecord, TestPartitionType])

		// Due to timestamp differences, modification time and stats involving timestamps are slightly off between remote and local actions, so they are not compared
		if remoteAdd.Path != localAdd.Path {
			t.Fatalf("%d: remote action path %s does not match local action path %s %v", actionNum, remoteAdd.Path, localAdd.Path, err)
		}

		if !cmp.Equal(remoteAdd.PartitionValues, localAdd.PartitionValues) {
			t.Fatalf("%d: remote action partition values %v does not match local action partition values %v %v", actionNum, remoteAdd.PartitionValues, localAdd.PartitionValues, err)
		}

		if remoteAdd.Size != localAdd.Size {
			t.Fatalf("%d: remote action size %d does not match local action size %d %v", actionNum, remoteAdd.Size, localAdd.Size, err)
		}

		if remoteAdd.DataChange != localAdd.DataChange {
			t.Fatalf("%d: remote action data change %t does not match local action data change %t %v", actionNum, remoteAdd.DataChange, localAdd.DataChange, err)
		}

		if !cmp.Equal(remoteAdd.Tags, localAdd.Tags) {
			t.Fatalf("%d: remote action tags %v does not match local action tags %v %v", actionNum, remoteAdd.Tags, localAdd.Tags, err)
		}

		remoteStats, err := delta.StatsFromJson([]byte(remoteAdd.Stats))
		if err != nil {
			t.Fatalf("%d: failed to get remote stats from JSON %v", actionNum, err)
		}

		localStats, err := delta.StatsFromJson([]byte(localAdd.Stats))
		if err != nil {
			t.Fatalf("%d: failed to get local stats from JSON %v", actionNum, err)
		}

		if remoteStats.NumRecords != localStats.NumRecords {
			t.Fatalf("%d: remote stats num records %d does not match local stats num records %d %v", actionNum, remoteStats.NumRecords, localStats.NumRecords, err)
		}

		if remoteStats.TightBounds != localStats.TightBounds {
			t.Fatalf("%d: remote stats tight bounds %t does not match local stats tight bounds %t %v", actionNum, remoteStats.TightBounds, localStats.TightBounds, err)
		}

		if !cmp.Equal(remoteStats.NullCount, localStats.NullCount) {
			t.Fatalf("%d: remote stats null count %v does not match local stats null count %v %v", actionNum, remoteStats.NullCount, localStats.NullCount, err)
		}

		if !cmp.Equal(remoteAdd.PartitionValuesParsed, localAdd.PartitionValuesParsed) {
			t.Fatalf("%d: remote action partition values parsed %v does not match local action partition values parsed %v %v", actionNum, remoteAdd.PartitionValuesParsed, localAdd.PartitionValuesParsed, err)
		}

		if !cmp.Equal(remoteAdd.StatsParsed, localAdd.StatsParsed) {
			t.Fatalf("%d: remote action stats parsed %v does not match local action stats parsed %v %v", actionNum, remoteAdd.StatsParsed, localAdd.StatsParsed, err)
		}
	}
}
