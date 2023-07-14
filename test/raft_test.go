package SurfTest

import (
	"cse224/proj5/pkg/surfstore"
	"fmt"

	emptypb "google.golang.org/protobuf/types/known/emptypb"
	"time"
	//"os"
	"testing"
)

func TestRaftSetLeader(t *testing.T) {
	//Setup
	cfgPath := "./config_files/3nodes.txt"
	test := InitTest(cfgPath)
	defer EndTest(test)

	// TEST
	leaderIdx := 0
	test.Clients[leaderIdx].SetLeader(test.Context, &emptypb.Empty{})

	// heartbeat
	for _, server := range test.Clients {
		server.SendHeartbeat(test.Context, &emptypb.Empty{})
	}
	for idx, server := range test.Clients {
		// all should have the leaders term
		state, _ := server.GetInternalState(test.Context, &emptypb.Empty{})
		//fmt.Println(idx, state.Term, state.Log, state.IsLeader)
		if state == nil {
			t.Fatalf("Could not get state")
		}
		if state.Term != int64(1) {
			t.Logf("SSServer %d should be in term %d, but in %d", idx, 1, state.Term)
			t.Fatalf("Server %d should be in term %d", idx, 1)
		}
		if idx == leaderIdx {
			// server should be the leader
			if !state.IsLeader {
				t.Fatalf("Server %d should be the leader", idx)
			}
		} else {
			// server should not be the leader
			if state.IsLeader {
				t.Fatalf("Server %d should not be the leader", idx)
			}
		}
	}

	leaderIdx = 2
	test.Clients[leaderIdx].SetLeader(test.Context, &emptypb.Empty{})

	// heartbeat
	for _, server := range test.Clients {
		server.SendHeartbeat(test.Context, &emptypb.Empty{})
	}

	for idx, server := range test.Clients {
		// all should have the leaders term
		state, _ := server.GetInternalState(test.Context, &emptypb.Empty{})
		if state == nil {
			t.Fatalf("Could not get state")
		}
		if state.Term != int64(2) {
			t.Fatalf("Server should be in term %d", 2)
		}
		if idx == leaderIdx {
			// server should be the leader
			if !state.IsLeader {
				t.Fatalf("Server %d should be the leader", idx)
			}
		} else {
			// server should not be the leader
			if state.IsLeader {
				t.Fatalf("Server %d should not be the leader", idx)
			}
		}
	}
}
func TestRaftFollowersGetUpdates(t *testing.T) {
	//Setup
	cfgPath := "./config_files/3nodes.txt"
	test := InitTest(cfgPath)
	defer EndTest(test)

	// TEST
	leaderIdx := 0
	test.Clients[leaderIdx].SetLeader(test.Context, &emptypb.Empty{})
	test.Clients[leaderIdx].SendHeartbeat(test.Context, &emptypb.Empty{})
	filemeta := &surfstore.FileMetaData{
		Filename:      "testFile1",
		Version:       1,
		BlockHashList: nil,
	}
	test.Clients[leaderIdx].UpdateFile(test.Context, filemeta)
	//fmt.Println("TEST get upload result", v, err)
	test.Clients[leaderIdx].SendHeartbeat(test.Context, &emptypb.Empty{})
	test.Clients[leaderIdx].SendHeartbeat(test.Context, &emptypb.Empty{})

	goldenMeta := make(map[string]*surfstore.FileMetaData)
	goldenMeta[filemeta.Filename] = filemeta
	//surfstore.NewMetaStore("")
	//goldenMeta.UpdateFile(test.Context, filemeta1)
	goldenLog := make([]*surfstore.UpdateOperation, 0)
	goldenLog = append(goldenLog, &surfstore.UpdateOperation{
		Term:         1,
		FileMetaData: filemeta,
	})
	term := int64(1)
	var leader bool
	var maps map[string]*surfstore.FileMetaData = goldenMeta
	var log []*surfstore.UpdateOperation = goldenLog
	for idx, server := range test.Clients {
		if idx == leaderIdx {
			leader = bool(true)
		} else {
			leader = bool(false)
		}
		res, err := CheckInternalState(&leader, &term, log, maps, server, test.Context)
		if err != nil {

			t.Fatalf("err check state for server %d", idx)
		}
		if !res {
			t.Fatalf("ERR in check! %d", idx)
		}
		//fmt.Println(state, leader, term, log, maps)
	}
	fmt.Println("+++++++++++++")
}

func TestMy2(t *testing.T) {
	t.Logf("change leader")
	cfgPath := "./config_files/3nodes.txt"
	test := InitTest(cfgPath)
	defer EndTest(test)
	test.Clients[0].SetLeader(test.Context, &emptypb.Empty{})
	test.Clients[0].SendHeartbeat(test.Context, &emptypb.Empty{})
	test.Clients[1].Crash(test.Context, &emptypb.Empty{})
	test.Clients[2].Crash(test.Context, &emptypb.Empty{})
	filemeta := &surfstore.FileMetaData{
		Filename:      "testFile1",
		Version:       1,
		BlockHashList: nil,
	}
	go test.Clients[0].UpdateFile(test.Context, filemeta)

	//test.Clients[1].Restore(test.Context, &emptypb.Empty{})
	//test.Clients[2].Restore(test.Context, &emptypb.Empty{})
	test.Clients[0].SendHeartbeat(test.Context, &emptypb.Empty{})
	test.Clients[0].SendHeartbeat(test.Context, &emptypb.Empty{})
	test.Clients[0].SendHeartbeat(test.Context, &emptypb.Empty{})
	goldenMeta := make(map[string]*surfstore.FileMetaData)
	goldenMeta[filemeta.Filename] = filemeta
	goldenLog := make([]*surfstore.UpdateOperation, 0)
	goldenLog = append(goldenLog, &surfstore.UpdateOperation{
		Term:         1,
		FileMetaData: filemeta,
	})

	term := int64(1)
	var leader bool
	var maps map[string]*surfstore.FileMetaData = goldenMeta
	var log []*surfstore.UpdateOperation = goldenLog
	for idx, server := range test.Clients {
		if idx == 0 {
			leader = bool(true)
		} else {
			leader = bool(false)
		}
		res, err := CheckInternalState(&leader, &term, log, maps, server, test.Context)
		if err != nil {
			fmt.Println(err)
			t.Fatalf("err checking state for server %d", idx)
		}
		if !res {
			t.Fatalf("ERR in check! %d", idx)
		}
	}

}

func TestMy3(t *testing.T) {
	t.Logf("Most crash")
	cfgPath := "./config_files/3nodes.txt"
	test := InitTest(cfgPath)
	defer EndTest(test)
	test.Clients[0].SetLeader(test.Context, &emptypb.Empty{})
	test.Clients[0].SendHeartbeat(test.Context, &emptypb.Empty{})
	test.Clients[1].Crash(test.Context, &emptypb.Empty{})
	test.Clients[2].Crash(test.Context, &emptypb.Empty{})
	filemeta := &surfstore.FileMetaData{
		Filename:      "testFile1",
		Version:       1,
		BlockHashList: nil,
	}
	go test.Clients[0].UpdateFile(test.Context, filemeta)
	t.Logf("RRRRRRREEEESSSSSTTTOORRE!!!!")
	test.Clients[1].Restore(test.Context, &emptypb.Empty{})
	test.Clients[2].Restore(test.Context, &emptypb.Empty{})
	test.Clients[0].SendHeartbeat(test.Context, &emptypb.Empty{})
	test.Clients[0].SendHeartbeat(test.Context, &emptypb.Empty{})
	test.Clients[0].SendHeartbeat(test.Context, &emptypb.Empty{})
	goldenMeta := make(map[string]*surfstore.FileMetaData)
	goldenMeta[filemeta.Filename] = filemeta
	goldenLog := make([]*surfstore.UpdateOperation, 0)
	goldenLog = append(goldenLog, &surfstore.UpdateOperation{
		Term:         1,
		FileMetaData: filemeta,
	})

	term := int64(1)
	var leader bool
	var maps map[string]*surfstore.FileMetaData = goldenMeta
	var log []*surfstore.UpdateOperation = goldenLog
	for idx, server := range test.Clients {
		if idx == 0 {
			leader = bool(true)
		} else {
			leader = bool(false)
		}
		res, err := CheckInternalState(&leader, &term, log, maps, server, test.Context)
		if err != nil {
			fmt.Println(err)
			t.Fatalf("err checking state for server %d", idx)
		}
		if !res {
			t.Fatalf("ERR in check! %d", idx)
		}
	}

}

func TestMy4(t *testing.T) {
	// Most crash, restore
	cfgPath := "./config_files/3nodes.txt"
	test := InitTest(cfgPath)
	defer EndTest(test)
	test.Clients[0].SetLeader(test.Context, &emptypb.Empty{})
	test.Clients[0].SendHeartbeat(test.Context, &emptypb.Empty{})
	worker1 := InitDirectoryWorker("test0", SRC_PATH)
	worker2 := InitDirectoryWorker("test1", SRC_PATH)
	defer worker1.CleanUp()
	defer worker2.CleanUp()
	//clients add different files
	file1 := "multi_file1.txt"
	err := worker1.AddFile(file1)
	if err != nil {
		t.FailNow()
	}
	//client1 syncs
	t.Logf("NNNNNNN0000000000000000000000")
	err = SyncClient("localhost:8080", "test0", BLOCK_SIZE, cfgPath)
	if err != nil {
		t.Fatalf("Sync failed")
	}
	t.Logf("NNNNNNN1111111111111111111111")
	test.Clients[0].SendHeartbeat(test.Context, &emptypb.Empty{})
	//client2 syncs
	err = SyncClient("localhost:8080", "test1", BLOCK_SIZE, cfgPath)
	if err != nil {
		t.Fatalf("Sync failed")
	}
	t.Logf("NNNNNNN2222222222222222222222222222")
	test.Clients[1].Crash(test.Context, &emptypb.Empty{})
	test.Clients[2].Crash(test.Context, &emptypb.Empty{})

	//client1 syncs
	err = SyncClient("localhost:8080", "test1", BLOCK_SIZE, cfgPath)
	if err != nil {
		t.Fatalf("Sync failed")
	}

}

func TestMy5(t *testing.T) {
	// Most crash, restore change leader
	t.Logf("Most crash")
	cfgPath := "./config_files/3nodes.txt"
	test := InitTest(cfgPath)
	defer EndTest(test)
	test.Clients[0].SetLeader(test.Context, &emptypb.Empty{})
	test.Clients[0].SendHeartbeat(test.Context, &emptypb.Empty{})
	test.Clients[1].Crash(test.Context, &emptypb.Empty{})
	test.Clients[2].Crash(test.Context, &emptypb.Empty{})
	filemeta := &surfstore.FileMetaData{
		Filename:      "testFile1",
		Version:       1,
		BlockHashList: nil,
	}
	filemeta2 := &surfstore.FileMetaData{
		Filename:      "testFile2",
		Version:       1,
		BlockHashList: nil,
	}
	filemeta3 := &surfstore.FileMetaData{
		Filename:      "testFile3",
		Version:       1,
		BlockHashList: nil,
	}
	go test.Clients[0].UpdateFile(test.Context, filemeta)
	//test.Clients[0].SendHeartbeat(test.Context, &emptypb.Empty{})
	go test.Clients[0].UpdateFile(test.Context, filemeta2)
	t.Logf("RRRRRRREEEESSSSSTTTOORRE!!!!")
	time.Sleep(1001 * time.Millisecond)
	//test.Clients[0].SendHeartbeat(test.Context, &emptypb.Empty{})
	term := int64(1)
	var leader bool
	var maps map[string]*surfstore.FileMetaData
	var log []*surfstore.UpdateOperation
	for idx, server := range test.Clients {
		if idx == 0 {
			res, err := CheckInternalState(&leader, &term, log, maps, server, test.Context)
			fmt.Println("Check for", idx, "Here", res, err, leader, term, log, maps, server)
		}
	}
	test.Clients[0].Crash(test.Context, &emptypb.Empty{})
	fmt.Println("CRash")
	test.Clients[1].Restore(test.Context, &emptypb.Empty{})
	test.Clients[2].Restore(test.Context, &emptypb.Empty{})

	test.Clients[1].SetLeader(test.Context, &emptypb.Empty{})
	fmt.Println("SSEETTT")
	//test.Clients[0].SendHeartbeat(test.Context, &emptypb.Empty{})
	//test.Clients[0].SendHeartbeat(test.Context, &emptypb.Empty{})
	test.Clients[1].SendHeartbeat(test.Context, &emptypb.Empty{})
	go test.Clients[1].UpdateFile(test.Context, filemeta3)
	for idx, server := range test.Clients {
		res, err := CheckInternalState(&leader, &term, log, maps, server, test.Context)
		fmt.Println("Check for", idx, "Here", res, err, leader, term, log, maps, server)
	}
}
