package surfstore

import (
	context "context"
	"fmt"
	"log"
	"math"
	"sync"
	"time"

	"google.golang.org/grpc"
	emptypb "google.golang.org/protobuf/types/known/emptypb"
)

// TODO Add fields you need here
type RaftSurfstore struct {
	isLeader      bool
	isLeaderMutex *sync.RWMutex
	term          int64
	log           []*UpdateOperation
	///////
	commitIndex int64 //highest Commit
	lastApplied int64 //highest apply to machine
	//nextIndex   int64
	//matchIndex  int64
	Ip_list        []string
	ServerId       int64
	pendingCommits []chan bool
	/////
	metaStore *MetaStore

	/*--------------- Chaos Monkey --------------*/
	isCrashed      bool
	isCrashedMutex *sync.RWMutex
	UnimplementedRaftSurfstoreServer
}

func (s *RaftSurfstore) GetFileInfoMap(ctx context.Context, empty *emptypb.Empty) (*FileInfoMap, error) {
	s.isCrashedMutex.Lock()
	is_crash := s.isCrashed
	s.isCrashedMutex.Unlock()
	succ, _ := s.SendHeartbeat(ctx, empty)
	log.Println("Get Map crash?", is_crash, s.ServerId, succ.Flag)
	if is_crash {
		return nil, ERR_SERVER_CRASHED
	}
	s.isLeaderMutex.RLock()
	is_leader := s.isLeader
	s.isLeaderMutex.RUnlock()
	if !is_leader {
		return nil, ERR_NOT_LEADER
	}
	log.Println("Get Map!!!!!!!!!!!!!!", is_leader, s.ServerId)
	meta, _ := s.metaStore.GetFileInfoMap(ctx, empty)
	if succ.Flag {
		return meta, nil
	} else {
		return meta, fmt.Errorf("Majority Crash!! Cannot process for client.")
	}
}

func (s *RaftSurfstore) GetBlockStoreMap(ctx context.Context, hashes *BlockHashes) (*BlockStoreMap, error) {
	//log.Println("GET MAP is HERE000000000!!!!!")
	s.isCrashedMutex.Lock()
	is_crash := s.isCrashed
	s.isCrashedMutex.Unlock()
	if is_crash {
		return nil, ERR_SERVER_CRASHED
	}
	s.isLeaderMutex.RLock()
	is_leader := s.isLeader
	s.isLeaderMutex.RUnlock()
	if !is_leader {
		return nil, ERR_NOT_LEADER
	}
	log.Println("GET MAP is HERE!!!!!")
	for { //block
		succ, _ := s.SendHeartbeat(ctx, &emptypb.Empty{})
		break
		if succ.Flag == true {
			break
		}
	}
	return s.metaStore.GetBlockStoreMap(ctx, hashes)
}

func (s *RaftSurfstore) GetBlockStoreAddrs(ctx context.Context, empty *emptypb.Empty) (*BlockStoreAddrs, error) {
	s.isCrashedMutex.Lock()
	is_crash := s.isCrashed
	s.isCrashedMutex.Unlock()
	if is_crash {
		return nil, ERR_SERVER_CRASHED
	}
	s.isLeaderMutex.RLock()
	is_leader := s.isLeader
	s.isLeaderMutex.RUnlock()
	log.Println("LLLLeader", is_leader, s.ServerId)
	if !is_leader {
		return nil, ERR_NOT_LEADER
	}
	for { //block
		succ, _ := s.SendHeartbeat(ctx, empty)
		break
		if succ.Flag == true {
			break
		}
	}
	log.Println("Get Store ADDR!!!!!!")
	return s.metaStore.GetBlockStoreAddrs(ctx, empty)
}
func (s *RaftSurfstore) UpdateFile(ctx context.Context, filemeta *FileMetaData) (*Version, error) {
	log.Println("UUUUUUUUPdate try!!")
	s.isCrashedMutex.RLock()
	isCrashed := s.isCrashed
	s.isCrashedMutex.RUnlock()
	if isCrashed {
		return nil, ERR_SERVER_CRASHED
	}
	s.isLeaderMutex.RLock()
	isLeader := s.isLeader
	s.isLeaderMutex.RUnlock()
	if !isLeader {
		return nil, ERR_NOT_LEADER
	}
	//1 append
	s.log = append(s.log, &UpdateOperation{
		Term:         s.term,
		FileMetaData: filemeta,
	})
	//log.Println("Log for", s.ServerId, "Is appended!", s.log)
	commitChan := make(chan bool)
	s.pendingCommits = append(s.pendingCommits, commitChan)
	for { //block
		succ, _ := s.SendHeartbeat(ctx, &emptypb.Empty{})
		if succ.Flag == true {
			break
		}
		time.Sleep(1000 * time.Millisecond)
	}
	//2 send
	go s.sendparallel()
	//3 commit
	commit := <-commitChan
	if commit { // APply for state machine
		s.lastApplied = s.commitIndex
		log.Println("Ready to upload!!!!!", filemeta)
		return s.metaStore.UpdateFile(ctx, filemeta)
	}

	return nil, ERR_SERVER_CRASHED
}
func (s *RaftSurfstore) sendparallel() {
	responses := make(chan bool, len(s.Ip_list)-1)
	//send to all
	for idx, addr := range s.Ip_list {
		if int64(idx) == s.ServerId {
			continue
		}
		go s.sendtofollower(addr, responses)
	}
	totalAppends := 1
	pendidx := int64(len(s.pendingCommits) - 1)
	//wait for response
	for {
		result := <-responses
		s.isCrashedMutex.RLock()
		is_Crashed := s.isCrashed
		s.isCrashedMutex.RUnlock()
		if is_Crashed {
			s.pendingCommits[pendidx] <- false
			break
		}
		if result {
			totalAppends++
		}
		if totalAppends > len(s.Ip_list)/2 {
			s.pendingCommits[pendidx] <- true
			s.commitIndex++
			break
		}
	}
	log.Println("LLLLLLLLLLLLLLLL", s.ServerId, totalAppends)

}
func (s *RaftSurfstore) sendtofollower(addr string, responses chan bool) {
	s.isCrashedMutex.RLock()
	isCrashed := s.isCrashed
	s.isCrashedMutex.RUnlock()
	if isCrashed {
		responses <- false
	}
	conn, _ := grpc.Dial(addr, grpc.WithInsecure())
	client := NewRaftSurfstoreClient(conn)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	var prevLogTerm int64
	if s.commitIndex == -1 {
		prevLogTerm = 0
	} else {
		prevLogTerm = s.log[s.commitIndex].Term
	}
	input := &AppendEntryInput{
		Term:         s.term,
		PrevLogIndex: int64(s.commitIndex),
		PrevLogTerm:  int64(prevLogTerm),
		Entries:      s.log[:s.commitIndex+2],
		LeaderCommit: s.commitIndex,
	}
	output, _ := client.AppendEntries(ctx, input)
	log.Println("UUPload File has result", output)
	if output != nil && output.Success {
		responses <- true
	} else {
		responses <- false
	}
	return
}

// 1. Reply false if term < currentTerm (§5.1)
// 2. Reply false if log doesn’t contain an entry at prevLogIndex whose term
// matches prevLogTerm (§5.3)
// 3. If an existing entry conflicts with a new one (same index but different
// terms), delete the existing entry and all that follow it (§5.3)
// 4. Append any new entries not already in the log
// 5. If leaderCommit > commitIndex, set commitIndex = min(leaderCommit, index
// of last new entry)
func (s *RaftSurfstore) AppendEntries(ctx context.Context, input *AppendEntryInput) (*AppendEntryOutput, error) {
	//                       Input:      Term     PrevLogIndex   PrevLogTerm Entries LeaderCommit
	//log.Println("Append", input, "From", s.ServerId)
	out := &AppendEntryOutput{
		Success:      false,
		MatchedIndex: -110, //?????
	}
	s.isCrashedMutex.Lock()
	is_crash := s.isCrashed
	s.isCrashedMutex.Unlock()
	if is_crash {
		return out, ERR_SERVER_CRASHED
	}
	//log.Println("IIINput for append:", input.Term, input.PrevLogIndex, input.PrevLogTerm, input.Entries, input.LeaderCommit)
	in_term := input.Term //Leader term
	in_entry := input.Entries
	in_l_comit := input.LeaderCommit //leader commit index
	if s.term < in_term {
		log.Println("I am:", s.ServerId, " have old term:", s.term, "New term", in_term)
		s.term = in_term
		s.isLeaderMutex.Lock()
		s.isLeader = false
		s.isLeaderMutex.Unlock()
	}
	if s.term > in_term { // 1111111111
		log.Println("ERR! I have higher term than input!")
		return out, nil
	}
	//log.Println("I SSSLOG2", s.log)
	var ind int = -1
	for _, Log := range s.log { //3
		ind++
		//log.Println(Log, in_entry[ind], Log == in_entry[ind])
		if Log.Term != in_entry[ind].Term {
			log.Println("Term not match!!!!!", Log.Term, in_entry[ind].Term)
			s.log = s.log[:ind]
			break
		}
	}
	if len(in_entry) > 0 {
		in_entry = in_entry[ind+1:]
	}
	s.log = append(s.log, in_entry...) //4
	//log.Println("I SSSLOG3", s.log, in_entry, len(in_entry), ind)
	//s.log = append(s.log, in_entry...) //4
	if in_l_comit > s.commitIndex { //5
		log.Println("I have old commit index!", s.commitIndex, in_l_comit)
		s.commitIndex = int64(math.Min(float64(in_l_comit), float64(len(s.log)-1)))
		log.Println("But change to!", s.commitIndex)
		//????
	}
	log.Println("I am a server", s.ServerId, "Append result:", s.ServerId, s.lastApplied, s.commitIndex)
	for s.lastApplied < s.commitIndex {
		entry := s.log[s.lastApplied+1]
		s.metaStore.UpdateFile(ctx, entry.FileMetaData)
		s.lastApplied++
	}
	out.Success = true
	return out, nil
}

func (s *RaftSurfstore) SetLeader(ctx context.Context, _ *emptypb.Empty) (*Success, error) {
	log.Println("LLLLLEEAAADDDEERRRRRRRRRRRR is ", s.ServerId)
	s.isCrashedMutex.Lock()
	is_crash := s.isCrashed
	s.isCrashedMutex.Unlock()
	if is_crash {
		return &Success{Flag: false}, ERR_SERVER_CRASHED
	}
	s.isLeaderMutex.Lock()
	s.isLeader = true
	s.isLeaderMutex.Unlock()
	s.term += 1
	time.Sleep(500 * time.Millisecond)
	return &Success{Flag: true}, nil
}

func (s *RaftSurfstore) SendHeartbeat(ctx context.Context, _ *emptypb.Empty) (*Success, error) {
	log.Println(s.ServerId, "Send Heart beat:")
	s.isCrashedMutex.Lock()
	is_crash := s.isCrashed
	s.isCrashedMutex.Unlock()
	if is_crash {
		return &Success{Flag: false}, ERR_SERVER_CRASHED
	}
	s.isLeaderMutex.RLock()
	is_leader := s.isLeader
	s.isLeaderMutex.RUnlock()
	if !is_leader {
		return &Success{Flag: false}, ERR_NOT_LEADER
	}
	ip_len := len(s.Ip_list)
	var counter int = 0
	var flag bool = false
	for idx, ip := range s.Ip_list {
		if int64(idx) == s.ServerId { //skip current IP
			continue
		}
		conn, err := grpc.Dial(ip, grpc.WithInsecure())
		if err != nil {
			return &Success{Flag: false}, nil
		}
		defer conn.Close()
		c := NewRaftSurfstoreClient(conn)

		// perform the call
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		var prev_term int64
		if s.commitIndex < 0 {
			//log.Println("Current commitIndex is:", s.commitIndex)
			prev_term = 0
		} else {
			prev_term = s.log[s.commitIndex].Term
		}
		input := &AppendEntryInput{
			Term:         s.term,
			PrevLogIndex: s.commitIndex,
			PrevLogTerm:  prev_term,
			Entries:      s.log,
			LeaderCommit: s.commitIndex,
		}
		//log.Println("Send Heartbeat!")
		try, err := c.AppendEntries(ctx, input)
		//log.Println("Append in Heartbeat has result:::", s.log, s.commitIndex, s.lastApplied)
		if try != nil && try.Success {
			counter += 1
		}
	}
	if counter >= ip_len/2 {
		flag = true
	}
	return &Success{Flag: flag}, nil

}

// ========== DO NOT MODIFY BELOW THIS LINE =====================================

func (s *RaftSurfstore) Crash(ctx context.Context, _ *emptypb.Empty) (*Success, error) {
	log.Print("Crash happen in server:", s.ServerId)
	s.isCrashedMutex.Lock()
	s.isCrashed = true
	s.isCrashedMutex.Unlock()

	return &Success{Flag: true}, nil
}

func (s *RaftSurfstore) Restore(ctx context.Context, _ *emptypb.Empty) (*Success, error) {
	log.Print("Restore happen in server:", s.ServerId)
	s.isCrashedMutex.Lock()
	s.isCrashed = false
	s.isCrashedMutex.Unlock()

	return &Success{Flag: true}, nil
}

func (s *RaftSurfstore) GetInternalState(ctx context.Context, empty *emptypb.Empty) (*RaftInternalState, error) {
	fileInfoMap, _ := s.metaStore.GetFileInfoMap(ctx, empty)
	log.Println("I am", s.ServerId, " STATE!!!!!", "log is", s.log)
	s.isLeaderMutex.RLock()
	state := &RaftInternalState{
		IsLeader: s.isLeader,
		Term:     s.term,
		Log:      s.log,
		MetaMap:  fileInfoMap,
	}
	s.isLeaderMutex.RUnlock()

	return state, nil
}

var _ RaftSurfstoreInterface = new(RaftSurfstore)
