package surfstore

import (
	"bufio"
	"encoding/json"
	"io"
	"log"
	"net"
	"os"
	"sync"

	"google.golang.org/grpc"
)

type RaftConfig struct {
	RaftAddrs  []string
	BlockAddrs []string
}

func LoadRaftConfigFile(filename string) (cfg RaftConfig) {
	configFD, e := os.Open(filename)
	if e != nil {
		log.Fatal("Error Open config file:", e)
	}
	defer configFD.Close()

	configReader := bufio.NewReader(configFD)
	decoder := json.NewDecoder(configReader)

	if err := decoder.Decode(&cfg); err == io.EOF {
		return
	} else if err != nil {
		log.Fatal(err)
	}
	return
}

func NewRaftServer(id int64, config RaftConfig) (*RaftSurfstore, error) {
	// TODO Any initialization you need here

	isLeaderMutex := sync.RWMutex{}
	isCrashedMutex := sync.RWMutex{}

	server := RaftSurfstore{
		isLeader:      false,
		isLeaderMutex: &isLeaderMutex,
		term:          0,
		metaStore:     NewMetaStore(config.BlockAddrs),
		log:           make([]*UpdateOperation, 0),

		//IP:          config.RaftAddrs[id],
		Ip_list:        config.RaftAddrs,
		ServerId:       id,
		commitIndex:    -1,
		lastApplied:    -1,
		pendingCommits: make([]chan bool, 0),
		//nextIndex:   0,
		//matchIndex:  0,

		isCrashed:      false,
		isCrashedMutex: &isCrashedMutex,
	}

	return &server, nil
}

// TODO Start up the Raft server and any services here
func ServeRaftServer(server *RaftSurfstore) error {
	lis, err := net.Listen("tcp", server.Ip_list[server.ServerId])
	//log.Println("UUUTILity", server.IP, server.Ip_list)
	if err != nil {
		log.Fatalf("Raft Util failed to listen: %v", err)
	}
	grpcServer := grpc.NewServer()
	RegisterRaftSurfstoreServer(grpcServer, server)
	grpcServer.Serve(lis)
	return nil
}
