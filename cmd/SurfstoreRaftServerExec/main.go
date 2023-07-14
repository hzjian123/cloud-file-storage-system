package main

import (
	"cse224/proj5/pkg/surfstore"
	"flag"
	"io/ioutil"
	"log"
)

func main() {
	serverId := flag.Int64("i", -1, "(required) Server ID")
	configFile := flag.String("f", "", "(required) Config file, absolute path")
	debug := flag.Bool("d", false, "Output log statements")
	BlockStoreAddr := flag.String("b", "", "(required)  address of blockstore")
	flag.Parse()

	config := surfstore.LoadRaftConfigFile(*configFile)

	// Disable log outputs if debug flag is missing
	if *debug {
		log.SetFlags(0)
		log.SetOutput(ioutil.Discard)
	}
	var flag bool = false
	for _, b_addr := range config.BlockAddrs {
		if b_addr == *BlockStoreAddr || *BlockStoreAddr == "" {
			flag = true
			break
		}
	}
	if !flag {
		log.Println("Address not in list!!!!!!")
		return
	}
	//log.Println("Raft Server start!!!!!!", config.BlockAddrs, *BlockStoreAddr)
	log.Fatal(startServer(*serverId, config))
}

func startServer(id int64, config surfstore.RaftConfig) error {
	raftServer, err := surfstore.NewRaftServer(id, config)
	if err != nil {
		log.Fatal("Error creating servers")
	}

	return surfstore.ServeRaftServer(raftServer)
}
