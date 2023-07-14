package surfstore

import (
	"crypto/sha256"
	"encoding/hex"
	"sort"
)

type ConsistentHashRing struct {
	ServerMap map[string]string
}

func (c ConsistentHashRing) GetResponsibleServer(blockId string) string {
	//find block location
	hashes := []string{}
	for h, _ := range c.ServerMap {
		hashes = append(hashes, h)
	}
	sort.Strings(hashes)
	blockhash := blockId
	server := ""
	//var con string
	for i := 0; i < len(hashes); i++ {
		if hashes[i] > blockhash {
			server = c.ServerMap[hashes[i]]
			//con = "First" + string(int(i))
			//log.Println("~~~~", hashes[i], blockhash)
			break
		}
	}
	if server == "" {
		server = c.ServerMap[hashes[0]]
		//con = "Second"
	}
	//log.Println("***", blockId[:5], server, con)
	return server
}

func (c ConsistentHashRing) Hash(addr string) string {
	h := sha256.New()
	h.Write([]byte(addr))
	return hex.EncodeToString(h.Sum(nil))

}

func NewConsistentHashRing(serverAddrs []string) *ConsistentHashRing {
	ring := make(map[string]string) // server to ring
	for _, server_name := range serverAddrs {
		h := sha256.New()
		h.Write([]byte("blockstore" + server_name))
		hash := hex.EncodeToString(h.Sum(nil))
		serverhash := hash
		//log.Println("CCCCCCCCCCCC", "blockstore"+server_name, serverhash)
		ring[serverhash] = server_name
	}
	return &ConsistentHashRing{ServerMap: ring}
}
