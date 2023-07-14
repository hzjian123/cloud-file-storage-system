package surfstore

import (
	context "context"
	"log"

	emptypb "google.golang.org/protobuf/types/known/emptypb"
)

type MetaStore struct {
	FileMetaMap        map[string]*FileMetaData
	BlockStoreAddrs    []string
	ConsistentHashRing *ConsistentHashRing
	UnimplementedMetaStoreServer
}

func (m *MetaStore) GetFileInfoMap(ctx context.Context, _ *emptypb.Empty) (*FileInfoMap, error) {
	return &FileInfoMap{FileInfoMap: m.FileMetaMap}, nil
}

func (m *MetaStore) UpdateFile(ctx context.Context, fileMetaData *FileMetaData) (*Version, error) {
	v := fileMetaData.Version
	name := fileMetaData.Filename
	_, try := m.FileMetaMap[name]
	if try {
		meta := m.FileMetaMap[name]
		old_v := meta.Version
		if v-old_v == 1 {
			m.FileMetaMap[name] = fileMetaData
			log.Println("New version!", old_v, v, m.FileMetaMap[name].Version)
		} else {
			v = -1
		}
	} else {
		m.FileMetaMap[name] = fileMetaData
	}
	return &Version{Version: v}, nil
}
func (m *MetaStore) GetBlockStoreMap(ctx context.Context, blockHashesIn *BlockHashes) (*BlockStoreMap, error) { // hash->server
	blockmap := make(map[string][]string)
	BlockMap := make(map[string]*BlockHashes)
	for _, blockhash := range blockHashesIn.Hashes {
		server := m.ConsistentHashRing.GetResponsibleServer(blockhash)
		blockmap[server] = append(blockmap[server], blockhash)
	}
	for k, v := range blockmap {
		BlockMap[k] = &BlockHashes{Hashes: v}
	}
	return &BlockStoreMap{BlockStoreMap: BlockMap}, nil
}
func (m *MetaStore) GetBlockStoreAddrs(ctx context.Context, _ *emptypb.Empty) (*BlockStoreAddrs, error) {
	log.Println("Server get store addrs of:", m.BlockStoreAddrs)
	return &BlockStoreAddrs{BlockStoreAddrs: m.BlockStoreAddrs}, nil
}

// This line guarantees all method for MetaStore are implemented
var _ MetaStoreInterface = new(MetaStore)

func NewMetaStore(blockStoreAddrs []string) *MetaStore {
	return &MetaStore{
		FileMetaMap:        map[string]*FileMetaData{},
		BlockStoreAddrs:    blockStoreAddrs,
		ConsistentHashRing: NewConsistentHashRing(blockStoreAddrs),
	}
}
