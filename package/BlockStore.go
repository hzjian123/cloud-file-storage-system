package surfstore

import (
	context "context"

	emptypb "google.golang.org/protobuf/types/known/emptypb"
)

type BlockStore struct {
	BlockMap map[string]*Block
	UnimplementedBlockStoreServer
}

func (bs *BlockStore) GetBlock(ctx context.Context, blockHash *BlockHash) (*Block, error) {
	b := bs.BlockMap[blockHash.Hash]
	//log.Println("BBBBBBBBBBBB!", b, blockHash.Hash, bs.BlockMap)
	return b, nil
}

func (bs *BlockStore) PutBlock(ctx context.Context, block *Block) (*Success, error) {
	hash := GetBlockHashString(block.BlockData)
	bs.BlockMap[hash] = block
	return &Success{Flag: true}, nil
}

// Given a list of hashes “in”, returns a list containing the
// subset of in that are stored in the key-value store
func (bs *BlockStore) HasBlocks(ctx context.Context, blockHashesIn *BlockHashes) (*BlockHashes, error) {
	var hashes []string
	for _, hash := range blockHashesIn.Hashes {
		_, exist := bs.BlockMap[hash]
		if exist {
			hashes = append(hashes, hash)
		}
	}
	return &BlockHashes{Hashes: hashes}, nil

}

// This line guarantees all method for BlockStore are implemented
var _ BlockStoreInterface = new(BlockStore)

func NewBlockStore() *BlockStore {
	return &BlockStore{
		BlockMap: map[string]*Block{},
	}
}

// Return a list containing all blockHashes on this block server
func (bs *BlockStore) GetBlockHashes(ctx context.Context, _ *emptypb.Empty) (*BlockHashes, error) {
	hashes := []string{}
	for blk_hash, _ := range bs.BlockMap {
		//hash := GetBlockHashString(blk)
		hashes = append(hashes, blk_hash)
	}
	return &BlockHashes{Hashes: hashes}, nil
}
