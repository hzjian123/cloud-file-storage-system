package surfstore

import (
	context "context"
	"log"
	"strings"
	"time"

	grpc "google.golang.org/grpc"
	emptypb "google.golang.org/protobuf/types/known/emptypb"
)

type RPCClient struct {
	MetaStoreAddrs []string
	BaseDir        string
	BlockSize      int
}

func (surfClient *RPCClient) GetBlock(blockHash string, blockStoreAddr string, block *Block) error {
	// connect to the server
	conn, err := grpc.Dial(blockStoreAddr, grpc.WithInsecure())
	if err != nil {
		return err
	}
	c := NewBlockStoreClient(conn)
	// perform the call
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	b, err := c.GetBlock(ctx, &BlockHash{Hash: blockHash})
	if err != nil {
		log.Println("Error in get single block!", err)
		conn.Close()
		return err
	}
	block.BlockData = b.BlockData
	block.BlockSize = b.BlockSize

	// close the connection
	return conn.Close()
}

func (surfClient *RPCClient) PutBlock(block *Block, blockStoreAddr string, succ *bool) error {
	// connect to the server
	conn, err := grpc.Dial(blockStoreAddr, grpc.WithInsecure())
	if err != nil {
		return err
	}
	c := NewBlockStoreClient(conn)
	// perform the call
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	success, err := c.PutBlock(ctx, block)
	if err != nil {
		conn.Close()
		return err
	}
	*succ = success.Flag
	return conn.Close()
}

func (surfClient *RPCClient) HasBlocks(blockHashesIn []string, blockStoreAddr string, blockHashesOut *[]string) error {
	// connect to the server
	conn, err := grpc.Dial(blockStoreAddr, grpc.WithInsecure())
	if err != nil {
		return err
	}
	c := NewBlockStoreClient(conn)

	// perform the call
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	blockhashes, err := c.HasBlocks(ctx, &BlockHashes{Hashes: blockHashesIn})
	if err != nil {
		conn.Close()
		return err
	}
	*blockHashesOut = blockhashes.Hashes
	// close the connection
	return conn.Close()
}

func (surfClient *RPCClient) GetFileInfoMap(serverFileInfoMap *map[string]*FileMetaData) error {
	for _, addr := range surfClient.MetaStoreAddrs {
		conn, err := grpc.Dial(addr, grpc.WithInsecure())
		if err != nil {
			return err
		}
		c := NewRaftSurfstoreClient(conn)
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		maps, err := c.GetFileInfoMap(ctx, &emptypb.Empty{})
		if err != nil {
			if strings.Contains(err.Error(), ERR_SERVER_CRASHED.Error()) || strings.Contains(err.Error(), ERR_NOT_LEADER.Error()) {
				continue
			}
			conn.Close()
			return err
		}
		*serverFileInfoMap = maps.FileInfoMap
		return conn.Close()
	}
	return nil
}

func (surfClient *RPCClient) UpdateFile(fileMetaData *FileMetaData, latestVersion *int32) error {
	for _, addr := range surfClient.MetaStoreAddrs {
		conn, err := grpc.Dial(addr, grpc.WithInsecure())
		if err != nil {
			return err
		}
		c := NewRaftSurfstoreClient(conn)
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		v, err := c.UpdateFile(ctx, fileMetaData)
		if err != nil {
			if strings.Contains(err.Error(), ERR_SERVER_CRASHED.Error()) || strings.Contains(err.Error(), ERR_NOT_LEADER.Error()) {
				continue
			}
			conn.Close()
			return err
		}
		*latestVersion = v.Version
		return conn.Close()
	}
	return nil
}
func (surfClient *RPCClient) GetBlockHashes(blockStoreAddr string, blockHashes *[]string) error {
	conn, err := grpc.Dial(blockStoreAddr, grpc.WithInsecure())
	if err != nil {
		return err
	}
	//conn2, err := grpc.Dial(surfClient.MetaStoreAddrs], grpc.WithInsecure())
	if err != nil {
		return err
	}
	c := NewBlockStoreClient(conn)
	log.Println("CONN for Block and Meta:", blockStoreAddr, surfClient.MetaStoreAddrs)
	// perform the call
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	blockhashes, err := c.GetBlockHashes(ctx, &emptypb.Empty{})
	if err != nil {
		log.Println("Error when get blockhash", err, blockhashes)
		conn.Close()
		return err
	}
	blockStoreMap := make(map[string][]string)
	err = surfClient.GetBlockStoreMap(blockhashes.Hashes, &blockStoreMap)
	if err != nil {
		log.Println("Error when get store map", err)
		conn.Close()
		return err
	}
	list := blockStoreMap[blockStoreAddr]
	*blockHashes = list
	// close the connection
	//conn2.Close()
	return conn.Close()

}
func (surfClient *RPCClient) GetBlockStoreMap(blockHashesIn []string, blockStoreMap *map[string][]string) error {
	for _, addr := range surfClient.MetaStoreAddrs {
		conn, err := grpc.Dial(addr, grpc.WithInsecure())
		if err != nil {
			return err
		}
		c := NewRaftSurfstoreClient(conn)
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		m, err := c.GetBlockStoreMap(ctx, &BlockHashes{Hashes: blockHashesIn})
		//log.Println("RPCCC GEt MAPPPP",err)
		if err != nil {
			if strings.Contains(err.Error(), ERR_SERVER_CRASHED.Error()) || strings.Contains(err.Error(), ERR_NOT_LEADER.Error()) {
				continue
			}
			log.Println("Error in get block store map!", err, addr)
			conn.Close()
			return err
		}
		res := make(map[string][]string)
		for k, v := range m.BlockStoreMap {
			res[k] = v.Hashes
		}
		*blockStoreMap = res
		return conn.Close()
	}
	return nil
}

func (surfClient *RPCClient) GetBlockStoreAddrs(blockStoreAddrs *[]string) error {
	for _, addr := range surfClient.MetaStoreAddrs {
		conn, err := grpc.Dial(addr, grpc.WithInsecure())
		if err != nil {
			return err
		}
		c := NewRaftSurfstoreClient(conn)
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		addrs, err := c.GetBlockStoreAddrs(ctx, &emptypb.Empty{})
		if err != nil {
			if strings.Contains(err.Error(), ERR_SERVER_CRASHED.Error()) || strings.Contains(err.Error(), ERR_NOT_LEADER.Error()) {
				continue
			}
			log.Println("Error in get block store addr!", err, addrs)
			conn.Close()
			return err
		}
		Addrs := addrs.BlockStoreAddrs
		*blockStoreAddrs = Addrs
		log.Println("GET aAdr la!!!")
		return conn.Close()
	}
	return nil
}

// This line guarantees all method for RPCClient are implemented
var _ ClientInterface = new(RPCClient)

// Create an Surfstore RPC client
func NewSurfstoreRPCClient(addrs []string, baseDir string, blockSize int) RPCClient {
	return RPCClient{
		MetaStoreAddrs: addrs,
		BaseDir:        baseDir,
		BlockSize:      blockSize,
	}
}
