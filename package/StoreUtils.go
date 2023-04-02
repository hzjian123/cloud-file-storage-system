package surfstore

import (
	"io"
	"io/ioutil"
	"log"
	"math"
	"os"
	"reflect"
)

// Implement the logic for a client syncing with the server here.
func ClientSync(client RPCClient) {
	full_path := client.BaseDir + "/index.db"
	_, err := os.Stat(full_path)
	if os.IsNotExist(err) {
		log.Println("index does not exist!")
		indexdb, _ := os.Create(full_path)
		dummy := make(map[string]*FileMetaData)
		WriteMetaFile(dummy, client.BaseDir)
		defer indexdb.Close()
	} else if err != nil {
		log.Fatal(err)
	}
	//Local map
	localmap, err := LoadMetaFromMetaFile(client.BaseDir)
	if err != nil {
		log.Println("Error in Loading meta!", err)
	}
	log.Println("Local map")
	PrintMetaMap(localmap)
	// Local file
	hashmap := make(map[string][]string)
	files, err := ioutil.ReadDir(client.BaseDir)
	log.Println("Base map")
	for _, file := range files { //Load file to map
		if file.Name() == "index.db" || file.Name() == ".DS_Store" {
			continue
		}
		log.Println("~~~~~~~~~~~", file.Name(), "++++++++++")
		f, err := os.Open(client.BaseDir + "/" + file.Name())
		if err != nil {
			log.Println("Read error!!!! ", err)
		}
		defer f.Close()
		var num_block int = int(math.Ceil(float64(file.Size()) / float64(client.BlockSize)))
		for i := 0; i < num_block; i++ {
			buf := make([]byte, client.BlockSize)
			lens, err := f.Read(buf)
			if err != nil {
				log.Println("Error in reading to buf", err)
			}
			buf = buf[:lens]
			hash := GetBlockHashString(buf)
			hashmap[file.Name()] = append(hashmap[file.Name()], hash)

		}
	}
	//Check for delete
	for fname, mdata := range localmap {
		_, exist := hashmap[fname]
		log.Println("Existence of  file", exist, fname, mdata.BlockHashList[0])
		if !exist { //local dir miss local map file
			if len(mdata.BlockHashList) != 1 || mdata.BlockHashList[0] != "0" { //???????
				hashmap[fname] = []string{"0"}
				log.Println("Local file is deleted!!!!", localmap[fname].BlockHashList[0])
			}
		}
	}
	//Get remote
	remotemap := make(map[string]*FileMetaData)
	err2 := client.GetFileInfoMap(&remotemap)
	if err2 != nil {
		log.Println("Error in getting remote map!", err2)
		log.Fatal("Majority have crashed! Sync Stopped!!!!")
	}
	log.Println("Remote")
	PrintMetaMap(remotemap)
	// Get remote addr
	var blockStoreAddrs []string
	//blockStoreMap map[string][]string
	err = client.GetBlockStoreAddrs(&blockStoreAddrs)
	log.Println("Download!!!", blockStoreAddrs)
	for k, v := range remotemap {
		_, ok1 := hashmap[k]
		local_m2, ok2 := localmap[k]
		var path string = client.BaseDir + "/" + k
		var con1 bool = (len(v.BlockHashList) == 1 && v.BlockHashList[0] == "0")                    // Remote says delete local file
		var con2 bool = ok2 && len(local_m2.BlockHashList) == 1 && local_m2.BlockHashList[0] == "0" // local says local file is deleted
		log.Println("Check point ", ok1, ok2, con1, con2)

		if !(ok1 && ok2) || (ok2 && local_m2.Version < v.Version) { //Need to download new file
			if !(ok1 && ok2) { //
				log.Println("New file in server!!!", ok1, ok2, len(v.BlockHashList))
			} else {
				log.Println("Download new version!!!!")

			}
			file, err := os.Create(path)
			if err != nil {
				log.Println("Create file with error!!!", err)
			}
			if con1 { //Remote says delete
				log.Println("Remove path!")
				if err := os.Remove(path); err != nil {
					log.Println("Cannot remove file locally!", err)
				}
			} else {
				log.Println("Download blocks to local file", "Version", v.Version, "name", v.Filename, v.BlockHashList[0])
				blocks := download(client, v)
				file.WriteString(blocks)
				defer file.Close()
			}
			localmap[k] = v
			hashmap[k] = v.BlockHashList
		}

	}
	log.Println("UPLOAD")
	for k, v := range hashmap {
		_, ok1 := remotemap[k]
		_, ok2 := localmap[k]
		var con2 bool = ((ok1 && ok2) && (localmap[k].Version == remotemap[k].Version) && (!reflect.DeepEqual(localmap[k].BlockHashList, v))) //new content in local file
		if !(ok1 && ok2) || con2 {                                                                                                            //Upload
			var latest_v int32
			if !(ok1 && ok2) { //New file to server
				log.Println(k, "Not exist!", ok1, ok2, "Upload!")
				err := client.UpdateFile(&FileMetaData{Filename: k, Version: 1, BlockHashList: v}, &latest_v)
				log.Println("Version of new file is:", latest_v)
				if err != nil {
					log.Println("Error in Upload!", err)
					if latest_v == 0 {
						latest_v = 1
					}

				}
			} else { //Update version
				localmap[k].Version += 1 // Try to update, assume new version
				log.Println("Update version, local_v and remote_v:", localmap[k].Version, remotemap[k].Version)
				client.UpdateFile(&FileMetaData{Filename: k, Version: localmap[k].Version, BlockHashList: v}, &latest_v)
				if latest_v == -1 {
					log.Println("Fail to update! Current version", localmap[k].Version, "Remote version:", latest_v)
					localmap[k].Version -= 1 //Fail to update
					continue
				}
			}
			//Add local file to localmap
			localmap[k] = &FileMetaData{Filename: k, Version: latest_v, BlockHashList: v}
			WriteMetaFile(localmap, client.BaseDir)
			//Upload remote  block
			var remote_delete bool = ok1 && (len(remotemap[k].BlockHashList) == 1 && remotemap[k].BlockHashList[0] == "0")
			var local_delete bool = (len(v) == 1 && v[0] == "0")
			if !local_delete { //Write into block when local dir don't have delete
				path := client.BaseDir + "/" + k
				file_stat, _ := os.Stat(path)
				f, err := os.Open(path)
				if err != nil {
					log.Println("Error when open file: ", err)
				}
				defer f.Close()
				c_block := client.BlockSize
				var num_block int = int(math.Ceil(float64(file_stat.Size()) / float64(c_block)))
				//var blocks []*Block
				//var datas [][]byte
				blockStoreMap := make(map[string][]string)

				err = client.GetBlockStoreMap(hashmap[k], &blockStoreMap) // shd change to hash val
				if err != nil {
					log.Println("Error in getting store map!!!!", err)
				}
				for i := 0; i < num_block; i++ {
					buf := make([]byte, c_block)
					data_size, err := f.Read(buf)
					if err != io.EOF && err != nil {
						log.Println("Error when reading data from file!!", err)
					}
					var success bool
					buf = buf[:data_size]
					buf_hash := GetBlockHashString(buf)
					for k, v := range blockStoreMap {
						for _, val := range v {
							if val == buf_hash {
								block := Block{BlockData: buf, BlockSize: int32(data_size)}
								if err := client.PutBlock(&block, k, &success); err != nil {
									log.Println("Failed when put block!!!!! ", err, k)
								}
							}
						}
					}
					//datas = append(datas, buf)
				}
			}
		}

	}
	WriteMetaFile(localmap, client.BaseDir)
}

func download(client RPCClient, remotemeta *FileMetaData) string {
	var block Block
	data := ""
	blockHashes := remotemeta.BlockHashList        // Hash List of a remote file
	datablocks := make([]string, len(blockHashes)) //Temp var store correct order of data
	blockStoreMap := make(map[string][]string)
	var hashlist []string
	for _, blockHash := range blockHashes {
		hashlist = append(hashlist, blockHash)
	}
	err := client.GetBlockStoreMap(hashlist, &blockStoreMap) // [server] [Hash List]
	if err != nil {
		log.Println("Error in getting store map!!!!", err)
	}
	for k, v := range blockStoreMap {
		for _, val := range v {
			//GetResponsibleServer()
			for i := 0; i < len(hashlist); i++ {
				if hashlist[i] == val { //Use remote file index to guide
					err := client.GetBlock(val, k, &block)
					if err != nil {
						log.Println("Error in getting block", err)
						break
					}
					datablocks[i] = string(block.BlockData)
				}
			}
		}
	}
	for _, v := range datablocks {
		data += v
	}
	return data
}
