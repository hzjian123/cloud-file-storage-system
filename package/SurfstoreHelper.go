package surfstore

import (
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"fmt"
	"log"
	"os"
	"path/filepath"

	_ "github.com/mattn/go-sqlite3"
)

/* Hash Related */
func GetBlockHashBytes(blockData []byte) []byte {
	h := sha256.New()
	h.Write(blockData)
	return h.Sum(nil)
}

func GetBlockHashString(blockData []byte) string {
	blockHash := GetBlockHashBytes(blockData)
	return hex.EncodeToString(blockHash)
}

/* File Path Related */
func ConcatPath(baseDir, fileDir string) string {
	return baseDir + "/" + fileDir
}

/*
	Writing Local Metadata File Related
*/

const createTable string = `create table if not exists indexes (
		fileName TEXT, 
		version INT,
		hashIndex INT,
		hashValue TEXT
	);`

const insertTuple string = `insert into indexes (fileName,version, hashIndex, hashValue) VALUES (?,?,?,?)`

// WriteMetaFile writes the file meta map back to local metadata file index.db
func WriteMetaFile(fileMetas map[string]*FileMetaData, baseDir string) error {
	// remove index.db file if it exists
	outputMetaPath := ConcatPath(baseDir, DEFAULT_META_FILENAME)
	if _, err := os.Stat(outputMetaPath); err == nil {
		e := os.Remove(outputMetaPath)
		if e != nil {
			log.Fatal("Error During Meta Write Back")
		}
	}
	db, err := sql.Open("sqlite3", outputMetaPath)
	if err != nil {
		log.Fatal("Error During open path")
	}
	statement, err := db.Prepare(createTable)
	if err != nil {
		log.Fatal("Error During create table")
	}
	statement.Exec()
	statement, err = db.Prepare(insertTuple)
	for _, v := range fileMetas {
		log.Println("Write to DB", v.Filename, len(v.BlockHashList))
		if err != nil {
			log.Fatal("Error During writing data")
		}
		for i := 0; i < len(v.BlockHashList); i++ {
			statement.Exec(v.Filename, v.Version, i, v.BlockHashList[i])
		}
	}
	return nil
}

/*
Reading Local Metadata File Related
*/
const getDistinctFileName string = `select distinct fileName 
from indexes `

const getTuplesByFileName string = `select fileName,version,hashIndex,hashValue
from indexes 
where fileName = ?`

// order by hashIndex;
// LoadMetaFromMetaFile loads the local metadata file into a file meta map.
// The key is the file's name and the value is the file's metadata.
// You can use this function to load the index.db file in this project.
func LoadMetaFromMetaFile(baseDir string) (fileMetaMap map[string]*FileMetaData, e error) {
	metaFilePath, _ := filepath.Abs(ConcatPath(baseDir, DEFAULT_META_FILENAME))
	fileMetaMap = make(map[string]*FileMetaData)
	metaFileStats, e := os.Stat(metaFilePath)
	if e != nil || metaFileStats.IsDir() {
		return fileMetaMap, nil
	}
	db, err := sql.Open("sqlite3", metaFilePath)
	if err != nil {
		log.Fatal("Error When Opening Meta")
	}
	//Distinct name
	rows, err := db.Query(getDistinctFileName)
	if err != nil {
		log.Fatal("Query ERROR1!!!!!!")
	}
	var names []string
	for rows.Next() {
		var tempname string
		rows.Scan(&tempname)
		names = append(names, tempname)
	}
	//log.Println("Names:", names)
	for _, name := range names {
		//log.Println(name == "sea.jpg")
		rows, err = db.Query(getTuplesByFileName, name)
		if err != nil {
			log.Fatal("Query ERROR2!!!!!!")
		}
		var hashlist []string
		var filename string
		var version int
		var hashIndex int
		var hashValue string
		for rows.Next() {
			rows.Scan(&filename, &version, &hashIndex, &hashValue)
			//log.Println("VVVVVVVVVVVVVVVV", filename, hashValue, version)
			hashlist = append(hashlist, hashValue)
		}
		filemeta := &FileMetaData{
			Filename:      filename,
			Version:       int32(version),
			BlockHashList: hashlist,
		}
		fileMetaMap[filemeta.Filename] = filemeta
	}

	return fileMetaMap, nil //Eachann
}

/*
	Debugging Related
*/

// PrintMetaMap prints the contents of the metadata map.
// You might find this function useful for debugging.
func PrintMetaMap(metaMap map[string]*FileMetaData) {

	fmt.Println("--------BEGIN PRINT MAP--------")

	for _, filemeta := range metaMap {
		fmt.Println("\t", filemeta.Filename, filemeta.Version)
		continue
		for _, blockHash := range filemeta.BlockHashList {
			fmt.Println("\t", blockHash)
		}
	}

	fmt.Println("---------END PRINT MAP--------")

}
