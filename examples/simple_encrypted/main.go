package main

import (
	"encoding/hex"
	"fmt"
	"log"
	"os"

	"github.com/exonlabs/go-filedb/pkg/filedb"
)

func main() {
	path := "encrypted"
	key, _ := hex.DecodeString("000102030405060708090A0B0C0D0E0F")
	authKey, _ := hex.DecodeString("D0D1D2D3D4D5D6D7D8D9DADBDCDDDEDF")

	err := os.Mkdir(path, os.ModePerm)
	if err != nil {
		log.Panic(err)
	}

	db, err := filedb.NewDB(path)
	if err != nil {
		log.Panic(err)
	}

	fmt.Println("root path:", db.GetRootPath())

	db.SetCiphering(key, authKey)

	err = db.Set("devices/SEE0031008997/status", []byte("ACT"))
	if err != nil {
		log.Println(err)
	}

	err = db.Set("devices/SEE0031008997/mac", []byte("a0:19:b2:20:6b:0f"))
	if err != nil {
		log.Println(err)
	}

	val, err := db.Get("devices/SEE0031008997/status")
	if err != nil {
		log.Println(err)
	}
	fmt.Println("get status:", string(val))

	val = db.Fetch("devices/SEE0031008997/status", []byte("no status"))
	fmt.Println("fetch status:", string(val))

	val = db.Fetch("devices/SEE0031008997/file", []byte("data"))
	if err != nil {
		log.Println(err)
	}
	fmt.Println("default value:", string(val))

	err = db.Delete("devices/SEE0031008997/mac")
	if err != nil {
		log.Println(err)
	}

	err = db.Purge()
	if err != nil {
		log.Println(err)
	}
}
