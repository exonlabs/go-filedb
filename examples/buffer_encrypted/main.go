package main

import (
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"syscall"

	"github.com/exonlabs/go-filedb/pkg/filedb"
	"github.com/exonlabs/go-utils/pkg/types"
)

var (
	SECRET = "123456789"
)

func init_security(db *filedb.Query, aes256 bool) {
	var err error
	if aes256 {
		err = db.InitAES256(SECRET)
	} else {
		err = db.InitAES128(SECRET)
	}
	if err != nil {
		panic(err)
	}
}

func main() {
	init := flag.Bool("init", false, "initialize database store")
	aes256 := flag.Bool("aes256", false, "use AES256 ciphering")
	flag.Parse()

	dbPath := filepath.Join(os.TempDir(), "filedb")
	fmt.Printf("\nUsing Database: %s\n", dbPath)

	if *init {
		syscall.Umask(0)
		os.RemoveAll(dbPath)
		os.MkdirAll(dbPath, os.ModePerm)

		db := filedb.NewPack(dbPath)
		init_security(db, *aes256)
		d := types.NewNDict(map[string]any{
			"k1": []int{1, 2, 3},
		})
		for _, k := range []string{
			"a.1.11", "a.1.12", "a.2.21", "b.1.11", "c.1.11"} {

			if err := db.SetSecureBuffer(k, d); err != nil {
				fmt.Println("error", err.Error())
			}
		}

		fmt.Printf("Done\n\n")
		return
	}

	db := filedb.NewPack(dbPath)
	init_security(db, *aes256)

	fmt.Println("\nTesting Read ...")
	for _, k := range []string{
		"a.1.11", "a.1.12", "a.2.21", "b.1.11", "c.1.11"} {
		if b, err := db.GetSecureBuffer(k); err != nil {
			fmt.Println("error", err.Error())
		} else {
			fmt.Printf("%s = %v\n", k, b)
		}
	}

	fmt.Println()
}
