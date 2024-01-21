package main

import (
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"syscall"

	"github.com/exonlabs/go-filedb/pkg/filedb"
)

var (
	SECRET = "123456789"
)

func init_security(db *filedb.DB, aes256 bool) {
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
		os.MkdirAll(dbPath, 0o777)

		db := filedb.NewDB(dbPath)
		init_security(db, *aes256)
		for _, k := range []string{
			"a.1.11", "a.1.12", "a.2.21", "b.1.11", "c.1.11"} {
			if err := db.SetSecure(k, []byte{0, 1, 2}); err != nil {
				fmt.Println("error", err.Error())
			}
		}

		fmt.Printf("Done\n\n")
		return
	}

	db := filedb.NewDB(dbPath)
	init_security(db, *aes256)

	fmt.Println("\nTesting Read ...")
	for _, k := range []string{
		"a.1.11", "a.1.12", "a.2.21", "b.1.11", "c.1.11"} {
		if b, err := db.GetSecure(k); err != nil {
			fmt.Println("error", err.Error())
		} else {
			fmt.Printf("%s = %v\n", k, b)
		}
	}

	fmt.Println()
}
