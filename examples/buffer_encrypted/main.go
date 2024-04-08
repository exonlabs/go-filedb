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
	DBPATH = filepath.Join(os.TempDir(), "filedb")
)

func init_security(dbc *filedb.Collection, aes256 bool) {
	var err error
	if aes256 {
		err = dbc.InitAES256(SECRET)
	} else {
		err = dbc.InitAES128(SECRET)
	}
	if err != nil {
		panic(fmt.Sprintf("Failed to initialize encryption: %v", err))
	}
}

func main() {
	init := flag.Bool("init", false, "initialize database store")
	aes256 := flag.Bool("aes256", false, "use AES256 ciphering")
	flag.Parse()

	fmt.Printf("\nUsing Database: %s\n", DBPATH)

	dbc := filedb.NewCollection(DBPATH)
	init_security(dbc, *aes256)

	if *init {
		syscall.Umask(0)
		os.RemoveAll(DBPATH)
		os.MkdirAll(DBPATH, os.ModePerm)

		dbq := dbc.Query()
		d := types.NewNDict(map[string]any{
			"k1": []int{1, 2, 3},
		})
		for _, k := range []string{
			"a.1.11", "a.1.12", "a.2.21", "b.1.11", "c.1.11"} {
			if err := dbq.SetSecureBuffer(k, d); err != nil {
				fmt.Println("Error:", err.Error())
				return
			}
		}

		fmt.Printf("Done\n\n")
		return
	}

	fmt.Println("\nTesting Read ...")
	dbq := dbc.Query()
	for _, k := range []string{
		"a.1.11", "a.1.12", "a.2.21", "b.1.11", "c.1.11"} {
		if b, err := dbq.GetSecureBuffer(k); err != nil {
			fmt.Println("Error:", err.Error())
			return
		} else {
			fmt.Printf("%s = %v\n", k, b)
		}
	}

	fmt.Println()
}
