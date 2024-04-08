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
				fmt.Printf(
					"Error setting secure buffer for key %s: %v\n", k, err)
				return
			}
		}

		fmt.Printf("Done\n\n")
		return
	}

	col1 := dbc

	res, err := col1.Query().Keys()
	fmt.Println(res, err)

	res, err = col1.ListChilds()
	fmt.Println(res, err)

	col2 := col1.Child("a")
	fmt.Println(col2)

	res, err = col2.Query().Keys()
	fmt.Println(res, err)

	res, err = col2.ListChilds()
	fmt.Println(res, err)

	col3 := col2.Child("2")
	fmt.Println(col3, col3.IsExist())

	err = col1.Copy("a", "c.1")
	fmt.Println(err)

	err = col1.Move("c.1.a", "b")
	fmt.Println(err)
}
