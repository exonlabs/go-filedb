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
	DBPATH = filepath.Join(os.TempDir(), "filedb")
)

func main() {
	init := flag.Bool("init", false, "initialize database store")
	flag.Parse()

	fmt.Printf("\nUsing Database: %s\n", DBPATH)

	dbc, _ := filedb.NewCollection(DBPATH)

	if *init {
		syscall.Umask(0)
		os.RemoveAll(DBPATH)
		os.MkdirAll(DBPATH, os.ModePerm)

		dbq := dbc.Query()
		d := types.NewNDict(map[string]any{
			"k1": []int{1, 2, 3},
			"k2": 1.4,
		})
		for _, k := range []string{
			"a.1.11", "a.1.12", "a.2.21", "b.1.11", "c.1.11"} {
			if err := dbq.SetBufferSlice(k, []types.NDict{d}); err != nil {
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
		if b, err := dbq.GetBufferSlice(k); err != nil {
			fmt.Println("Error:", err.Error())
			return
		} else {
			fmt.Printf("%s = %v\n", k, b)
		}
	}

	fmt.Println()
}
