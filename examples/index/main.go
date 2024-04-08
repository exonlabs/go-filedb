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
		})
		for _, k := range []string{
			"a.1.11", "a.1.12", "a.2.21", "b.1.11", "c.1.11"} {
			if err := dbq.SetBuffer(k, d); err != nil {
				fmt.Printf(
					"Error setting secure buffer for key %s: %v\n", k, err)
				return
			}
		}

		idx := dbc.Index("index.key1")
		idx.Mark("v1")
		idx.Mark("v2")
		dbc.Index("index.key2").Mark("v3")

		chld1 := dbc.Child("a")
		chld1.Index("h").Mark("h1")

		fmt.Printf("Initialization Done\n\n")
		return
	}

	fmt.Println("\nList Childs ...")
	res, err := dbc.ListChilds()
	fmt.Println(res, err)

	fmt.Println("\nList Indexes ...")
	res, err = dbc.ListIndexes()
	fmt.Println(res, err)

	idx := dbc.Index("index")
	fmt.Println("\nList sub indexes ...")
	res, err = idx.ListIndexes()
	fmt.Println(res, err)
	for _, n := range res {
		k := "index." + n
		l, _ := dbc.Index(k).List()
		fmt.Println(k, l)
	}

	// Marking a key
	fmt.Println("\nKeys Marking ...")
	if err := idx.Mark("KEY"); err != nil {
		fmt.Println("Error marking key:", err)
	} else {
		fmt.Println("Key marked")
		l, _ := idx.List()
		fmt.Println(l)
	}

	// Clearing a key
	fmt.Println("\nKeys Clearing ...")
	if err := idx.Clear("KEY"); err != nil {
		fmt.Println("Error clearing key :", err)
	} else {
		fmt.Println("Key cleared")
		l, _ := idx.List()
		fmt.Println(l)
	}

	// purge index
	fmt.Println("\nPurging Index ...")
	if err := idx.Purge(); err != nil {
		fmt.Println("Error purging index:", err)
	} else {
		fmt.Println("Index purged")
		res, err = dbc.ListIndexes()
		fmt.Println(res, err)
	}

	fmt.Println()
}
