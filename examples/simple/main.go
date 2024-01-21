package main

import (
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"syscall"

	"github.com/exonlabs/go-filedb/pkg/filedb"
)

func main() {
	init := flag.Bool("init", false, "initialize database store")
	flag.Parse()

	dbPath := filepath.Join(os.TempDir(), "filedb")
	fmt.Printf("\nUsing Database: %s\n", dbPath)

	if *init {
		syscall.Umask(0)
		os.RemoveAll(dbPath)
		os.MkdirAll(dbPath, 0o777)

		db := filedb.NewDB(dbPath)
		for _, k := range []string{
			"a.1.11", "a.1.12", "a.2.21", "b.1.11", "c.1.11"} {
			if err := db.Set(k, []byte{0, 1, 2, 3}); err != nil {
				panic(err)
			}
		}

		fmt.Printf("Done\n\n")
		return
	}

	db := filedb.NewDB(dbPath)

	fmt.Println("\nTesting Read ...")
	for _, k := range []string{
		"a.1.11", "a.1.12", "a.2.21", "b.1.11", "c.1.11"} {
		if b, err := db.Get(k); err != nil {
			panic(err)
		} else {
			fmt.Printf("%s = %v\n", k, b)
		}
	}

	fmt.Println("\nTesting Overwrite ...")
	for _, k := range []string{
		"a.1.11", "a.1.12", "a.2.21", "b.1.11", "c.1.11"} {
		if err := db.Set(k, []byte{10, 11, 12, 13}); err != nil {
			panic(err)
		}
	}
	for _, k := range []string{
		"a.1.11", "a.1.12", "a.2.21", "b.1.11", "c.1.11"} {
		if b, err := db.Get(k); err != nil {
			panic(err)
		} else {
			fmt.Printf("%s = %v\n", k, b)
		}
	}

	fmt.Println("\nTesting Delete ...")
	for _, k := range []string{"b.1.11", "c.1.11"} {
		if err := db.Delete(k); err != nil {
			panic(err)
		}
	}
	for _, k := range []string{
		"a.1.11", "a.1.12", "a.2.21", "b.1.11", "c.1.11", "a.b.c.d"} {
		fmt.Printf("%s   IsExist  %v\n", k, db.IsExist(k))
	}

	fmt.Println()
}
