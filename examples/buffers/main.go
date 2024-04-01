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

func main() {
	init := flag.Bool("init", false, "initialize database store")
	flag.Parse()

	dbPath := filepath.Join(os.TempDir(), "filedb")
	fmt.Printf("\nUsing Database: %s\n", dbPath)

	if *init {
		syscall.Umask(0)
		os.RemoveAll(dbPath)
		os.MkdirAll(dbPath, os.ModePerm)

		db := filedb.NewPack(dbPath)
		d := types.NewNDictSlice([]map[string]any{
			{"k1": []int{1, 2, 3}},
			{"k2": 1.4},
		})
		for _, k := range []string{
			"a.1.11", "a.1.12", "a.2.21", "b.1.11", "c.1.11"} {
			if err := db.SetBufferSlice(k, d); err != nil {
				fmt.Println("error", err.Error())
			}
		}

		fmt.Printf("Done\n\n")
		return
	}

	db := filedb.NewPack(dbPath)
	fmt.Println("\nTesting Read ...")
	for _, k := range []string{
		"a.1.11", "a.1.12", "a.2.21", "b.1.11", "c.1.11"} {
		if b, err := db.GetBufferSlice(k); err != nil {
			fmt.Println("error", err.Error())
		} else {
			fmt.Printf("%s = %v\n", k, b)

			fmt.Printf("+++++++++ %T\n", b[1].GetFloat64("k2", 0))
		}
	}

	fmt.Println()
}
