package main

import (
	"fmt"
	"os"
	"path/filepath"

	"github.com/exonlabs/go-filedb/pkg/filedb"
)

var (
	DBPATH = filepath.Join(os.TempDir(), "filedb")
)

func main() {
	// init := flag.Bool("init", false, "initialize database store")
	// flag.Parse()

	fmt.Printf("\nUsing Database: %s\n", DBPATH)

	col1 := filedb.NewCollection(DBPATH)

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

	col3 := col2.Child("a2")
	fmt.Println(col3, col3.Child("c.1").IsExist())

	err = col1.Copy("a", "c.1")
	fmt.Println(err)

	err = col1.Move("c.1.a", "b")
	fmt.Println(err)

	fmt.Println()
}
