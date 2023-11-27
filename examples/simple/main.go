package main

import (
	"fmt"
	"log"
	"os"

	"github.com/exonlabs/go-filedb/pkg/filedb"
)

func main() {
	path := "data"

	err := os.Mkdir(path, os.ModePerm)
	if err != nil {
		log.Panic(err)
	}

	db, err := filedb.NewDB(path)
	if err != nil {
		log.Panic(err)
	}

	fmt.Println("root path:", db.GetRootPath())

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

	err = db.SetUint("devices/SEE0031008997/uint", 10)
	if err != nil {
		log.Println(err)
	}
	u, err := db.GetUint("devices/SEE0031008997/uint")
	if err != nil {
		log.Println(err)
	}
	fmt.Println("get uint:", u)
	u = db.FetchUint("devices/SEE0031008997/nouint", 3)
	if err != nil {
		log.Println(err)
	}
	fmt.Println("fetch uint:", u)

	err = db.SetInt("devices/SEE0031008997/int", 123456)
	if err != nil {
		log.Println(err)
	}
	i, err := db.GetInt("devices/SEE0031008997/int")
	if err != nil {
		log.Println(err)
	}
	fmt.Println("get int:", i)
	i = db.FetchInt("devices/SEE0031008997/noint", 3)
	if err != nil {
		log.Println(err)
	}
	fmt.Println("fetch int:", i)

	err = db.SetFloat("devices/SEE0031008997/float", 12.3456)
	if err != nil {
		log.Println(err)
	}
	f, err := db.GetFloat("devices/SEE0031008997/float")
	if err != nil {
		log.Println(err)
	}
	fmt.Println("get float:", f)
	f = db.FetchFloat("devices/SEE0031008997/nofloat", 3.4)
	if err != nil {
		log.Println(err)
	}
	fmt.Println("fetch float:", f)

	err = db.SetString("devices/SEE0031008997/string", "Hello World")
	if err != nil {
		log.Println(err)
	}
	s, err := db.GetString("devices/SEE0031008997/string")
	if err != nil {
		log.Println(err)
	}
	fmt.Println("get string:", s)
	s = db.FetchString("devices/SEE0031008997/nostring", "data")
	if err != nil {
		log.Println(err)
	}
	fmt.Println("fetch string:", s)

	err = db.SetBool("devices/SEE0031008997/bool", true)
	if err != nil {
		log.Println(err)
	}
	b, err := db.GetBool("devices/SEE0031008997/bool")
	if err != nil {
		log.Println(err)
	}
	fmt.Println("get bool:", b)
	b = db.FetchBool("devices/SEE0031008997/nobool", true)
	if err != nil {
		log.Println(err)
	}
	fmt.Println("fetch bool:", b)

	err = db.Purge()
	if err != nil {
		log.Println(err)
	}
}
