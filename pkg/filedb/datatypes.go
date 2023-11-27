package filedb

import (
	"math"
	"strconv"

	"github.com/exonlabs/go-filedb/pkg/filedb/helpers"
)

// uint64
func (db *DB) SetUint(key string, value uint64) error {
	err := db.Set(key, helpers.B8(value))
	if err != nil {
		return err
	}
	return nil
}

func (db *DB) GetUint(key string) (uint64, error) {
	val, err := db.Get(key)
	if err != nil {
		return 0, err
	}

	return helpers.U64(val), nil
}

func (db *DB) FetchUint(key string, defVal uint64) uint64 {
	data, err := db.GetUint(key)
	if err != nil {
		return defVal
	}

	return data
}

// int64
func (db *DB) SetInt(key string, value int64) error {
	err := db.Set(key, helpers.Q8(value))
	if err != nil {
		return err
	}
	return nil
}

func (db *DB) GetInt(key string) (int64, error) {
	val, err := db.Get(key)
	if err != nil {
		return 0, err
	}

	return helpers.I64(val), nil
}

func (db *DB) FetchInt(key string, defVal int64) int64 {
	data, err := db.GetInt(key)
	if err != nil {
		return defVal
	}

	return data
}

// float64
func (db *DB) SetFloat(key string, value float64) error {
	err := db.Set(key, helpers.B8(math.Float64bits(value)))
	if err != nil {
		return err
	}
	return nil
}

func (db *DB) GetFloat(key string) (float64, error) {
	val, err := db.Get(key)
	if err != nil {
		return 0, err
	}

	return math.Float64frombits(helpers.U64(val)), nil
}

func (db *DB) FetchFloat(key string, defVal float64) float64 {
	data, err := db.GetFloat(key)
	if err != nil {
		return defVal
	}

	return data
}

// string
func (db *DB) SetString(key string, value string) error {
	err := db.Set(key, []byte(value))
	if err != nil {
		return err
	}
	return nil
}

func (db *DB) GetString(key string) (string, error) {
	val, err := db.Get(key)
	if err != nil {
		return "", err
	}

	return string(val), nil
}

func (db *DB) FetchString(key string, defVal string) string {
	data, err := db.GetString(key)
	if err != nil {
		return defVal
	}

	return data
}

// bool
func (db *DB) SetBool(key string, value bool) error {
	boolVal := []byte("0")
	if value {
		boolVal = []byte("1")
	}

	err := db.Set(key, boolVal)
	if err != nil {
		return err
	}
	return nil
}

func (db *DB) GetBool(key string) (bool, error) {
	val, err := db.Get(key)
	if err != nil {
		return false, err
	}

	return strconv.ParseBool(string(val))
}

func (db *DB) FetchBool(key string, defVal bool) bool {
	data, err := db.GetBool(key)
	if err != nil {
		return defVal
	}

	return data
}
