package filedb

import (
	"encoding/json"
	"fmt"
	"path/filepath"
	"strings"

	"github.com/exonlabs/go-utils/pkg/crypto/xcipher"
	"github.com/exonlabs/go-utils/pkg/types"
)

const (
	keySep       = "."
	keyBakSuffix = "_bak"
)

type DB struct {
	*FileEngine

	// root file path prefix for all operations
	RootPath string

	// cipher object
	cipher xcipher.Cipher
}

func NewDB(rootpath string) *DB {
	return &DB{
		FileEngine: NewFileEngine(),
		RootPath:   rootpath,
	}
}

func (db *DB) String() string {
	return fmt.Sprintf("<FileDB: %s>", db.RootPath)
}

func (db *DB) KeyPath(key string) string {
	k := strings.ReplaceAll(key, keySep, string(filepath.Separator))
	return filepath.Join(db.RootPath, k)
}

func (db *DB) IsExist(key string) bool {
	return db.FileExist(db.KeyPath(key))
}

func (db *DB) Get(key string) ([]byte, error) {
	keypath := db.KeyPath(key)
	keybakpath := db.KeyPath(key + keyBakSuffix)

	err := ErrNotExist

	// check main file
	if db.FileExist(keypath) {
		var data []byte
		data, err = db.ReadFile(keypath)
		if err == nil {
			db.WriteFile(keybakpath, data)
			return data, nil
		}
	}

	// check backup
	if db.FileExist(keybakpath) {
		var data []byte
		data, err = db.ReadFile(keybakpath)
		if err == nil {
			db.WriteFile(keypath, data)
			return data, nil
		}
	}

	return nil, err
}
func (db *DB) GetBuffer(key string) (Buffer, error) {
	keypath := db.KeyPath(key)
	keybakpath := db.KeyPath(key + keyBakSuffix)

	err := ErrNotExist

	// check main file
	if db.FileExist(keypath) {
		var rawdata []byte
		rawdata, err = db.ReadFile(keypath)
		if err == nil {
			var data map[string]any
			err = json.Unmarshal(rawdata, &data)
			if err == nil {
				db.WriteFile(keybakpath, rawdata)
				return types.NewNDict(data), nil
			}
		}
	}

	// check backup
	if db.FileExist(keybakpath) {
		var rawdata []byte
		rawdata, err = db.ReadFile(keybakpath)
		if err == nil {
			var data map[string]any
			err = json.Unmarshal(rawdata, &data)
			if err == nil {
				db.WriteFile(keypath, rawdata)
				return types.NewNDict(data), nil
			}
		}
	}

	return nil, err
}
func (db *DB) GetBufferSlice(key string) ([]Buffer, error) {
	keypath := db.KeyPath(key)
	keybakpath := db.KeyPath(key + keyBakSuffix)

	err := ErrNotExist

	// check main file
	if db.FileExist(keypath) {
		var rawdata []byte
		rawdata, err = db.ReadFile(keypath)
		if err == nil {
			var data []map[string]any
			err = json.Unmarshal(rawdata, &data)
			if err == nil {
				db.WriteFile(keybakpath, rawdata)
				return types.NewNDictSlice(data), nil
			}
		}
	}

	// check backup
	if db.FileExist(keybakpath) {
		var rawdata []byte
		rawdata, err = db.ReadFile(keybakpath)
		if err == nil {
			var data []map[string]any
			err = json.Unmarshal(rawdata, &data)
			if err == nil {
				db.WriteFile(keypath, rawdata)
				return types.NewNDictSlice(data), nil
			}
		}
	}

	return nil, err
}

func (db *DB) Set(key string, value []byte) error {
	keypath := db.KeyPath(key)
	keybakpath := db.KeyPath(key + keyBakSuffix)

	err := db.WriteFile(keypath, value)
	if err != nil {
		return err
	}
	return db.WriteFile(keybakpath, value)
}
func (db *DB) SetBuffer(key string, value Buffer) error {
	data, err := json.MarshalIndent(value, "", "  ")
	if err != nil {
		return fmt.Errorf("%w - %s", ErrWrite, err.Error())
	}
	return db.Set(key, data)
}
func (db *DB) SetBufferSlice(key string, value []Buffer) error {
	data, err := json.MarshalIndent(value, "", "  ")
	if err != nil {
		return fmt.Errorf("%w - %s", ErrWrite, err.Error())
	}
	return db.Set(key, data)
}

// delete file
func (db *DB) Delete(key string) error {
	keypath := db.KeyPath(key)
	keybakpath := db.KeyPath(key + keyBakSuffix)
	if db.FileExist(keybakpath) {
		db.PurgeFile(keybakpath)
	}
	if db.FileExist(keypath) {
		return db.PurgeFile(keypath)
	}
	return nil
}

// //////////////////////////////////////////

func (db *DB) InitAES128(secret string) error {
	cipher, err := xcipher.NewAES128(secret)
	if err != nil {
		return err
	}
	db.cipher = cipher
	return nil
}

func (db *DB) InitAES256(secret string) error {
	cipher, err := xcipher.NewAES256(secret)
	if err != nil {
		return err
	}
	db.cipher = cipher
	return nil
}

// read file content with shared locking
func (db *DB) GetSecure(key string) ([]byte, error) {
	if db.cipher == nil {
		return nil, ErrNoSecurity
	}

	keypath := db.KeyPath(key)
	keybakpath := db.KeyPath(key + keyBakSuffix)

	err := ErrNotExist

	// check main file
	if db.FileExist(keypath) {
		var rawdata []byte
		rawdata, err = db.ReadFile(keypath)
		if err == nil {
			var value []byte
			value, err = db.cipher.Decrypt(rawdata)
			if err == nil {
				db.WriteFile(keybakpath, rawdata)
				return value, nil
			}
		}
	}

	// check backup
	if db.FileExist(keybakpath) {
		var rawdata []byte
		rawdata, err = db.ReadFile(keybakpath)
		if err == nil {
			var value []byte
			value, err = db.cipher.Decrypt(rawdata)
			if err == nil {
				db.WriteFile(keypath, rawdata)
				return value, nil
			}
		}
	}

	return nil, err
}
func (db *DB) GetSecureBuffer(key string) (Buffer, error) {
	if db.cipher == nil {
		return nil, ErrNoSecurity
	}

	keypath := db.KeyPath(key)
	keybakpath := db.KeyPath(key + keyBakSuffix)

	err := ErrNotExist

	// check main file
	if db.FileExist(keypath) {
		var rawdata []byte
		rawdata, err = db.ReadFile(keypath)
		if err == nil {
			var value []byte
			value, err = db.cipher.Decrypt(rawdata)
			if err == nil {
				var data map[string]any
				err = json.Unmarshal(value, &data)
				if err == nil {
					db.WriteFile(keybakpath, rawdata)
					return types.NewNDict(data), nil
				}
			}
		}
	}

	// check backup
	if db.FileExist(keybakpath) {
		var rawdata []byte
		rawdata, err = db.ReadFile(keybakpath)
		if err == nil {
			var value []byte
			value, err = db.cipher.Decrypt(rawdata)
			if err == nil {
				var data map[string]any
				err = json.Unmarshal(value, &data)
				if err == nil {
					db.WriteFile(keypath, rawdata)
					return types.NewNDict(data), nil
				}
			}
		}
	}

	return nil, err
}
func (db *DB) GetSecureBufferSlice(key string) ([]Buffer, error) {
	if db.cipher == nil {
		return nil, ErrNoSecurity
	}

	keypath := db.KeyPath(key)
	keybakpath := db.KeyPath(key + keyBakSuffix)

	err := ErrNotExist

	// check main file
	if db.FileExist(keypath) {
		var rawdata []byte
		rawdata, err = db.ReadFile(keypath)
		if err == nil {
			var value []byte
			value, err = db.cipher.Decrypt(rawdata)
			if err == nil {
				var data []map[string]any
				err = json.Unmarshal(value, &data)
				if err == nil {
					db.WriteFile(keybakpath, rawdata)
					return types.NewNDictSlice(data), nil
				}
			}
		}
	}

	// check backup
	if db.FileExist(keybakpath) {
		var rawdata []byte
		rawdata, err = db.ReadFile(keybakpath)
		if err == nil {
			var value []byte
			value, err = db.cipher.Decrypt(rawdata)
			if err == nil {
				var data []map[string]any
				err = json.Unmarshal(value, &data)
				if err == nil {
					db.WriteFile(keypath, rawdata)
					return types.NewNDictSlice(data), nil
				}
			}
		}
	}

	return nil, err
}

// write content to file with exclusive locking
func (db *DB) SetSecure(key string, value []byte) error {
	if db.cipher == nil {
		return ErrNoSecurity
	}
	b, err := db.cipher.Encrypt(value)
	if err != nil {
		return fmt.Errorf("%w%s", ErrEncrypt, err.Error())
	}
	return db.Set(key, b)
}
func (db *DB) SetSecureBuffer(key string, value Buffer) error {
	data, err := json.Marshal(value)
	if err != nil {
		return fmt.Errorf("%w - %s", ErrWrite, err.Error())
	}
	return db.SetSecure(key, data)
}
func (db *DB) SetSecureBufferSlice(key string, value []Buffer) error {
	data, err := json.Marshal(value)
	if err != nil {
		return fmt.Errorf("%w - %s", ErrWrite, err.Error())
	}
	return db.SetSecure(key, data)
}
