package filedb

import (
	"encoding/json"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"strings"

	"github.com/exonlabs/go-utils/pkg/types"
)

type Query struct {
	*FileEngine
	collection *Collection
}

func newQuery(dbc *Collection) *Query {
	return &Query{
		FileEngine: NewFileEngine(),
		collection: dbc,
	}
}

func (dbq *Query) Keys() ([]string, error) {
	res := []string{}
	err := filepath.Walk(dbq.collection.base_path,
		func(path string, info os.FileInfo, err error) error {
			if err != nil {
				return err
			}
			if !info.IsDir() {
				if !strings.HasSuffix(path, keyBakSuffix) {
					res = append(res, strings.TrimPrefix(
						path, dbq.collection.base_path+fileSep))
				}
			} else if path != dbq.collection.base_path {
				return fs.SkipDir
			}
			return nil
		},
	)
	if err != nil {
		return nil, err
	}
	return res, nil
}

func (dbq *Query) IsExist(key string) bool {
	return dbq.FileExist(dbq.collection.KeyPath(key))
}

func (dbq *Query) Get(key string) ([]byte, error) {
	keypath := dbq.collection.KeyPath(key)
	keybakpath := dbq.collection.KeyPath(key + keyBakSuffix)

	err := ErrNotExist

	// check main file
	if dbq.FileExist(keypath) {
		var data []byte
		data, err = dbq.ReadFile(keypath)
		if err == nil {
			dbq.WriteFile(keybakpath, data)
			return data, nil
		}
	}

	// check backup
	if dbq.FileExist(keybakpath) {
		var data []byte
		data, err = dbq.ReadFile(keybakpath)
		if err == nil {
			dbq.WriteFile(keypath, data)
			return data, nil
		}
	}

	return nil, err
}
func (dbq *Query) GetBuffer(key string) (Buffer, error) {
	keypath := dbq.collection.KeyPath(key)
	keybakpath := dbq.collection.KeyPath(key + keyBakSuffix)

	err := ErrNotExist

	// check main file
	if dbq.FileExist(keypath) {
		var rawdata []byte
		rawdata, err = dbq.ReadFile(keypath)
		if err == nil {
			var data map[string]any
			err = json.Unmarshal(rawdata, &data)
			if err == nil {
				dbq.WriteFile(keybakpath, rawdata)
				return types.NewNDict(data), nil
			}
		}
	}

	// check backup
	if dbq.FileExist(keybakpath) {
		var rawdata []byte
		rawdata, err = dbq.ReadFile(keybakpath)
		if err == nil {
			var data map[string]any
			err = json.Unmarshal(rawdata, &data)
			if err == nil {
				dbq.WriteFile(keypath, rawdata)
				return types.NewNDict(data), nil
			}
		}
	}

	return nil, err
}
func (dbq *Query) GetBufferSlice(key string) ([]Buffer, error) {
	keypath := dbq.collection.KeyPath(key)
	keybakpath := dbq.collection.KeyPath(key + keyBakSuffix)

	err := ErrNotExist

	// check main file
	if dbq.FileExist(keypath) {
		var rawdata []byte
		rawdata, err = dbq.ReadFile(keypath)
		if err == nil {
			var data []map[string]any
			err = json.Unmarshal(rawdata, &data)
			if err == nil {
				dbq.WriteFile(keybakpath, rawdata)
				var res []Buffer
				for _, d := range data {
					res = append(res, types.NewNDict(d))
				}
				return res, nil
			}
		}
	}

	// check backup
	if dbq.FileExist(keybakpath) {
		var rawdata []byte
		rawdata, err = dbq.ReadFile(keybakpath)
		if err == nil {
			var data []map[string]any
			err = json.Unmarshal(rawdata, &data)
			if err == nil {
				dbq.WriteFile(keypath, rawdata)
				var res []Buffer
				for _, d := range data {
					res = append(res, types.NewNDict(d))
				}
				return res, nil
			}
		}
	}

	return nil, err
}

func (dbq *Query) Set(key string, value []byte) error {
	keypath := dbq.collection.KeyPath(key)
	keybakpath := dbq.collection.KeyPath(key + keyBakSuffix)

	err := dbq.WriteFile(keypath, value)
	if err != nil {
		return err
	}
	return dbq.WriteFile(keybakpath, value)
}
func (dbq *Query) SetBuffer(key string, value Buffer) error {
	data, err := json.MarshalIndent(value, "", "  ")
	if err != nil {
		return fmt.Errorf("%w - %s", ErrWrite, err.Error())
	}
	return dbq.Set(key, data)
}
func (dbq *Query) SetBufferSlice(key string, value []Buffer) error {
	data, err := json.MarshalIndent(value, "", "  ")
	if err != nil {
		return fmt.Errorf("%w - %s", ErrWrite, err.Error())
	}
	return dbq.Set(key, data)
}

// delete file
func (dbq *Query) Delete(key string) error {
	keypath := dbq.collection.KeyPath(key)
	keybakpath := dbq.collection.KeyPath(key + keyBakSuffix)
	if dbq.FileExist(keybakpath) {
		dbq.PurgeFile(keybakpath)
	}
	if dbq.FileExist(keypath) {
		return dbq.PurgeFile(keypath)
	}
	return nil
}

// read file content with shared locking
func (dbq *Query) GetSecure(key string) ([]byte, error) {
	if dbq.collection.cipher == nil {
		return nil, ErrNoSecurity
	}

	keypath := dbq.collection.KeyPath(key)
	keybakpath := dbq.collection.KeyPath(key + keyBakSuffix)

	err := ErrNotExist

	// check main file
	if dbq.FileExist(keypath) {
		var rawdata []byte
		rawdata, err = dbq.ReadFile(keypath)
		if err == nil {
			var value []byte
			value, err = dbq.collection.cipher.Decrypt(rawdata)
			if err == nil {
				dbq.WriteFile(keybakpath, rawdata)
				return value, nil
			}
		}
	}

	// check backup
	if dbq.FileExist(keybakpath) {
		var rawdata []byte
		rawdata, err = dbq.ReadFile(keybakpath)
		if err == nil {
			var value []byte
			value, err = dbq.collection.cipher.Decrypt(rawdata)
			if err == nil {
				dbq.WriteFile(keypath, rawdata)
				return value, nil
			}
		}
	}

	return nil, err
}
func (dbq *Query) GetSecureBuffer(key string) (Buffer, error) {
	if dbq.collection.cipher == nil {
		return nil, ErrNoSecurity
	}

	keypath := dbq.collection.KeyPath(key)
	keybakpath := dbq.collection.KeyPath(key + keyBakSuffix)

	err := ErrNotExist

	// check main file
	if dbq.FileExist(keypath) {
		var rawdata []byte
		rawdata, err = dbq.ReadFile(keypath)
		if err == nil {
			var value []byte
			value, err = dbq.collection.cipher.Decrypt(rawdata)
			if err == nil {
				var data map[string]any
				err = json.Unmarshal(value, &data)
				if err == nil {
					dbq.WriteFile(keybakpath, rawdata)
					return types.NewNDict(data), nil
				}
			}
		}
	}

	// check backup
	if dbq.FileExist(keybakpath) {
		var rawdata []byte
		rawdata, err = dbq.ReadFile(keybakpath)
		if err == nil {
			var value []byte
			value, err = dbq.collection.cipher.Decrypt(rawdata)
			if err == nil {
				var data map[string]any
				err = json.Unmarshal(value, &data)
				if err == nil {
					dbq.WriteFile(keypath, rawdata)
					return types.NewNDict(data), nil
				}
			}
		}
	}

	return nil, err
}
func (dbq *Query) GetSecureBufferSlice(key string) ([]Buffer, error) {
	if dbq.collection.cipher == nil {
		return nil, ErrNoSecurity
	}

	keypath := dbq.collection.KeyPath(key)
	keybakpath := dbq.collection.KeyPath(key + keyBakSuffix)

	err := ErrNotExist

	// check main file
	if dbq.FileExist(keypath) {
		var rawdata []byte
		rawdata, err = dbq.ReadFile(keypath)
		if err == nil {
			var value []byte
			value, err = dbq.collection.cipher.Decrypt(rawdata)
			if err == nil {
				var data []map[string]any
				err = json.Unmarshal(value, &data)
				if err == nil {
					dbq.WriteFile(keybakpath, rawdata)
					var res []Buffer
					for _, d := range data {
						res = append(res, types.NewNDict(d))
					}
					return res, nil
				}
			}
		}
	}

	// check backup
	if dbq.FileExist(keybakpath) {
		var rawdata []byte
		rawdata, err = dbq.ReadFile(keybakpath)
		if err == nil {
			var value []byte
			value, err = dbq.collection.cipher.Decrypt(rawdata)
			if err == nil {
				var data []map[string]any
				err = json.Unmarshal(value, &data)
				if err == nil {
					dbq.WriteFile(keypath, rawdata)
					var res []Buffer
					for _, d := range data {
						res = append(res, types.NewNDict(d))
					}
					return res, nil
				}
			}
		}
	}

	return nil, err
}

// write content to file with exclusive locking
func (dbq *Query) SetSecure(key string, value []byte) error {
	if dbq.collection.cipher == nil {
		return ErrNoSecurity
	}
	b, err := dbq.collection.cipher.Encrypt(value)
	if err != nil {
		return fmt.Errorf("%w%s", ErrEncrypt, err.Error())
	}
	return dbq.Set(key, b)
}
func (dbq *Query) SetSecureBuffer(key string, value Buffer) error {
	data, err := json.Marshal(value)
	if err != nil {
		return fmt.Errorf("%w - %s", ErrWrite, err.Error())
	}
	return dbq.SetSecure(key, data)
}
func (dbq *Query) SetSecureBufferSlice(key string, value []Buffer) error {
	data, err := json.Marshal(value)
	if err != nil {
		return fmt.Errorf("%w - %s", ErrWrite, err.Error())
	}
	return dbq.SetSecure(key, data)
}
