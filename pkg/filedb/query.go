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

func newQuery(col *Collection) *Query {
	return &Query{
		FileEngine: NewFileEngine(),
		collection: col,
	}
}

func (qry *Query) Keys() ([]string, error) {
	res := []string{}
	err := filepath.Walk(qry.collection.BasePath,
		func(path string, info os.FileInfo, err error) error {
			if err != nil {
				return err
			}
			if !info.IsDir() {
				if !strings.HasSuffix(path, keyBakSuffix) {
					res = append(res, strings.TrimPrefix(
						path, qry.collection.BasePath+fileSep))
				}
			} else if path != qry.collection.BasePath {
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

func (qry *Query) IsExist(key string) bool {
	return qry.FileExist(qry.collection.KeyPath(key))
}

func (qry *Query) Get(key string) ([]byte, error) {
	keypath := qry.collection.KeyPath(key)
	keybakpath := qry.collection.KeyPath(key + keyBakSuffix)

	err := ErrNotExist

	// check main file
	if qry.FileExist(keypath) {
		var data []byte
		data, err = qry.ReadFile(keypath)
		if err == nil {
			qry.WriteFile(keybakpath, data)
			return data, nil
		}
	}

	// check backup
	if qry.FileExist(keybakpath) {
		var data []byte
		data, err = qry.ReadFile(keybakpath)
		if err == nil {
			qry.WriteFile(keypath, data)
			return data, nil
		}
	}

	return nil, err
}
func (qry *Query) GetBuffer(key string) (Buffer, error) {
	keypath := qry.collection.KeyPath(key)
	keybakpath := qry.collection.KeyPath(key + keyBakSuffix)

	err := ErrNotExist

	// check main file
	if qry.FileExist(keypath) {
		var rawdata []byte
		rawdata, err = qry.ReadFile(keypath)
		if err == nil {
			var data map[string]any
			err = json.Unmarshal(rawdata, &data)
			if err == nil {
				qry.WriteFile(keybakpath, rawdata)
				return types.NewNDict(data), nil
			}
		}
	}

	// check backup
	if qry.FileExist(keybakpath) {
		var rawdata []byte
		rawdata, err = qry.ReadFile(keybakpath)
		if err == nil {
			var data map[string]any
			err = json.Unmarshal(rawdata, &data)
			if err == nil {
				qry.WriteFile(keypath, rawdata)
				return types.NewNDict(data), nil
			}
		}
	}

	return nil, err
}
func (qry *Query) GetBufferSlice(key string) ([]Buffer, error) {
	keypath := qry.collection.KeyPath(key)
	keybakpath := qry.collection.KeyPath(key + keyBakSuffix)

	err := ErrNotExist

	// check main file
	if qry.FileExist(keypath) {
		var rawdata []byte
		rawdata, err = qry.ReadFile(keypath)
		if err == nil {
			var data []map[string]any
			err = json.Unmarshal(rawdata, &data)
			if err == nil {
				qry.WriteFile(keybakpath, rawdata)
				return types.NewNDictSlice(data), nil
			}
		}
	}

	// check backup
	if qry.FileExist(keybakpath) {
		var rawdata []byte
		rawdata, err = qry.ReadFile(keybakpath)
		if err == nil {
			var data []map[string]any
			err = json.Unmarshal(rawdata, &data)
			if err == nil {
				qry.WriteFile(keypath, rawdata)
				return types.NewNDictSlice(data), nil
			}
		}
	}

	return nil, err
}

func (qry *Query) Set(key string, value []byte) error {
	keypath := qry.collection.KeyPath(key)
	keybakpath := qry.collection.KeyPath(key + keyBakSuffix)

	err := qry.WriteFile(keypath, value)
	if err != nil {
		return err
	}
	return qry.WriteFile(keybakpath, value)
}
func (qry *Query) SetBuffer(key string, value Buffer) error {
	data, err := json.MarshalIndent(value, "", "  ")
	if err != nil {
		return fmt.Errorf("%w - %s", ErrWrite, err.Error())
	}
	return qry.Set(key, data)
}
func (qry *Query) SetBufferSlice(key string, value []Buffer) error {
	data, err := json.MarshalIndent(value, "", "  ")
	if err != nil {
		return fmt.Errorf("%w - %s", ErrWrite, err.Error())
	}
	return qry.Set(key, data)
}

// delete file
func (qry *Query) Delete(key string) error {
	keypath := qry.collection.KeyPath(key)
	keybakpath := qry.collection.KeyPath(key + keyBakSuffix)
	if qry.FileExist(keybakpath) {
		qry.PurgeFile(keybakpath)
	}
	if qry.FileExist(keypath) {
		return qry.PurgeFile(keypath)
	}
	return nil
}

// read file content with shared locking
func (qry *Query) GetSecure(key string) ([]byte, error) {
	if qry.collection.cipher == nil {
		return nil, ErrNoSecurity
	}

	keypath := qry.collection.KeyPath(key)
	keybakpath := qry.collection.KeyPath(key + keyBakSuffix)

	err := ErrNotExist

	// check main file
	if qry.FileExist(keypath) {
		var rawdata []byte
		rawdata, err = qry.ReadFile(keypath)
		if err == nil {
			var value []byte
			value, err = qry.collection.cipher.Decrypt(rawdata)
			if err == nil {
				qry.WriteFile(keybakpath, rawdata)
				return value, nil
			}
		}
	}

	// check backup
	if qry.FileExist(keybakpath) {
		var rawdata []byte
		rawdata, err = qry.ReadFile(keybakpath)
		if err == nil {
			var value []byte
			value, err = qry.collection.cipher.Decrypt(rawdata)
			if err == nil {
				qry.WriteFile(keypath, rawdata)
				return value, nil
			}
		}
	}

	return nil, err
}
func (qry *Query) GetSecureBuffer(key string) (Buffer, error) {
	if qry.collection.cipher == nil {
		return nil, ErrNoSecurity
	}

	keypath := qry.collection.KeyPath(key)
	keybakpath := qry.collection.KeyPath(key + keyBakSuffix)

	err := ErrNotExist

	// check main file
	if qry.FileExist(keypath) {
		var rawdata []byte
		rawdata, err = qry.ReadFile(keypath)
		if err == nil {
			var value []byte
			value, err = qry.collection.cipher.Decrypt(rawdata)
			if err == nil {
				var data map[string]any
				err = json.Unmarshal(value, &data)
				if err == nil {
					qry.WriteFile(keybakpath, rawdata)
					return types.NewNDict(data), nil
				}
			}
		}
	}

	// check backup
	if qry.FileExist(keybakpath) {
		var rawdata []byte
		rawdata, err = qry.ReadFile(keybakpath)
		if err == nil {
			var value []byte
			value, err = qry.collection.cipher.Decrypt(rawdata)
			if err == nil {
				var data map[string]any
				err = json.Unmarshal(value, &data)
				if err == nil {
					qry.WriteFile(keypath, rawdata)
					return types.NewNDict(data), nil
				}
			}
		}
	}

	return nil, err
}
func (qry *Query) GetSecureBufferSlice(key string) ([]Buffer, error) {
	if qry.collection.cipher == nil {
		return nil, ErrNoSecurity
	}

	keypath := qry.collection.KeyPath(key)
	keybakpath := qry.collection.KeyPath(key + keyBakSuffix)

	err := ErrNotExist

	// check main file
	if qry.FileExist(keypath) {
		var rawdata []byte
		rawdata, err = qry.ReadFile(keypath)
		if err == nil {
			var value []byte
			value, err = qry.collection.cipher.Decrypt(rawdata)
			if err == nil {
				var data []map[string]any
				err = json.Unmarshal(value, &data)
				if err == nil {
					qry.WriteFile(keybakpath, rawdata)
					return types.NewNDictSlice(data), nil
				}
			}
		}
	}

	// check backup
	if qry.FileExist(keybakpath) {
		var rawdata []byte
		rawdata, err = qry.ReadFile(keybakpath)
		if err == nil {
			var value []byte
			value, err = qry.collection.cipher.Decrypt(rawdata)
			if err == nil {
				var data []map[string]any
				err = json.Unmarshal(value, &data)
				if err == nil {
					qry.WriteFile(keypath, rawdata)
					return types.NewNDictSlice(data), nil
				}
			}
		}
	}

	return nil, err
}

// write content to file with exclusive locking
func (qry *Query) SetSecure(key string, value []byte) error {
	if qry.collection.cipher == nil {
		return ErrNoSecurity
	}
	b, err := qry.collection.cipher.Encrypt(value)
	if err != nil {
		return fmt.Errorf("%w%s", ErrEncrypt, err.Error())
	}
	return qry.Set(key, b)
}
func (qry *Query) SetSecureBuffer(key string, value Buffer) error {
	data, err := json.Marshal(value)
	if err != nil {
		return fmt.Errorf("%w - %s", ErrWrite, err.Error())
	}
	return qry.SetSecure(key, data)
}
func (qry *Query) SetSecureBufferSlice(key string, value []Buffer) error {
	data, err := json.Marshal(value)
	if err != nil {
		return fmt.Errorf("%w - %s", ErrWrite, err.Error())
	}
	return qry.SetSecure(key, data)
}
