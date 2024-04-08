package filedb

import (
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"strings"

	"github.com/exonlabs/go-utils/pkg/crypto/xcipher"
	"github.com/exonlabs/go-utils/pkg/os/xcopy"
)

type Collection struct {
	// base path prefix for all operations
	BasePath string

	// cipher object
	cipher xcipher.Cipher
}

func NewCollection(path string) *Collection {
	return &Collection{
		BasePath: strings.TrimSuffix(path, fileSep),
	}
}

func (dbc *Collection) String() string {
	return fmt.Sprintf("<Collection: %s>", dbc.BasePath)
}

func (dbc *Collection) InitAES128(secret string) error {
	cipher, err := xcipher.NewAES128(secret)
	if err != nil {
		return err
	}
	dbc.cipher = cipher
	return nil
}

func (dbc *Collection) InitAES256(secret string) error {
	cipher, err := xcipher.NewAES256(secret)
	if err != nil {
		return err
	}
	dbc.cipher = cipher
	return nil
}

// convert relative file or collection key to absolute path
func (dbc *Collection) KeyPath(key string) string {
	if key == "" {
		return dbc.BasePath
	}
	k := strings.ReplaceAll(key, keySep, fileSep)
	return dbc.BasePath + fileSep + k
}

func (dbc *Collection) IsExist() bool {
	finfo, err := os.Stat(dbc.BasePath)
	if os.IsNotExist(err) {
		return false
	}
	if finfo != nil {
		return finfo.Mode().IsDir()
	}
	return true
}

func (dbc *Collection) Copy(srckey, dstkey string) error {
	srckeypath := dbc.KeyPath(srckey)
	srcinfo, err := os.Stat(srckeypath)
	if os.IsNotExist(err) {
		return fmt.Errorf("%wsrc collection does not exist", ErrError)
	} else if srcinfo != nil && !srcinfo.Mode().IsDir() {
		return fmt.Errorf("%wsrc key is not collection", ErrError)
	}

	srckeyParts := strings.Split(srckey, keySep)
	srckeyBase := srckeyParts[len(srckeyParts)-1]

	dstkeypath := dbc.KeyPath(dstkey + keySep + srckeyBase)
	_, err = os.Stat(dstkeypath)
	if !os.IsNotExist(err) {
		return fmt.Errorf("%wdst collection already exists", ErrError)
	}

	if err := xcopy.CopyDir(srckeypath, dstkeypath); err != nil {
		return fmt.Errorf("%w%s", ErrError, err.Error())
	}
	return nil
}

func (dbc *Collection) Purge(key string) error {
	keypath := dbc.KeyPath(key)
	finfo, err := os.Stat(keypath)
	if os.IsNotExist(err) {
		return nil
	} else if finfo != nil && !finfo.Mode().IsDir() {
		return fmt.Errorf("%wkey is not collection", ErrError)
	}
	return os.RemoveAll(keypath)
}

func (dbc *Collection) Move(srckey, dstkey string) error {
	if err := dbc.Copy(srckey, dstkey); err != nil {
		return err
	}
	return dbc.Purge(srckey)
}

//////////////////////////////// child methods

// create child collection relative to parent collection
func (dbc *Collection) Child(key string) *Collection {
	return &Collection{
		BasePath: dbc.KeyPath(key),
		cipher:   dbc.cipher,
	}
}

func (dbc *Collection) ListChilds() ([]string, error) {
	res := []string{}
	err := filepath.Walk(dbc.BasePath,
		func(path string, info os.FileInfo, err error) error {
			if err != nil {
				return err
			}
			if info.IsDir() && path != dbc.BasePath {
				res = append(
					res, strings.TrimPrefix(path, dbc.BasePath+fileSep))
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

func (dbc *Collection) GetChilds() ([]*Collection, error) {
	keys, err := dbc.ListChilds()
	if err != nil {
		return nil, err
	}
	res := []*Collection{}
	for _, k := range keys {
		res = append(res, dbc.Child(k))
	}
	return res, nil
}

//////////////////////////////// Query methods

func (dbc *Collection) Query() *Query {
	return newQuery(dbc)
}
