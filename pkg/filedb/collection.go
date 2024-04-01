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

func (col *Collection) Query() *Query {
	return newQuery(col)
}

func (col *Collection) String() string {
	return fmt.Sprintf("<Collection: %s>", col.BasePath)
}

func (col *Collection) InitAES128(secret string) error {
	cipher, err := xcipher.NewAES128(secret)
	if err != nil {
		return err
	}
	col.cipher = cipher
	return nil
}

func (col *Collection) InitAES256(secret string) error {
	cipher, err := xcipher.NewAES256(secret)
	if err != nil {
		return err
	}
	col.cipher = cipher
	return nil
}

// convert relative file or collection key to absolute path
func (col *Collection) KeyPath(key string) string {
	if key == "" {
		return col.BasePath
	}
	k := strings.ReplaceAll(key, keySep, fileSep)
	return col.BasePath + fileSep + k
}

func (col *Collection) IsExist() bool {
	finfo, err := os.Stat(col.BasePath)
	if os.IsNotExist(err) {
		return false
	}
	if finfo != nil {
		return finfo.Mode().IsDir()
	}
	return true
}

func (col *Collection) ListChilds() ([]string, error) {
	res := []string{}
	err := filepath.Walk(col.BasePath,
		func(path string, info os.FileInfo, err error) error {
			if err != nil {
				return err
			}
			if info.IsDir() && path != col.BasePath {
				res = append(
					res, strings.TrimPrefix(path, col.BasePath+fileSep))
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

// create child collection relative to parent collection
func (col *Collection) Child(key string) *Collection {
	return &Collection{
		BasePath: col.KeyPath(key),
		cipher:   col.cipher,
	}
}

func (col *Collection) GetChilds() ([]*Collection, error) {
	keys, err := col.ListChilds()
	if err != nil {
		return nil, err
	}
	res := []*Collection{}
	for _, k := range keys {
		res = append(res, col.Child(k))
	}
	return res, nil
}

func (col *Collection) Copy(srckey, dstkey string) error {
	srckeypath := col.KeyPath(srckey)
	srcinfo, err := os.Stat(srckeypath)
	if os.IsNotExist(err) {
		return fmt.Errorf("%wsrc collection does not exist", ErrError)
	} else if srcinfo != nil && !srcinfo.Mode().IsDir() {
		return fmt.Errorf("%wsrc key is not collection", ErrError)
	}

	srckeyParts := strings.Split(srckey, keySep)
	srckeyBase := srckeyParts[len(srckeyParts)-1]

	dstkeypath := col.KeyPath(dstkey + keySep + srckeyBase)
	_, err = os.Stat(dstkeypath)
	if !os.IsNotExist(err) {
		return fmt.Errorf("%wdst collection already exists", ErrError)
	}

	if err := xcopy.CopyDir(srckeypath, dstkeypath); err != nil {
		return fmt.Errorf("%w%s", ErrError, err.Error())
	}
	return nil
}

func (col *Collection) Purge(key string) error {
	keypath := col.KeyPath(key)
	finfo, err := os.Stat(keypath)
	if os.IsNotExist(err) {
		return nil
	} else if finfo != nil && !finfo.Mode().IsDir() {
		return fmt.Errorf("%wkey is not collection", ErrError)
	}
	return os.RemoveAll(keypath)
}

func (col *Collection) Move(srckey, dstkey string) error {
	if err := col.Copy(srckey, dstkey); err != nil {
		return err
	}
	return col.Purge(srckey)
}
