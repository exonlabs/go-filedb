package filedb

import (
	"os"
)

type Index struct {
	collection *Collection
}

func (indx *Index) List() ([]string, error) {
	return indx.collection.Query().Keys()
}

func (indx *Index) ListIndexes() ([]string, error) {
	return indx.collection.ListChilds()
}

func (indx *Index) Check(key string) bool {
	return indx.collection.Query().IsExist(key)
}

func (indx *Index) Mark(key string) error {
	fpath := indx.collection.KeyPath(key)
	return indx.collection.Query().TouchFile(fpath)
}

func (indx *Index) Clear(key string) error {
	return indx.collection.Query().Delete(key)
}

func (indx *Index) ClearAll(key string) error {
	indxlist, err := indx.ListIndexes()
	if err != nil {
		return err
	}
	for _, ix := range indxlist {
		indx.collection.Query().Delete(ix + keySep + key)
	}
	return nil
}

func (indx *Index) Purge() error {
	return os.RemoveAll(indx.collection.base_path)
}
