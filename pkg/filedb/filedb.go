package filedb

import (
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
	"errors"
	"io"
	"os"
	"path/filepath"
	"strings"
	"time"

	"golang.org/x/sys/unix"
)

type DB struct {
	rootPath       string
	retriesTimeout time.Duration
	retry          int
	*security
}

type security struct {
	encKey, authKey []byte
}

func NewDB(rootPath string) (*DB, error) {
	if _, err := os.Stat(rootPath); errors.Is(err, os.ErrNotExist) {
		return &DB{}, err
	}
	return &DB{
		rootPath:       rootPath,
		retry:          5,
		retriesTimeout: time.Second * 1,
	}, nil
}

func (db *DB) SetCiphering(encKey, authKey []byte) {
	db.security = &security{
		encKey:  encKey,
		authKey: authKey,
	}
}

func (db *DB) encrypt(data []byte) ([]byte, error) {
	block, err := aes.NewCipher(db.security.encKey)
	if err != nil {
		return nil, err
	}

	nonce := make([]byte, 12)
	if _, err := io.ReadFull(rand.Reader, nonce); err != nil {
		panic(err.Error())
	}

	aesgcm, err := cipher.NewGCM(block)
	if err != nil {
		return nil, err
	}

	encData := aesgcm.Seal(nil, nonce, data, db.security.authKey)

	return append(nonce, encData...), nil
}

func (db *DB) decrypt(cipherData []byte) ([]byte, error) {
	block, err := aes.NewCipher(db.security.encKey)
	if err != nil {
		return nil, err
	}

	aesgcm, err := cipher.NewGCM(block)
	if err != nil {
		return nil, err
	}

	data, err := aesgcm.Open(nil, cipherData[:12], cipherData[12:], db.security.authKey)
	if err != nil {
		return nil, err
	}

	return data, nil
}

func (db *DB) GetRootPath() string {
	return db.rootPath
}

func (db *DB) Get(key string) ([]byte, error) {
	path := filepath.Join(db.rootPath, key)
	// data, err := os.ReadFile(path)
	// if err != nil {
	// 	return nil, err
	// }
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	r_retry := 1
lock:
	// lock file
	if err := unix.Flock(int(f.Fd()), unix.LOCK_EX|unix.LOCK_NB); err != nil {
		if err == unix.EWOULDBLOCK && r_retry <= db.retry {
			r_retry++
			time.Sleep(db.retriesTimeout)
			goto lock
		}

		return nil, err
	}
	// unlock file
	defer unix.Flock(int(f.Fd()), unix.LOCK_UN)

	// get file info
	fi, err := f.Stat()
	if err != nil {
		return nil, err
	}

	// read data
	data := make([]byte, fi.Size())
	_, err = f.Read(data)
	if err != nil {
		return nil, err
	}

	// decrypted value
	if db.security != nil {
		data, err = db.decrypt(data)
		if err != nil {
			return nil, err
		}
	}

	return data, nil
}

func (db *DB) Fetch(key string, defVal []byte) []byte {
	data, err := db.Get(key)
	if err != nil {
		return defVal
	}

	return data
}

func (db *DB) Set(key string, value []byte) error {
	var newPath string
	path := filepath.Join(db.rootPath, key)
	pathSpl := strings.Split(key, "/")

	if len(pathSpl) > 1 {
		newPath = filepath.Join(db.rootPath, strings.Join(pathSpl[:len(pathSpl)-1], "/"))
		if _, err := os.Stat(newPath); errors.Is(err, os.ErrNotExist) {
			err := os.MkdirAll(newPath, os.ModePerm)
			if err != nil {
				return err
			}
		}
	}

	f, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE|os.O_TRUNC, os.ModePerm)
	if err != nil {
		return err
	}
	defer f.Close()

	w_retry := 1
lock:
	// lock file
	if err := unix.Flock(int(f.Fd()), unix.LOCK_EX|unix.LOCK_NB); err != nil {
		if err == unix.EWOULDBLOCK && w_retry <= db.retry {
			w_retry++
			time.Sleep(db.retriesTimeout)
			goto lock
		}

		return err
	}
	// unlock file
	defer unix.Flock(int(f.Fd()), unix.LOCK_UN)

	// encrypt value
	if db.security != nil {
		value, err = db.encrypt(value)
		if err != nil {
			return err
		}
	}

	_, err = f.Write(value)
	if err != nil {
		return nil
	}

	return nil
}

func (db *DB) Delete(key string) error {
	path := filepath.Join(db.rootPath, key)
	if err := os.RemoveAll(path); err != nil {
		return err
	}
	return nil
}

func (db *DB) Purge() error {
	if err := os.RemoveAll(db.rootPath); err != nil {
		return err
	}
	return nil
}
