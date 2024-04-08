package filedb

import (
	"fmt"
	"os"
	"path/filepath"
	"time"

	"github.com/exonlabs/go-utils/pkg/sync/xevent"
	"golang.org/x/sys/unix"
)

type FileEngine struct {
	// operation events
	evtBreak *xevent.Event

	// timeout for operations like read/write
	OpTimeout float64
	// polling interval for blocked operations
	OpPolling float64
	// permission for new dir creation
	DirPerm uint32
	// permission for new file creation
	FilePerm uint32
}

// create new file engine
func NewFileEngine() *FileEngine {
	return &FileEngine{
		evtBreak:  xevent.NewEvent(),
		OpTimeout: defaultOpTimeout,
		OpPolling: defaultOpPolling,
		DirPerm:   defaultDirPerm,
		FilePerm:  defaultFilePerm,
	}
}

// update file engine options
func (dbe *FileEngine) UpdateOptions(opts Options) {
	dbe.OpTimeout = opts.GetFloat64("op_timeout", dbe.OpTimeout)
	dbe.OpPolling = opts.GetFloat64("op_polling", dbe.OpPolling)
	dbe.DirPerm = opts.GetUint32("dir_perm", dbe.DirPerm)
	dbe.FilePerm = opts.GetUint32("file_perm", dbe.FilePerm)
}

// check if file exists and is regular file
func (dbe *FileEngine) FileExist(fpath string) bool {
	finfo, err := os.Stat(fpath)
	if os.IsNotExist(err) {
		return false
	}
	if finfo != nil {
		return finfo.Mode().IsRegular()
	}
	return true
}

// read file content with shared locking
func (dbe *FileEngine) ReadFile(fpath string) ([]byte, error) {
	// open file for read
	f, err := os.OpenFile(fpath, os.O_RDONLY, os.ModePerm)
	if err != nil {
		return nil, fmt.Errorf("%w - %s", ErrRead, err.Error())
	}
	defer f.Close()

	// aquire file lock with retries
	if err := dbe.aquireFilelock(
		f, false, dbe.OpTimeout, dbe.OpPolling); err != nil {
		return nil, err
	}
	defer dbe.releaseFilelock(f)

	finfo, _ := f.Stat()
	data := make([]byte, finfo.Size())
	_, err = f.Read(data)
	if err != nil {
		return nil, fmt.Errorf("%w - %s", ErrRead, err.Error())
	}
	return data, nil
}

// write content to file with exclusive locking
func (dbe *FileEngine) WriteFile(fpath string, data []byte) error {
	// create dir tree for file if not exist
	if !dbe.FileExist(fpath) {
		dirpath := filepath.Dir(fpath)
		if err := os.MkdirAll(dirpath, os.FileMode(dbe.DirPerm)); err != nil {
			return fmt.Errorf("%w - %s", ErrWrite, err.Error())
		}
	}

	// open file for write
	f, err := os.OpenFile(
		fpath, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, os.FileMode(dbe.FilePerm))
	if err != nil {
		return fmt.Errorf("%w - %s", ErrWrite, err.Error())
	}
	defer f.Close()

	// aquire file lock with retries
	if err := dbe.aquireFilelock(
		f, true, dbe.OpTimeout, dbe.OpPolling); err != nil {
		return err
	}
	defer dbe.releaseFilelock(f)

	_, err = f.Write(data)
	if err != nil {
		return fmt.Errorf("%w - %s", ErrWrite, err.Error())
	}
	return nil
}

// create file if not exist
func (dbe *FileEngine) TouchFile(fpath string) error {
	// create dir tree for file if not exist
	if !dbe.FileExist(fpath) {
		dirpath := filepath.Dir(fpath)
		if err := os.MkdirAll(dirpath, os.FileMode(dbe.DirPerm)); err != nil {
			return fmt.Errorf("%w - %s", ErrWrite, err.Error())
		}
	}

	// open file for write
	f, err := os.OpenFile(
		fpath, os.O_WRONLY|os.O_CREATE, os.FileMode(dbe.FilePerm))
	if err != nil {
		return fmt.Errorf("%w - %s", ErrWrite, err.Error())
	}
	defer f.Close()

	return nil
}

// delete file
func (dbe *FileEngine) PurgeFile(fpath string) error {
	err := os.Remove(fpath)
	if err != nil {
		return fmt.Errorf("%w%s", ErrError, err.Error())
	}
	return nil
}

// cancel blocking operations
func (dbe *FileEngine) Cancel() {
	dbe.evtBreak.Set()
}

// aquire file lock with retries
func (dbe *FileEngine) aquireFilelock(
	f *os.File, wr bool, tout, tpoll float64) error {
	var err error
	dbe.evtBreak.Clear()
	tbreak := float64(time.Now().Unix()) + tout
	for {
		if wr {
			// exclusive lock for writing
			err = unix.Flock(int(f.Fd()), unix.LOCK_EX|unix.LOCK_NB)
		} else {
			// shared lock for reading
			err = unix.Flock(int(f.Fd()), unix.LOCK_SH|unix.LOCK_NB)
		}
		if err == nil {
			return nil
		} else if err != unix.EWOULDBLOCK {
			return fmt.Errorf("%w%s", ErrError, err.Error())
		} else if tout <= 0 {
			return ErrLocked
		}
		time.Sleep(time.Duration(tpoll * 1000000000))
		if dbe.evtBreak.IsSet() {
			return ErrBreak
		}
		if float64(time.Now().Unix()) >= tbreak {
			return ErrTimeout
		}
	}
}

// release file lock
func (dbe *FileEngine) releaseFilelock(f *os.File) {
	unix.Flock(int(f.Fd()), unix.LOCK_UN|unix.LOCK_NB)
}
