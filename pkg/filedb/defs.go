package filedb

import (
	"errors"
	"fmt"
	"path/filepath"

	"github.com/exonlabs/go-utils/pkg/abc/dictx"
)

type Options = dictx.Dict
type Buffer = dictx.Dict

const (
	keySep           = "."
	keyBakSuffix     = "_bak"
	fileSep          = string(filepath.Separator)
	defaultOpTimeout = float64(3)
	defaultOpPolling = float64(0.1)
	defaultDirPerm   = uint(0o775)
	defaultFilePerm  = uint(0o664)
)

var (
	ErrError      = errors.New("")
	ErrTimeout    = fmt.Errorf("%wtimeout", ErrError)
	ErrBreak      = fmt.Errorf("%woperation break", ErrError)
	ErrRead       = fmt.Errorf("%wread failed", ErrError)
	ErrWrite      = fmt.Errorf("%wwrite failed", ErrError)
	ErrNotExist   = fmt.Errorf("%wfile does not exist", ErrError)
	ErrLocked     = fmt.Errorf("%wfile locked", ErrError)
	ErrNoSecurity = fmt.Errorf("%wsecurity not configured", ErrError)
	ErrInvalidKey = fmt.Errorf("%winvalid key size", ErrError)
	ErrEncrypt    = fmt.Errorf("%wencryption failed", ErrError)
	ErrDecrypt    = fmt.Errorf("%wdecryption failed", ErrError)
)
