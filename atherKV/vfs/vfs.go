package vfs

import (
	"errors"
	"io"
	"os"
)

var (
	ErrNotFound      = errors.New("vfs: file or directory not found")
	ErrAlreadyExists = errors.New("vfs: file or directory already exists")
	ErrClosed        = errors.New("vfs: file is closed")
)

// FS abstracts disk operations so systems can run on OS or SimFS.
type FS interface {
	Open(name string) (File, error)
	Create(name string) (File, error)
	Rename(oldpath, newpath string) error
	Remove(name string) error
	List(dir string) ([]string, error)
	SyncDir(dir string) error
	MkdirAll(dir string) error
}

// File abstracts random read/write access and durability sync boundaries.
type File interface {
	io.Closer
	ReadAt(p []byte, off int64) (n int, err error)
	WriteAt(p []byte, off int64) (n int, err error)
	Truncate(size int64) error
	Sync() error
	Size() (int64, error)
}

// OSFS implements FS directly using the host operating system.
type OSFS struct{}

func NewOSFS() *OSFS { return &OSFS{} }

func (o *OSFS) Open(name string) (File, error) {
	f, err := os.OpenFile(name, os.O_RDWR, 0o644)
	if err != nil {
		return nil, err
	}
	return &osFile{f}, nil
}

func (o *OSFS) Create(name string) (File, error) {
	f, err := os.OpenFile(name, os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0o644)
	if err != nil {
		return nil, err
	}
	return &osFile{f}, nil
}

func (o *OSFS) Rename(oldpath, newpath string) error {
	return os.Rename(oldpath, newpath)
}

func (o *OSFS) Remove(name string) error {
	return os.Remove(name)
}

func (o *OSFS) List(dir string) ([]string, error) {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return nil, err
	}
	var names []string
	for _, e := range entries {
		names = append(names, e.Name())
	}
	return names, nil
}

func (o *OSFS) SyncDir(dir string) error {
	d, err := os.Open(dir)
	if err != nil {
		return err
	}
	defer d.Close()
	return d.Sync()
}

func (o *OSFS) MkdirAll(dir string) error {
	return os.MkdirAll(dir, 0o755)
}

type osFile struct {
	*os.File
}

func (f *osFile) Size() (int64, error) {
	st, err := f.Stat()
	if err != nil {
		return 0, err
	}
	return st.Size(), nil
}
