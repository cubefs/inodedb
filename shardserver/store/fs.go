package store

import (
	"os"
	"path/filepath"
)

type (
	RawFS interface {
		CreateRawFile(name string) (RawFile, error)
		OpenRawFile(name string) (RawFile, error)
		ReadDir(dir string) ([]string, error)
	}
	RawFile interface {
		Read(p []byte) (n int, err error)
		Write(p []byte) (n int, err error)
		Close() error
	}
)

type posixRawFS struct {
	path string
}

func (r *posixRawFS) CreateRawFile(name string) (RawFile, error) {
	filePath := r.path + "/" + name

	f, err := os.OpenFile(r.path+"/"+name, os.O_CREATE|os.O_RDWR, 0o755)
	if err != nil {
		if !os.IsNotExist(err) {
			return nil, err
		}

		dir := filepath.Dir(filePath)
		if err := os.MkdirAll(dir, 0o755); err != nil {
			return nil, err
		}
	}

	return f, nil
}

func (r *posixRawFS) OpenRawFile(name string) (RawFile, error) {
	return os.OpenFile(r.path+"/"+name, os.O_RDONLY, 0o755)
}

func (r *posixRawFS) ReadDir(dir string) ([]string, error) {
	entries, err := os.ReadDir(r.path + "/" + dir)
	if err != nil {
		return nil, err
	}

	ret := make([]string, len(entries))
	for i := range entries {
		ret[i] = entries[i].Name()
	}

	return ret, nil
}
