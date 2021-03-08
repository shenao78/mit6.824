package mr

import (
	"io/ioutil"
	"os"
)

type AtomicRenameFile struct {
	*os.File
	RealName string
}

func NewAtomicRenameFile(fileName string) (*AtomicRenameFile, error) {
	file, err := ioutil.TempFile(".", "mr_*")
	if err != nil {
		return nil, err
	}

	return &AtomicRenameFile{File: file, RealName: fileName}, nil
}

func (a *AtomicRenameFile) Close() error {
	if err := os.Rename(a.File.Name(), a.RealName); err != nil {
		return err
	}

	return a.File.Close()
}
