package main

import (
	"io/ioutil"
	"net/url"
	"os"
	"strings"
	"time"
)

const (
	DirectoryPermissions = 0744
	FilePermissions      = 0644
)

type file struct {
	logger logger
	Path   string
}

func NewFileStage(l logger, url url.URL) *file {
	path := url.Path
	// remove the extra slash, if any
	path = strings.TrimRight(path, "/")
	path += "/"

	return &file{
		logger: l,
		Path:   path,
	}
}

func (f file) Read(ch chan<- Message) {
	// check if the source directory exists
	_, err := os.Stat(f.Path)
	f.logger.Check(FileSourceDirectoryDoesNotExist, err)
	if os.IsNotExist(err) {
		f.logger.Fatal("Unable to access source directory")
	}

	files, err := ioutil.ReadDir(f.Path)
	f.logger.Check(FileSourceDirectoryNotReadable, err)

	for _, file := range files {
		body, err := ioutil.ReadFile(f.Path + file.Name())
		f.logger.Check(FileSourceNotReadable, err)
		ch <- Message{Id: file.Name(), Body: string(body)}
	}

	close(ch)
}

func (f file) Write(ch <-chan Message, done chan<- bool) {
	f.logger.Info("Writing messages to path", f.Path)

	for {
		message, more := <-ch
		if !more {
			done <- true

			return
		}

		if message.Id == "" {
			f.logger.Warn("Skipping message without an ID", message)
			continue
		}

		backupDirSuffix := time.Now().Format("2006/01/02/15/04/")
		backupDir := f.Path
		backupDir += backupDirSuffix

		// check if the destination directory exists
		_, err := os.Stat(backupDir)
		if os.IsNotExist(err) {
			err = os.MkdirAll(backupDir, DirectoryPermissions)
			f.logger.Check(FileDestinationDirectoryCreationFailed, err)
		}

		err = os.Chmod(backupDir, DirectoryPermissions)
		f.logger.Check(FileDestinationDirectoryPermissionsFailed, err)

		filePath := backupDir + message.Id
		err = ioutil.WriteFile(filePath, []byte(message.Body), FilePermissions)
		f.logger.Check(FileDestinationWritingFailed, err)
	}
}
