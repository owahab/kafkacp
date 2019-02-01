package lib

import (
	"fmt"
	"log"
	"os"
)

const (
	Success                                   = 0
	FileSourceDirectoryDoesNotExist           = 20
	FileSourceDirectoryNotReadable            = 21
	FileSourceNotReadable                     = 22
	FileDestinationDirectoryCreationFailed    = 30
	FileDestinationDirectoryPermissionsFailed = 31
	FileDestinationWritingFailed              = 32
	KafkaConnectionFailed                     = 40
	KafkaTopicSubscriptionFailed              = 41
	KafkaSendingFailed                        = 50
	S3BucketInvalid                           = 60
	S3ConnectionFailed                        = 61
	S3ErrorListingObjects                     = 62
	S3ReadFileFailed                          = 63
)

type logger struct {
	Verbose bool
}

func NewLogger(verbose bool) *logger {
	return &logger{Verbose: verbose}
}

func (l logger) Check(code int, err error) {
	if err != nil {
		l.Exit(code, "Error ", code, err)
	}
}

func (l logger) Exit(code int, message ...interface{}) {
	_, _ = fmt.Fprintln(os.Stderr, message...)
	os.Exit(code)
}

func (l logger) Info(message ...interface{}) {
	if l.Verbose {
		log.Println(message...)
	}
}

func (l logger) Warn(message ...interface{}) {
	_, _ = fmt.Fprintln(os.Stderr, message...)
}

func (l logger) Fatal(message ...interface{}) {
	log.Fatalln(message...)
}
