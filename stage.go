package main

import "net/url"

type Message struct {
	Id, Body string
}

type Stage interface {
	Read(ch chan<- Message)
	Write(ch <-chan Message, done chan<- bool)
}

// is there a better way to do this? I don't know yet.
func buildStage(l logger, url url.URL) Stage {
	var stage Stage

	switch url.Scheme {
	case "file":
		stage = NewFileStage(l, url)
	case "kafka":
		stage = NewKafkaStage(l, url)
	case "s3":
		stage = NewS3Stage(l, url)
	}

	return stage
}
