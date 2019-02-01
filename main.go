package main

import (
	"flag"
	"fmt"
	"github.com/owahab/kafkacp/lib"
	"net/url"
	"os"
)

const Version = "0.1.0"

const (
	Success                    = 0
	ArgumentsTooFew            = 1
	ArgumentInvalidSource      = 11
	ArgumentInvalidDestination = 12
)

func main() {
	flag.Usage = func() {
		_, _ = fmt.Fprintf(os.Stdout, `usage: %s <source> <destination>

Source and destination can be any of the following:
  kafka://host/topic					Connect to the given Kafka host and the topic
  file:///tmp/backups/					Use the local path /tmp/backups
  s3Stage://AWS_KEY:AWS_SECRET@BUCKET:AWS_REGION/path	S3 URL with optional credentials

Flags:
  -h	Display usage and exit
`, os.Args[0])
		flag.PrintDefaults()
	}

	verbose := flag.Bool("d", false, "Turn verbose output on")
	displayVersion := flag.Bool("v", false, "Display version and exit")
	flag.Parse()
	l := lib.NewLogger(*verbose)

	if *displayVersion {
		l.Exit(Success, os.Args[0], "version", Version)
	}

	if len(flag.Args()) < 2 {
		flag.Usage()
		os.Exit(ArgumentsTooFew)
	}

	source := flag.Arg(0)
	destination := flag.Arg(1)

	sourceUrl, err := url.Parse(source)
	if err != nil {
		l.Exit(ArgumentInvalidSource, "Unable to parse sources URL")
	}

	destinationUrl, err := url.Parse(destination)
	if err != nil {
		l.Exit(ArgumentInvalidDestination, "Unable to parse destinations URL")
	}

	l.Info("Kafka snapshot started!")

	messages := make(chan lib.Message)
	done := make(chan bool)

	// unbuffered channel, we need to start the writer first
	go lib.BuildStage(*l, *destinationUrl).Write(messages, done)
	go lib.BuildStage(*l, *sourceUrl).Read(messages)

	// wait for the done signal
	<-done
}
