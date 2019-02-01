package main

import (
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3manager"
	"net/url"
	"strings"
	"time"
)

const (
	DefaultAwsRegion = "us-east-1"
)

type s3Stage struct {
	logger logger
	svc    s3.S3
	Bucket string
	Path   string
}

func NewS3Stage(l logger, url url.URL) *s3Stage {
	bucket := url.Hostname()
	region := url.Port()

	if bucket == "" {
		l.Exit(S3BucketInvalid, "Invalid bucket name")
	}

	if region == "" {
		region = DefaultAwsRegion
	}

	// remove the leading slash
	path := strings.TrimLeft(url.Path, "/")
	// remove the trailing slash, if any
	path = strings.TrimRight(path, "/")
	path += "/"

	sess, err := session.NewSession(&aws.Config{Region: aws.String(region)})
	l.Check(S3ConnectionFailed, err)
	svc := s3.New(sess)

	return &s3Stage{
		logger: l,
		svc:    *svc,
		Bucket: bucket,
		Path:   path,
	}
}

func (s s3Stage) Read(ch chan<- Message) {
	query := &s3.ListObjectsV2Input{
		Bucket: aws.String(s.Bucket),
		Prefix: &s.Path,
	}
	truncatedListing := true

	for truncatedListing {
		resp, err := s.svc.ListObjectsV2(query)
		s.logger.Check(S3ErrorListingObjects, err)
		for _, key := range resp.Contents {
			keyName := strings.Replace(*key.Key, s.Path, "", 1)

			// ignore directory entries
			if strings.Contains(keyName, "/") {
				s.logger.Warn("Skipping directory", keyName)
				continue
			}

			buff := &aws.WriteAtBuffer{}
			downloader := s3manager.NewDownloaderWithClient(&s.svc)
			_, err := downloader.Download(buff, &s3.GetObjectInput{
				Bucket: &s.Bucket,
				Key:    key.Key,
			})
			s.logger.Check(S3ReadFileFailed, err)
			s.logger.Info(string(buff.Bytes()))
			ch <- Message{Id: keyName, Body: string(buff.Bytes())}
			s.logger.Check(S3ReadFileFailed, err)
		}

		query.ContinuationToken = resp.NextContinuationToken
		truncatedListing = *resp.IsTruncated
	}

	close(ch)
}

func (s s3Stage) Write(ch <-chan Message, done chan<- bool) {
	s.logger.Info("Writing messages to S3 bucket", s.Bucket)

	for {
		message, more := <-ch
		if !more {
			done <- true

			return
		}

		if message.Id == "" {
			s.logger.Warn("Skipping message without an ID", message)
			continue
		}

		backupDirSuffix := time.Now().Format("2006/01/02/15/04/")
		backupDir := s.Path
		backupDir += backupDirSuffix
		filePath := backupDir + message.Id

		_, err := s.svc.PutObject(&s3.PutObjectInput{
			Bucket:               aws.String(s.Bucket),
			Key:                  aws.String(filePath),
			ACL:                  aws.String("private"),
			Body:                 strings.NewReader(message.Body),
			ContentDisposition:   aws.String("attachment"),
			ServerSideEncryption: aws.String("AES256"),
		})
		s.logger.Check(FileDestinationWritingFailed, err)
	}
}
