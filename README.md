# kafkacp

Copy a Kafka topic from/to another topic or S3 or local directory.

# Usage

    $ kafkacp <source> <destination>
    
Source and destination can be any stage URL.

## Stages

### Kafka

In order to use Kafka as a source or destination, use the following URL scheme:

    kafka://host/topic

### File

Use a file URL to represent a source or destination, a trailing slash is optional.

    file:///tmp/backups/
    
### S3

Use an S3 URL with optional credentials. For example:

    s3://AWS_KEY:AWS_SECRET@BUCKET:AWS_REGION/path


# Limitations

## Kafka

1. No support for authentication, yet.
1. Messages should have a string key.
1. Can only copy the entire topic.