package lode

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/pithecene-io/lode/lode"
	lodes3 "github.com/pithecene-io/lode/lode/s3"
)

// S3Config holds configuration for S3 storage backend.
type S3Config struct {
	// Bucket is the S3 bucket name (required).
	Bucket string
	// Prefix is the key prefix within the bucket (optional).
	Prefix string
	// Region is the AWS region (optional, uses default chain if empty).
	Region string
	// Endpoint is a custom S3 endpoint URL for S3-compatible providers
	// (e.g. Cloudflare R2, MinIO). Empty uses the default AWS endpoint.
	Endpoint string
	// UsePathStyle forces path-style addressing (bucket in path, not subdomain).
	// Required by most S3-compatible providers (R2, MinIO, etc.).
	UsePathStyle bool
}

// Validate checks that required S3 configuration is present.
func (c *S3Config) Validate() error {
	if c.Bucket == "" {
		return errors.New("S3 bucket is required")
	}
	return nil
}

// ParseS3Path parses a path in format "bucket/prefix" or "bucket".
func ParseS3Path(path string) (bucket, prefix string) {
	parts := strings.SplitN(path, "/", 2)
	bucket = parts[0]
	if len(parts) > 1 {
		prefix = parts[1]
	}
	return bucket, prefix
}

// NewLodeS3Client creates a new Lode client with S3 storage backend.
// Uses AWS SDK default credential chain (env vars, shared config, IAM role).
func NewLodeS3Client(cfg Config, s3cfg S3Config) (*LodeClient, error) {
	if err := s3cfg.Validate(); err != nil {
		return nil, err
	}

	// Load AWS config with optional region
	ctx := context.Background()
	var opts []func(*config.LoadOptions) error
	if s3cfg.Region != "" {
		opts = append(opts, config.WithRegion(s3cfg.Region))
	}

	awsConfig, err := config.LoadDefaultConfig(ctx, opts...)
	if err != nil {
		return nil, fmt.Errorf("failed to load AWS config: %w", err)
	}

	// Create S3 client with optional endpoint and path-style overrides
	var s3Opts []func(*s3.Options)
	if s3cfg.Endpoint != "" {
		endpoint := s3cfg.Endpoint
		s3Opts = append(s3Opts, func(o *s3.Options) {
			o.BaseEndpoint = &endpoint
		})
	}
	if s3cfg.UsePathStyle {
		s3Opts = append(s3Opts, func(o *s3.Options) {
			o.UsePathStyle = true
		})
	}
	s3Client := s3.NewFromConfig(awsConfig, s3Opts...)

	// Create Lode S3 store factory
	// StoreFactory is func() (Store, error)
	s3Factory := func() (lode.Store, error) {
		return lodes3.New(s3Client, lodes3.Config{
			Bucket: s3cfg.Bucket,
			Prefix: s3cfg.Prefix,
		})
	}

	// Create dataset with Hive layout
	gate := &retryGate{}
	ds, err := lode.NewDataset(lode.DatasetID(cfg.Dataset), s3Factory, writeDatasetOptions(cfg, gate)...)
	if err != nil {
		return nil, fmt.Errorf("failed to create Lode dataset: %w", err)
	}

	return newClient(ds, cfg, s3Factory, gate), nil
}
