package lode

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/pithecene/lode/lode"
	lodes3 "github.com/pithecene/lode/lode/s3"
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

// newS3StoreFactory builds the Lode S3 store factory used by both the write
// and read paths, so they reach the same endpoint with the same addressing.
// Uses AWS SDK default credential chain (env vars, shared config, IAM role).
func newS3StoreFactory(s3cfg S3Config) (lode.Factory[lode.RangeStore], error) {
	if err := s3cfg.Validate(); err != nil {
		return nil, err
	}

	var opts []func(*config.LoadOptions) error
	if s3cfg.Region != "" {
		opts = append(opts, config.WithRegion(s3cfg.Region))
	}

	awsConfig, err := config.LoadDefaultConfig(context.Background(), opts...)
	if err != nil {
		return nil, fmt.Errorf("failed to load AWS config: %w", err)
	}

	s3Client := s3.NewFromConfig(awsConfig, s3ClientOptions(s3cfg)...)
	return lodes3.Factory(s3Client, lodes3.Config{
		Bucket: s3cfg.Bucket,
		Prefix: s3cfg.Prefix,
	}), nil
}

// s3ClientOptions applies the endpoint and path-style overrides for
// S3-compatible providers.
//
// It also silences the SDK's warning that a response carried no checksum
// it supports. Stores such as R2 and MinIO often send none, and the SDK
// logs the warning on every such read, flooding stderr. Validation is
// unchanged: the SDK still validates every checksum a store sends.
func s3ClientOptions(s3cfg S3Config) []func(*s3.Options) {
	s3Opts := []func(*s3.Options){func(o *s3.Options) {
		o.DisableLogOutputChecksumValidationSkipped = true
	}}
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
	return s3Opts
}

// NewLodeS3Client creates a new Lode client with S3 storage backend.
// Uses AWS SDK default credential chain (env vars, shared config, IAM role).
// Opening reads the dataset's head under ctx.
func NewLodeS3Client(ctx context.Context, cfg Config, s3cfg S3Config) (*LodeClient, error) {
	s3Factory, err := newS3StoreFactory(s3cfg)
	if err != nil {
		return nil, err
	}
	c, err := openClient(ctx, cfg, s3Factory)
	if err != nil {
		return nil, fmt.Errorf("failed to create Lode dataset: %w", err)
	}
	return c, nil
}

// NewStoreFactory returns the store factory for a storage backend, built
// exactly as the write path builds it ("fs": path is a directory; "s3": path
// is bucket/prefix and s3cfg supplies region, endpoint, and addressing).
func NewStoreFactory(backend, path string, s3cfg S3Config) (lode.Factory[lode.RangeStore], error) {
	switch backend {
	case "fs":
		return lode.FS(path), nil
	case "s3":
		s3cfg.Bucket, s3cfg.Prefix = ParseS3Path(path)
		return newS3StoreFactory(s3cfg)
	default:
		return nil, fmt.Errorf("unsupported storage-backend: %s (must be fs or s3)", backend)
	}
}
