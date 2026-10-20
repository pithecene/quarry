package lode

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
)

// fakeS3 records the request paths it receives and answers every request
// with an empty ListObjectsV2 result.
type fakeS3 struct {
	mu    sync.Mutex
	paths []string
}

func (f *fakeS3) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	f.mu.Lock()
	f.paths = append(f.paths, r.URL.Path)
	f.mu.Unlock()
	w.Header().Set("Content-Type", "application/xml")
	_, _ = w.Write([]byte(`<?xml version="1.0" encoding="UTF-8"?>` +
		`<ListBucketResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/">` +
		`<Name>test-bucket</Name><KeyCount>0</KeyCount><IsTruncated>false</IsTruncated>` +
		`</ListBucketResult>`))
}

func (f *fakeS3) requests() []string {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]string(nil), f.paths...)
}

// The read path must reach the configured S3-compatible endpoint with
// path-style addressing, exactly like the write path (issue #253).
func TestNewReadDatasetS3_HonorsEndpointAndPathStyle(t *testing.T) {
	t.Setenv("AWS_ACCESS_KEY_ID", "test")
	t.Setenv("AWS_SECRET_ACCESS_KEY", "test")
	t.Setenv("AWS_ENDPOINT_URL", "")
	t.Setenv("AWS_ENDPOINT_URL_S3", "")
	t.Setenv("AWS_CONFIG_FILE", "/dev/null")
	t.Setenv("AWS_SHARED_CREDENTIALS_FILE", "/dev/null")

	fake := &fakeS3{}
	srv := httptest.NewServer(fake)
	t.Cleanup(srv.Close)

	ds, err := NewReadDatasetS3("quarry", S3Config{
		Bucket:       "test-bucket",
		Prefix:       "data",
		Region:       "us-east-1",
		Endpoint:     srv.URL,
		UsePathStyle: true,
	})
	if err != nil {
		t.Fatalf("NewReadDatasetS3: %v", err)
	}

	// The listing result is irrelevant; only where the request went matters.
	_, _ = ds.Snapshots(t.Context())

	got := fake.requests()
	if len(got) == 0 {
		t.Fatal("no request reached the configured endpoint")
	}
	for _, path := range got {
		if !strings.HasPrefix(path, "/test-bucket") {
			t.Errorf("request path %q is not path-style (want /test-bucket/...)", path)
		}
	}
}
