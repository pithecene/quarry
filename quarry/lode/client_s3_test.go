package lode

import (
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/service/s3"

	"github.com/pithecene/quarry/internal/testutil"
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
func TestNewStoreFactoryS3_ReadHonorsEndpointAndPathStyle(t *testing.T) {
	testutil.Serial(t, "calls t.Setenv, which changes the process environment")
	t.Setenv("AWS_ACCESS_KEY_ID", "test")
	t.Setenv("AWS_SECRET_ACCESS_KEY", "test")
	t.Setenv("AWS_ENDPOINT_URL", "")
	t.Setenv("AWS_ENDPOINT_URL_S3", "")
	t.Setenv("AWS_CONFIG_FILE", "/dev/null")
	t.Setenv("AWS_SHARED_CREDENTIALS_FILE", "/dev/null")

	fake := &fakeS3{}
	srv := httptest.NewServer(fake)
	t.Cleanup(srv.Close)

	factory, err := NewStoreFactory("s3", "test-bucket/data", S3Config{
		Region:       "us-east-1",
		Endpoint:     srv.URL,
		UsePathStyle: true,
	})
	if err != nil {
		t.Fatalf("NewStoreFactory: %v", err)
	}

	// The query result is irrelevant; only where the requests went matters.
	_, _ = QueryLatestMetrics(t.Context(), factory, "quarry", "", "")

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

// A read from a store that sends no checksum must not log a warning: the
// SDK otherwise writes one to stderr for every such response.
func TestS3ClientOptions_SilenceSkippedChecksumWarning(t *testing.T) {
	t.Parallel()
	for name, cfg := range map[string]S3Config{
		"AWS":           {},
		"S3-compatible": {Endpoint: "http://127.0.0.1:9000", UsePathStyle: true},
	} {
		t.Run(name, func(t *testing.T) {
			var o s3.Options
			for _, apply := range s3ClientOptions(cfg) {
				apply(&o)
			}
			if !o.DisableLogOutputChecksumValidationSkipped {
				t.Error("the skipped-checksum warning is not silenced")
			}
		})
	}
}

// stallingS3 holds its answer to the first PUT past the attempt bound, as a
// store that accepts a request and stalls does, and answers later PUTs at once.
type stallingS3 struct {
	mu    sync.Mutex
	puts  int
	stall time.Duration
}

func (s *stallingS3) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	_, _ = io.Copy(io.Discard, r.Body)
	s.mu.Lock()
	s.puts++
	first := s.puts == 1
	s.mu.Unlock()
	if first {
		select {
		case <-time.After(s.stall):
		case <-r.Context().Done():
			return
		}
	}
	w.Header().Set("ETag", `"etag"`)
	w.WriteHeader(http.StatusOK)
}

// A store request whose answer stalls fails its attempt within the attempt
// bound, and the SDK sends it again (issue #389).
func TestNewStoreFactoryS3_BoundsEachAttempt(t *testing.T) {
	testutil.Serial(t, "calls t.Setenv, which changes the process environment")
	t.Setenv("AWS_ACCESS_KEY_ID", "test")
	t.Setenv("AWS_SECRET_ACCESS_KEY", "test")
	t.Setenv("AWS_ENDPOINT_URL", "")
	t.Setenv("AWS_ENDPOINT_URL_S3", "")
	t.Setenv("AWS_CONFIG_FILE", "/dev/null")
	t.Setenv("AWS_SHARED_CREDENTIALS_FILE", "/dev/null")

	fake := &stallingS3{stall: 3 * time.Second}
	srv := httptest.NewServer(fake)
	t.Cleanup(srv.Close)

	factory, err := NewStoreFactory("s3", "test-bucket/data", S3Config{
		Region:         "us-east-1",
		Endpoint:       srv.URL,
		UsePathStyle:   true,
		AttemptTimeout: 200 * time.Millisecond,
	})
	if err != nil {
		t.Fatalf("NewStoreFactory: %v", err)
	}
	store, err := factory()
	if err != nil {
		t.Fatalf("store: %v", err)
	}

	start := time.Now()
	if err := store.Put(t.Context(), "events/one", strings.NewReader("payload")); err != nil {
		t.Fatalf("Put: %v", err)
	}
	if elapsed := time.Since(start); elapsed >= fake.stall {
		t.Errorf("Put took %s, want it bounded below the %s stall", elapsed, fake.stall)
	}
	fake.mu.Lock()
	defer fake.mu.Unlock()
	if fake.puts != 2 {
		t.Errorf("store received %d PUTs, want 2: the stalled attempt and its retry", fake.puts)
	}
}

func TestS3Config_DefaultAttemptTimeout(t *testing.T) {
	t.Parallel()
	if got := (S3Config{}).attemptTimeout(); got != DefaultS3AttemptTimeout {
		t.Errorf("attemptTimeout() = %s, want %s", got, DefaultS3AttemptTimeout)
	}
	if got := (S3Config{AttemptTimeout: time.Second}).attemptTimeout(); got != time.Second {
		t.Errorf("attemptTimeout() = %s, want 1s", got)
	}
}
