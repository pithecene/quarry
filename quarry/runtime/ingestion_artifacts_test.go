package runtime

import (
	"bytes"
	"context"
	"encoding/binary"
	"io"
	"strings"
	"testing"
	"time"

	"github.com/vmihailenco/msgpack/v5"

	"github.com/pithecene/quarry/lode"
	"github.com/pithecene/quarry/log"
	"github.com/pithecene/quarry/policy"
	"github.com/pithecene/quarry/reader"
	"github.com/pithecene/quarry/types"
)

// frameOf frames one msgpack-encoded value.
func frameOf(t *testing.T, v any) []byte {
	t.Helper()
	payload, err := msgpack.Marshal(v)
	if err != nil {
		t.Fatalf("Marshal: %v", err)
	}
	return encodeFrame(payload)
}

// artifactEvent is the artifact event a script emits for artifactID.
func artifactEvent(seq int64, artifactID string, size int64) *types.EventEnvelope {
	return &types.EventEnvelope{
		ContractVersion: types.ContractVersion, EventID: "evt-" + artifactID, RunID: "run-123", Seq: seq,
		Type: types.EventTypeArtifact, Ts: "2024-01-01T00:00:00Z", Attempt: 1,
		Payload: map[string]any{"artifact_id": artifactID, "name": artifactID + ".bin", "content_type": "application/octet-stream", "size_bytes": size},
	}
}

// startOf is a start time within the run's partition day.
func startOf(day string) time.Time {
	t, _ := time.Parse(time.DateOnly, day)
	return t.Add(time.Hour)
}

func terminalEvent(seq int64) *types.EventEnvelope {
	return &types.EventEnvelope{
		ContractVersion: types.ContractVersion, EventID: "evt-end", RunID: "run-123", Seq: seq,
		Type: types.EventTypeRunComplete, Ts: "2024-01-01T00:00:01Z", Payload: map[string]any{}, Attempt: 1,
	}
}

// decodeAcks decodes every artifact_range_ack frame the engine wrote.
func decodeAcks(t *testing.T, data []byte) []types.ArtifactRangeAckFrame {
	t.Helper()
	var acks []types.ArtifactRangeAckFrame
	for len(data) > 0 {
		n := binary.BigEndian.Uint32(data[:4])
		var ack types.ArtifactRangeAckFrame
		if err := msgpack.Unmarshal(data[4:4+n], &ack); err != nil {
			t.Fatalf("decode ack: %v", err)
		}
		acks = append(acks, ack)
		data = data[4+n:]
	}
	return acks
}

// recordingPolicy keeps the artifact events it ingests.
type recordingPolicy struct {
	policy.Policy
	artifacts []*types.EventEnvelope
}

func (p *recordingPolicy) IngestEvent(ctx context.Context, envelope *types.EventEnvelope) error {
	if envelope.Type == types.EventTypeArtifact {
		p.artifacts = append(p.artifacts, envelope)
	}
	return p.Policy.IngestEvent(ctx, envelope)
}

func TestIngestionEngine_Artifact_OpenRangesCommit(t *testing.T) {
	t.Parallel()
	runMeta := &types.RunMeta{RunID: "run-123", Attempt: 1}
	f := newArtifactStoreFixture(t)
	data := []byte("0123456789abcdef")

	var buf bytes.Buffer
	buf.Write(frameOf(t, openFrame("a1", int64(len(data)))))
	buf.Write(frameOf(t, rangeFrame("a1", 1, 10, data[10:])))
	buf.Write(frameOf(t, rangeFrame("a1", 2, 0, data[:10])))
	buf.Write(encodeEventFrame(artifactEvent(1, "a1", int64(len(data)))))
	buf.Write(encodeEventFrame(terminalEvent(2)))

	var ackBuf bytes.Buffer
	pol := &recordingPolicy{Policy: policy.NewNoopPolicy()}
	engine := NewIngestionEngine(&buf, pol, NewArtifactManager(f.client), nil, log.NewLogger(runMeta), runMeta, nil, nil, &ackBuf)
	if err := engine.Run(t.Context()); err != nil {
		t.Fatalf("Run: %v", err)
	}

	acks := decodeAcks(t, ackBuf.Bytes())
	if len(acks) != 2 || acks[0].RangeID != 1 || !acks[0].OK || acks[1].RangeID != 2 || !acks[1].OK {
		t.Fatalf("acks = %+v; want range 1 and 2 committed", acks)
	}
	if len(pol.artifacts) != 1 || pol.artifacts[0].Storage == nil || pol.artifacts[0].Storage.Kind != types.ArtifactStorageVolume {
		t.Fatalf("policy received %+v; want one artifact event carrying volume storage", pol.artifacts)
	}
	// The record the storage client writes names the Volume the reader opens.
	storage := pol.artifacts[0].Storage
	if err := f.client.WriteEvents(t.Context(), f.cfg.Dataset, f.cfg.RunID, pol.artifacts); err != nil {
		t.Fatalf("WriteEvents: %v", err)
	}
	if err := f.client.WriteStartRecord(t.Context(), runMeta, startOf(f.cfg.Day)); err != nil {
		t.Fatalf("WriteStartRecord: %v", err)
	}
	r, err := reader.New(f.cfg.Dataset, f.factory)
	if err != nil {
		t.Fatalf("reader: %v", err)
	}
	run, err := r.FindRun(t.Context(), reader.Selector{Source: f.cfg.Source, Category: f.cfg.Category, Day: f.cfg.Day}, f.cfg.RunID)
	if err != nil {
		t.Fatalf("FindRun: %v", err)
	}
	a, rd, err := r.OpenArtifact(t.Context(), run, "a1")
	if err != nil {
		t.Fatalf("OpenArtifact: %v", err)
	}
	if got, err := io.ReadAll(rd); err != nil || !bytes.Equal(got, data) || a.VolumeSnapshotID != storage.VolumeSnapshotID {
		t.Fatalf("read back %q, %v (snapshot %s); want %q from snapshot %s", got, err, a.VolumeSnapshotID, data, storage.VolumeSnapshotID)
	}
}

func TestIngestionEngine_Artifact_EmptyArtifact(t *testing.T) {
	t.Parallel()
	runMeta := &types.RunMeta{RunID: "run-123", Attempt: 1}
	f := newArtifactStoreFixture(t)
	var buf bytes.Buffer
	buf.Write(frameOf(t, openFrame("e1", 0)))
	buf.Write(encodeEventFrame(artifactEvent(1, "e1", 0)))
	buf.Write(encodeEventFrame(terminalEvent(2)))
	pol := &recordingPolicy{Policy: policy.NewNoopPolicy()}
	engine := NewIngestionEngine(&buf, pol, NewArtifactManager(f.client), nil, log.NewLogger(runMeta), runMeta, nil, nil, nil)
	if err := engine.Run(t.Context()); err != nil {
		t.Fatalf("Run: %v", err)
	}
	if len(pol.artifacts) != 1 || pol.artifacts[0].Storage == nil || pol.artifacts[0].Storage.Kind != types.ArtifactStorageEmpty {
		t.Fatalf("policy received %+v; want one artifact event with empty storage", pol.artifacts)
	}
}

// A refused range is acknowledged with its error, and ingestion continues.
func TestIngestionEngine_Artifact_RefusedRangeAcksAndContinues(t *testing.T) {
	t.Parallel()
	runMeta := &types.RunMeta{RunID: "run-123", Attempt: 1}
	f := newArtifactStoreFixture(t)
	var buf bytes.Buffer
	buf.Write(frameOf(t, openFrame("a1", 4)))
	buf.Write(frameOf(t, rangeFrame("nope", 1, 0, []byte("x"))))  // not open
	buf.Write(frameOf(t, rangeFrame("a1", 2, 3, []byte("xx"))))   // past the size
	buf.Write(frameOf(t, rangeFrame("a1", 3, 0, []byte("abcd")))) // committed
	buf.Write(encodeEventFrame(artifactEvent(1, "a1", 4)))
	buf.Write(encodeEventFrame(terminalEvent(2)))
	var ackBuf bytes.Buffer
	engine := NewIngestionEngine(&buf, policy.NewNoopPolicy(), NewArtifactManager(f.client), nil, log.NewLogger(runMeta), runMeta, nil, nil, &ackBuf)
	if err := engine.Run(t.Context()); err != nil {
		t.Fatalf("Run: %v", err)
	}
	acks := decodeAcks(t, ackBuf.Bytes())
	if len(acks) != 3 {
		t.Fatalf("acks = %+v; want three", acks)
	}
	if acks[0].OK || acks[0].Error == nil || !strings.Contains(*acks[0].Error, "not open") {
		t.Fatalf("ack 1 = %+v; want a refusal for an artifact that is not open", acks[0])
	}
	if acks[1].OK || acks[1].Error == nil || !strings.Contains(*acks[1].Error, "refused") {
		t.Fatalf("ack 2 = %+v; want a refusal for a range past the size", acks[1])
	}
	if !acks[2].OK {
		t.Fatalf("ack 3 = %+v; want committed", acks[2])
	}
}

func TestIngestionEngine_Artifact_StreamErrors(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name   string
		store  func(t *testing.T) lode.ArtifactStore
		frames func(t *testing.T) []byte
		want   string
	}{
		{"open without a store", func(*testing.T) lode.ArtifactStore { return nil }, func(t *testing.T) []byte {
			return frameOf(t, openFrame("a1", 1))
		}, "no ArtifactStore"},
		{"open above the maximum", func(t *testing.T) lode.ArtifactStore { return newArtifactStoreFixture(t).client }, func(t *testing.T) []byte {
			return frameOf(t, openFrame("a1", MaxArtifactSize+1))
		}, "exceeds max"},
		{"event without an open", func(t *testing.T) lode.ArtifactStore { return newArtifactStoreFixture(t).client }, func(t *testing.T) []byte {
			return encodeEventFrame(artifactEvent(1, "a1", 1))
		}, "not open"},
		{"event before the ranges cover", func(t *testing.T) lode.ArtifactStore { return newArtifactStoreFixture(t).client }, func(t *testing.T) []byte {
			return bytes.Join([][]byte{frameOf(t, openFrame("a1", 4)), frameOf(t, rangeFrame("a1", 1, 0, []byte("ab"))), encodeEventFrame(artifactEvent(1, "a1", 4))}, nil)
		}, "do not cover"},
		{"event with another size", func(t *testing.T) lode.ArtifactStore { return newArtifactStoreFixture(t).client }, func(t *testing.T) []byte {
			return bytes.Join([][]byte{frameOf(t, openFrame("a1", 4)), encodeEventFrame(artifactEvent(1, "a1", 5))}, nil)
		}, "size mismatch"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			runMeta := &types.RunMeta{RunID: "run-123", Attempt: 1}
			engine := NewIngestionEngine(bytes.NewReader(tc.frames(t)), policy.NewNoopPolicy(), NewArtifactManager(tc.store(t)), nil, log.NewLogger(runMeta), runMeta, nil, nil, nil)
			err := engine.Run(t.Context())
			if !IsStreamError(err) || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("Run = %v; want a stream error containing %q", err, tc.want)
			}
		})
	}
}

// A range after the terminal event is refused with an ack, as a file write is.
func TestIngestionEngine_Artifact_RangeAfterTerminal(t *testing.T) {
	t.Parallel()
	runMeta := &types.RunMeta{RunID: "run-123", Attempt: 1}
	f := newArtifactStoreFixture(t)
	var buf bytes.Buffer
	buf.Write(frameOf(t, openFrame("a1", 1)))
	buf.Write(encodeEventFrame(terminalEvent(1)))
	buf.Write(frameOf(t, rangeFrame("a1", 1, 0, []byte("x"))))
	var ackBuf bytes.Buffer
	engine := NewIngestionEngine(&buf, policy.NewNoopPolicy(), NewArtifactManager(f.client), nil, log.NewLogger(runMeta), runMeta, nil, nil, &ackBuf)
	if err := engine.Run(t.Context()); err != nil {
		t.Fatalf("Run: %v", err)
	}
	acks := decodeAcks(t, ackBuf.Bytes())
	if len(acks) != 1 || acks[0].OK || acks[0].Error == nil || !strings.Contains(*acks[0].Error, "terminated") {
		t.Fatalf("acks = %+v; want one refusal after the terminal event", acks)
	}
	if ids := engine.artifacts.OrphanIDs(); len(ids) != 1 || ids[0] != "a1" {
		t.Fatalf("OrphanIDs = %v; want a1, still open without an event", ids)
	}
}
