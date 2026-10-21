package ipc

import (
	"bytes"
	"io"
	"testing"
	"testing/iotest"

	"github.com/vmihailenco/msgpack/v5"

	"github.com/pithecene/quarry/types"
)

// frameTypeProbe is the old approach: unmarshal the entire payload into a
// struct just to read the "type" field. Kept here as baseline for benchmarks.
type frameTypeProbe struct {
	Type string `msgpack:"type"`
}

// probeFrameTypeOld is the pre-#205 implementation that does a full
// msgpack.Unmarshal to extract just the type field.
func probeFrameTypeOld(payload []byte) (string, error) {
	var probe frameTypeProbe
	if err := msgpack.Unmarshal(payload, &probe); err != nil {
		return "", err
	}
	return probe.Type, nil
}

// buildEventStream encodes n event envelopes into a contiguous byte buffer.
func buildEventStream(b *testing.B, n int) []byte {
	b.Helper()
	var buf bytes.Buffer
	for i := range n {
		env := &types.EventEnvelope{
			ContractVersion: types.Version,
			EventID:         "evt-001",
			RunID:           "run-001",
			Seq:             int64(i + 1),
			Type:            types.EventTypeItem,
			Ts:              "2024-01-15T10:00:00Z",
			Attempt:         1,
			Payload:         map[string]any{"item_type": "product", "data": map[string]any{"name": "widget"}},
		}
		frame, err := encodeEventFrame(env)
		if err != nil {
			b.Fatalf("encodeEventFrame: %v", err)
		}
		buf.Write(frame)
	}
	return buf.Bytes()
}

// buildMixedStream encodes a realistic mixed workload: events, artifact
// chunks, file writes, and a terminal event.
func buildMixedStream(b *testing.B) []byte {
	b.Helper()
	var buf bytes.Buffer

	// 5 item events
	for i := range 5 {
		env := &types.EventEnvelope{
			ContractVersion: types.Version,
			EventID:         "evt-item",
			RunID:           "run-001",
			Seq:             int64(i + 1),
			Type:            types.EventTypeItem,
			Ts:              "2024-01-15T10:00:00Z",
			Attempt:         1,
			Payload:         map[string]any{"item_type": "product", "data": map[string]any{"name": "widget"}},
		}
		frame, _ := encodeEventFrame(env)
		buf.Write(frame)
	}

	// 2 artifact chunks
	for i := range 2 {
		chunk := &types.ArtifactChunkFrame{
			Type:       ArtifactChunkType,
			ArtifactID: "art-001",
			Seq:        int64(i + 1),
			IsLast:     i == 1,
			Data:       bytes.Repeat([]byte("x"), 4096),
		}
		frame, _ := encodeArtifactChunkFrame(chunk)
		buf.Write(frame)
	}

	// 1 file write
	fw := &types.FileWriteFrame{
		Type:        FileWriteType,
		WriteID:     1,
		Filename:    "image.png",
		ContentType: "image/png",
		Data:        bytes.Repeat([]byte("p"), 2048),
	}
	fwFrame, _ := encodeFileWriteFrame(fw)
	buf.Write(fwFrame)

	// Terminal event
	terminal := &types.EventEnvelope{
		ContractVersion: types.Version,
		EventID:         "evt-terminal",
		RunID:           "run-001",
		Seq:             6,
		Type:            types.EventTypeRunComplete,
		Ts:              "2024-01-15T10:00:05Z",
		Attempt:         1,
		Payload:         map[string]any{},
	}
	frame, _ := encodeEventFrame(terminal)
	buf.Write(frame)

	return buf.Bytes()
}

// --- Type probe benchmarks ---

// BenchmarkProbeFrameType_Old measures the pre-#205 approach: full
// msgpack.Unmarshal into a struct to extract one field.
func BenchmarkProbeFrameType_Old(b *testing.B) {
	env := &types.EventEnvelope{
		ContractVersion: types.Version,
		EventID:         "evt-001",
		RunID:           "run-001",
		Seq:             1,
		Type:            types.EventTypeItem,
		Ts:              "2024-01-15T10:00:00Z",
		Attempt:         1,
		Payload:         map[string]any{"item_type": "product", "data": map[string]any{"name": "widget", "price": 9.99}},
	}
	payload, err := msgpack.Marshal(env)
	if err != nil {
		b.Fatal(err)
	}

	b.ResetTimer()
	b.ReportAllocs()
	for range b.N {
		typ, err := probeFrameTypeOld(payload)
		if err != nil {
			b.Fatal(err)
		}
		if typ != string(types.EventTypeItem) {
			b.Fatalf("got %q", typ)
		}
	}
}

// BenchmarkProbeFrameType_New measures the #205 approach: streaming
// msgpack decoder that skips non-"type" fields without allocating.
func BenchmarkProbeFrameType_New(b *testing.B) {
	env := &types.EventEnvelope{
		ContractVersion: types.Version,
		EventID:         "evt-001",
		RunID:           "run-001",
		Seq:             1,
		Type:            types.EventTypeItem,
		Ts:              "2024-01-15T10:00:00Z",
		Attempt:         1,
		Payload:         map[string]any{"item_type": "product", "data": map[string]any{"name": "widget", "price": 9.99}},
	}
	payload, err := msgpack.Marshal(env)
	if err != nil {
		b.Fatal(err)
	}

	b.ResetTimer()
	b.ReportAllocs()
	for range b.N {
		typ, err := probeFrameType(payload)
		if err != nil {
			b.Fatal(err)
		}
		if typ != string(types.EventTypeItem) {
			b.Fatalf("got %q", typ)
		}
	}
}

// BenchmarkProbeFrameType_ArtifactChunk exercises probing on artifact_chunk
// payloads where "type" is typically the first field.
func BenchmarkProbeFrameType_ArtifactChunk(b *testing.B) {
	chunk := &types.ArtifactChunkFrame{
		Type:       ArtifactChunkType,
		ArtifactID: "art-001",
		Seq:        1,
		IsLast:     false,
		Data:       bytes.Repeat([]byte("x"), 4096),
	}
	payload, err := msgpack.Marshal(chunk)
	if err != nil {
		b.Fatal(err)
	}

	b.Run("old", func(b *testing.B) {
		b.ReportAllocs()
		for range b.N {
			typ, err := probeFrameTypeOld(payload)
			if err != nil {
				b.Fatal(err)
			}
			if typ != ArtifactChunkType {
				b.Fatalf("got %q", typ)
			}
		}
	})

	b.Run("new", func(b *testing.B) {
		b.ReportAllocs()
		for range b.N {
			typ, err := probeFrameType(payload)
			if err != nil {
				b.Fatal(err)
			}
			if typ != ArtifactChunkType {
				b.Fatalf("got %q", typ)
			}
		}
	})
}

// --- DecodeFrame benchmarks (type probe + full decode combined) ---

// BenchmarkDecodeFrame_Event measures full DecodeFrame throughput for events.
// This exercises probeFrameType + DecodeEventEnvelope.
func BenchmarkDecodeFrame_Event(b *testing.B) {
	env := &types.EventEnvelope{
		ContractVersion: types.Version,
		EventID:         "evt-001",
		RunID:           "run-001",
		Seq:             1,
		Type:            types.EventTypeItem,
		Ts:              "2024-01-15T10:00:00Z",
		Attempt:         1,
		Payload:         map[string]any{"item_type": "product", "data": map[string]any{"name": "widget", "price": 9.99}},
	}
	payload, err := msgpack.Marshal(env)
	if err != nil {
		b.Fatal(err)
	}

	b.ResetTimer()
	b.ReportAllocs()
	for range b.N {
		result, err := DecodeFrame(payload)
		if err != nil {
			b.Fatal(err)
		}
		if _, ok := result.(*types.EventEnvelope); !ok {
			b.Fatalf("got %T", result)
		}
	}
}

// --- FrameDecoder + ReadFrame benchmarks ---

// BenchmarkReadFrame_BufferedReader measures ReadFrame with the current
// bufio.Reader wrapping (new behavior).
func BenchmarkReadFrame_BufferedReader(b *testing.B) {
	data := buildEventStream(b, 100)

	b.ResetTimer()
	b.ReportAllocs()
	for range b.N {
		decoder := NewFrameDecoder(bytes.NewReader(data))
		for {
			_, err := decoder.ReadFrame()
			if err == io.EOF {
				break
			}
			if err != nil {
				b.Fatal(err)
			}
		}
	}
}

// BenchmarkReadFrame_OneByteReader measures ReadFrame through
// iotest.OneByteReader, simulating worst-case small-read behavior
// (e.g., unbuffered pipe returning 1 byte per read(2)).
// With bufio.Reader, the decoder batches these into larger reads.
// Without it, each io.ReadFull call would issue many syscalls.
func BenchmarkReadFrame_OneByteReader(b *testing.B) {
	data := buildEventStream(b, 20)

	b.ResetTimer()
	b.ReportAllocs()
	for range b.N {
		reader := iotest.OneByteReader(bytes.NewReader(data))
		decoder := NewFrameDecoder(reader)
		for {
			_, err := decoder.ReadFrame()
			if err == io.EOF {
				break
			}
			if err != nil {
				b.Fatal(err)
			}
		}
	}
}

// BenchmarkReadFrame_MixedStream measures ReadFrame + DecodeFrame on a
// realistic mixed workload (events + chunks + file_write + terminal).
func BenchmarkReadFrame_MixedStream(b *testing.B) {
	data := buildMixedStream(b)

	b.ResetTimer()
	b.ReportAllocs()
	for range b.N {
		decoder := NewFrameDecoder(bytes.NewReader(data))
		for {
			payload, err := decoder.ReadFrame()
			if err == io.EOF {
				break
			}
			if err != nil {
				b.Fatal(err)
			}
			if _, err := DecodeFrame(payload); err != nil {
				b.Fatal(err)
			}
		}
	}
}
