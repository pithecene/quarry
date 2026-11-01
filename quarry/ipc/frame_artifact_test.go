package ipc

import (
	"bytes"
	"errors"
	"testing"

	"github.com/vmihailenco/msgpack/v5"

	"github.com/pithecene/quarry/types"
)

// decodeOne frames and decodes one msgpack-encoded frame through DecodeFrame.
func decodeOne(t *testing.T, frame any) any {
	t.Helper()
	payload, err := msgpack.Marshal(frame)
	if err != nil {
		t.Fatalf("Marshal: %v", err)
	}
	decoder := NewFrameDecoder(bytes.NewReader(EncodeFrame(payload)))
	read, err := decoder.ReadFrame()
	if err != nil {
		t.Fatalf("ReadFrame: %v", err)
	}
	decoded, err := DecodeFrame(read)
	if err != nil {
		t.Fatalf("DecodeFrame: %v", err)
	}
	return decoded
}

func TestDecodeFrame_ArtifactOpen(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name  string
		frame types.ArtifactOpenFrame
	}{
		{"with expected digest", types.ArtifactOpenFrame{Type: ArtifactOpenType, ArtifactID: "a1", SizeBytes: 595_847_268, ExpectedSHA256: "1ded"}},
		{"without expected digest", types.ArtifactOpenFrame{Type: ArtifactOpenType, ArtifactID: "a2", SizeBytes: 0}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			decoded, ok := decodeOne(t, tc.frame).(*types.ArtifactOpenFrame)
			if !ok || *decoded != tc.frame {
				t.Fatalf("DecodeFrame = %+v, %v; want %+v", decoded, ok, tc.frame)
			}
		})
	}
}

func TestDecodeFrame_ArtifactRange(t *testing.T) {
	t.Parallel()
	frame := types.ArtifactRangeFrame{Type: ArtifactRangeType, ArtifactID: "a1", RangeID: 3, Offset: 8 * 1024 * 1024, Data: []byte("second range")}
	decoded, ok := decodeOne(t, frame).(*types.ArtifactRangeFrame)
	if !ok {
		t.Fatalf("DecodeFrame returned %T, want *types.ArtifactRangeFrame", decoded)
	}
	if decoded.ArtifactID != "a1" || decoded.RangeID != 3 || decoded.Offset != frame.Offset || !bytes.Equal(decoded.Data, frame.Data) {
		t.Fatalf("DecodeFrame = %+v; want %+v", decoded, frame)
	}
}

func TestEncodeDecodeArtifactRangeAck(t *testing.T) {
	t.Parallel()
	refused := "range ends past size_bytes"
	cases := []struct {
		name string
		ack  types.ArtifactRangeAckFrame
	}{
		{"committed", types.ArtifactRangeAckFrame{Type: ArtifactRangeAckType, RangeID: 7, OK: true}},
		{"refused", types.ArtifactRangeAckFrame{Type: ArtifactRangeAckType, RangeID: 8, OK: false, Error: &refused}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			frame, err := EncodeArtifactRangeAck(&tc.ack)
			if err != nil {
				t.Fatalf("EncodeArtifactRangeAck: %v", err)
			}
			payload, err := NewFrameDecoder(bytes.NewReader(frame)).ReadFrame()
			if err != nil {
				t.Fatalf("ReadFrame: %v", err)
			}
			result, err := DecodeFrame(payload)
			if err != nil {
				t.Fatalf("DecodeFrame: %v", err)
			}
			decoded, ok := result.(*types.ArtifactRangeAckFrame)
			if !ok {
				t.Fatalf("DecodeFrame returned %T, want *types.ArtifactRangeAckFrame", result)
			}
			if decoded.RangeID != tc.ack.RangeID || decoded.OK != tc.ack.OK {
				t.Fatalf("DecodeFrame = %+v; want %+v", decoded, tc.ack)
			}
			if (decoded.Error == nil) != (tc.ack.Error == nil) || (decoded.Error != nil && *decoded.Error != *tc.ack.Error) {
				t.Fatalf("Error = %v; want %v", decoded.Error, tc.ack.Error)
			}
		})
	}
}

// A frame whose type names an artifact frame but whose fields do not decode
// is a decode error, not a stream resynchronization.
func TestDecodeFrame_ArtifactRange_Malformed(t *testing.T) {
	t.Parallel()
	payload, err := msgpack.Marshal(map[string]any{"type": ArtifactRangeType, "range_id": "not a number"})
	if err != nil {
		t.Fatalf("Marshal: %v", err)
	}
	_, err = DecodeFrame(payload)
	var frameErr *FrameError
	if !errors.As(err, &frameErr) || frameErr.Kind != FrameErrorDecode {
		t.Fatalf("DecodeFrame error = %v; want a FrameErrorDecode", err)
	}
}

// A frame that omits a required field is a decode error, since a struct
// decode would otherwise read the field as zero.
func TestDecodeFrame_ArtifactFrames_RequireFields(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name  string
		frame map[string]any
	}{
		{"open without size_bytes", map[string]any{"type": ArtifactOpenType, "artifact_id": "a1"}},
		{"open without artifact_id", map[string]any{"type": ArtifactOpenType, "size_bytes": 1}},
		{"range without offset", map[string]any{"type": ArtifactRangeType, "artifact_id": "a1", "range_id": 1, "data": []byte("x")}},
		{"range without range_id", map[string]any{"type": ArtifactRangeType, "artifact_id": "a1", "offset": 0, "data": []byte("x")}},
		{"range without data", map[string]any{"type": ArtifactRangeType, "artifact_id": "a1", "range_id": 1, "offset": 0}},
		{"ack without ok", map[string]any{"type": ArtifactRangeAckType, "range_id": 1}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			payload, err := msgpack.Marshal(tc.frame)
			if err != nil {
				t.Fatalf("Marshal: %v", err)
			}
			_, err = DecodeFrame(payload)
			var frameErr *FrameError
			if !errors.As(err, &frameErr) || frameErr.Kind != FrameErrorDecode {
				t.Fatalf("DecodeFrame = %v; want a FrameErrorDecode for the missing field", err)
			}
		})
	}
}
