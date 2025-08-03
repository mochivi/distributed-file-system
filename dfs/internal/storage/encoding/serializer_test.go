package encoding

import (
	"bytes"
	"io"
	"testing"

	"errors"

	"github.com/mochivi/distributed-file-system/internal/common"
	"github.com/stretchr/testify/assert"
)

func TestProtoSerializer_Roundtrip(t *testing.T) {
	serializer := NewProtoSerializer()

	testCases := []struct {
		name   string
		header common.ChunkHeader
	}{
		{
			name: "success: full header",
			header: common.ChunkHeader{
				ID:       "chunk1",
				Index:    1,
				Size:     1024,
				Checksum: "somesha256checksum",
			},
		},
		{
			name: "success: empty checksum",
			header: common.ChunkHeader{
				ID:       "chunk2",
				Index:    2,
				Size:     2048,
				Checksum: "",
			},
		},
		{
			name: "success: empty ID",
			header: common.ChunkHeader{
				ID:       "",
				Index:    3,
				Size:     4096,
				Checksum: "anotherchecksum",
			},
		},
		{
			name: "success: zero index",
			header: common.ChunkHeader{
				ID:       "chunk4",
				Index:    0,
				Size:     512,
				Checksum: "yetanotherchecksum",
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			// Serialize
			serializedData, err := serializer.SerializeHeader(tc.header)
			assert.NoError(t, err)
			assert.NotNil(t, serializedData)

			// Deserialize
			reader := bytes.NewReader(serializedData)
			deserializedHeader, err := serializer.DeserializeHeader(reader)

			assert.NoError(t, err)
			assert.Equal(t, tc.header, deserializedHeader)
		})
	}
}

func TestProtoSerializer_DeserializeHeader_Errors(t *testing.T) {
	serializer := NewProtoSerializer().(*ProtoSerializer)
	validHeader := common.ChunkHeader{
		ID:       "chunk1",
		Index:    1,
		Size:     1024,
		Checksum: "somesha256checksum",
	}
	validSerialized, err := serializer.SerializeHeader(validHeader)
	assert.NoError(t, err)

	testCases := []struct {
		name        string
		data        []byte
		expectedErr error
		isEofError  bool
	}{
		{
			name:        "error: invalid magic",
			data:        []byte("BADM\x01\x00\x00\x00\x00"),
			expectedErr: ErrInValidHeader,
		},
		{
			name: "error: invalid version",
			data: bytes.Join([][]byte{
				[]byte(serializer.Magic),
				{2}, // invalid version
				validSerialized[5:],
			}, nil),
			expectedErr: ErrInValidHeader,
		},
		{
			name:       "error: eof reading magic",
			data:       []byte("PSC"),
			isEofError: true,
		},
		{
			name:       "error: eof reading version",
			data:       []byte(serializer.Magic),
			isEofError: true,
		},
		{
			name:       "error: eof reading length",
			data:       append([]byte(serializer.Magic), serializer.Version),
			isEofError: true,
		},
		{
			name:       "error: eof reading header",
			data:       validSerialized[:len(validSerialized)-1],
			isEofError: true,
		},
		{
			name:        "error: header length is 0",
			data:        append(append([]byte(serializer.Magic), serializer.Version), []byte{0, 0, 0, 0}...),
			expectedErr: ErrInValidHeader,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			reader := bytes.NewReader(tc.data)
			_, err := serializer.DeserializeHeader(reader)
			assert.Error(t, err)

			if tc.isEofError {
				isEof := errors.Is(err, io.EOF)
				isUnexpectedEof := errors.Is(err, io.ErrUnexpectedEOF)
				assert.True(t, isEof || isUnexpectedEof, "expected EOF or ErrUnexpectedEOF, but got %v", err)
			} else {
				assert.ErrorIs(t, err, tc.expectedErr)
			}
		})
	}
}

func TestProtoSerializer_HeaderSize(t *testing.T) {
	serializer := NewProtoSerializer()
	testCases := []struct {
		name   string
		header common.ChunkHeader
	}{
		{
			name: "success: full header",
			header: common.ChunkHeader{
				ID:       "chunk1",
				Index:    1,
				Size:     1024,
				Checksum: "somesha256checksum",
			},
		},
		{
			name:   "success: empty header",
			header: common.ChunkHeader{},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			serialized, err := serializer.SerializeHeader(tc.header)
			assert.NoError(t, err)

			size, err := serializer.HeaderSize(tc.header)
			assert.NoError(t, err)

			assert.Equal(t, len(serialized), size)
		})
	}
}
