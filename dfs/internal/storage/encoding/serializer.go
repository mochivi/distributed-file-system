package encoding

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"io"

	"github.com/mochivi/distributed-file-system/internal/common"
	"github.com/mochivi/distributed-file-system/pkg/proto"
	pb "google.golang.org/protobuf/proto"
)

type ChunkHeader struct {
	Magic    string
	Version  byte
	ID       string
	Index    int
	Size     int
	Checksum string
}

type Chunk struct {
	Header ChunkHeader
	Data   []byte
}

type ChunkSerializer interface {
	SerializeHeader(chunkHeader common.ChunkHeader) ([]byte, error)
	DeserializeHeader(reader io.Reader) (common.ChunkHeader, error)
	HeaderSize(chunkHeader common.ChunkHeader) (int, error)
}

type ProtoSerializer struct {
	Magic   string
	Version byte
}

func NewProtoSerializer() ChunkSerializer {
	return &ProtoSerializer{
		Magic:   "PSCH",
		Version: 1,
	}
}

func (s *ProtoSerializer) SerializeHeader(chunkHeader common.ChunkHeader) ([]byte, error) {
	// 1) marshal header
	headerPB := chunkHeader.ToProto()
	headerBytes, err := pb.Marshal(headerPB)
	if err != nil {
		return nil, err
	}

	// 2) build file: magic + ver + len + header + data
	buf := new(bytes.Buffer)
	buf.WriteString(s.Magic) // 4 B magic
	buf.WriteByte(s.Version) // 1 B version

	binary.Write(buf, binary.BigEndian, uint32(len(headerBytes))) // 4 B length
	buf.Write(headerBytes)

	return buf.Bytes(), nil
}

func (s *ProtoSerializer) DeserializeHeader(reader io.Reader) (common.ChunkHeader, error) {
	// 1) read magic
	magic := make([]byte, 4)
	if _, err := io.ReadFull(reader, magic); err != nil {
		return common.ChunkHeader{}, err
	}
	if string(magic) != s.Magic {
		return common.ChunkHeader{}, fmt.Errorf("invalid magic: %s", string(magic))
	}

	// 2) read version
	version := make([]byte, 1)
	if _, err := io.ReadFull(reader, version); err != nil {
		return common.ChunkHeader{}, err
	}
	if version[0] != s.Version {
		return common.ChunkHeader{}, fmt.Errorf("invalid version: %d", version[0])
	}

	// 3) read length
	lengthBytes := make([]byte, 4)
	if _, err := io.ReadFull(reader, lengthBytes); err != nil {
		return common.ChunkHeader{}, err
	}
	lengthInt := binary.BigEndian.Uint32(lengthBytes)

	if lengthInt == 0 {
		return common.ChunkHeader{}, errors.New("header length is 0")
	}

	// 4) read header
	headerBytes := make([]byte, lengthInt)
	if _, err := io.ReadFull(reader, headerBytes); err != nil {
		return common.ChunkHeader{}, err
	}

	headerPB := &proto.ChunkHeader{}
	if err := pb.Unmarshal(headerBytes, headerPB); err != nil {
		return common.ChunkHeader{}, err
	}
	return common.ChunkHeaderFromProto(headerPB), nil
}

func (s *ProtoSerializer) HeaderSize(chunkHeader common.ChunkHeader) (int, error) {
	headerPB := chunkHeader.ToProto()
	headerBytes, err := pb.Marshal(headerPB)
	if err != nil {
		return 0, err
	}
	return 9 + len(headerBytes), nil // 4 B magic + 1 B version + 4 B length
}
