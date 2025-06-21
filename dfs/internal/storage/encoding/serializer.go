package encoding

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"os"

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
	DeserializeHeader(file *os.File) (common.ChunkHeader, error)
	HeaderSize() int
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

func (s *ProtoSerializer) DeserializeHeader(file *os.File) (common.ChunkHeader, error) {
	// 1) read magic
	magic := make([]byte, 4)
	if _, err := file.Read(magic); err != nil {
		return common.ChunkHeader{}, err
	}
	if string(magic) != s.Magic {
		return common.ChunkHeader{}, fmt.Errorf("invalid magic: %s", string(magic))
	}

	// 2) read version
	version := make([]byte, 1)
	if _, err := file.Read(version); err != nil {
		return common.ChunkHeader{}, err
	}
	if version[0] != s.Version {
		return common.ChunkHeader{}, fmt.Errorf("invalid version: %d", version[0])
	}

	// 3) read length
	length := make([]byte, 4)
	if _, err := file.Read(length); err != nil {
		return common.ChunkHeader{}, err
	}
	lengthInt := binary.BigEndian.Uint32(length)

	// 4) read header
	headerBytes := make([]byte, lengthInt)
	if _, err := file.Read(headerBytes); err != nil {
		return common.ChunkHeader{}, err
	}

	headerPB := &proto.ChunkHeader{}
	if err := pb.Unmarshal(headerBytes, headerPB); err != nil {
		return common.ChunkHeader{}, err
	}
	return common.ChunkHeaderFromProto(headerPB), nil
}

func (s *ProtoSerializer) HeaderSize() int {
	return 13 // 4 B magic + 1 B version + 4 B length + 4 B header
}
