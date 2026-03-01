package common

import (
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"path/filepath"
	"strconv"
	"strings"

	"github.com/mochivi/distributed-file-system/pkg/proto"
)

// ChunkHeader + proto conversions
type ChunkHeader struct {
	ID       ChunkID
	Version  byte
	Index    int // Index of the chunk in the file
	Size     FileSize
	Checksum string
}

func ChunkHeaderFromProto(pb *proto.ChunkHeader) (ChunkHeader, error) {
	size, err := NewFileSize(pb.Size)
	if err != nil {
		return ChunkHeader{}, err
	}
	chunkID, err := NewChunkID(pb.Id)
	if err != nil {
		return ChunkHeader{}, err
	}

	return ChunkHeader{
		ID:       chunkID,
		Version:  byte(pb.Version),
		Index:    int(pb.Index),
		Size:     size,
		Checksum: pb.Checksum,
	}, nil
}

func (ch ChunkHeader) ToProto() *proto.ChunkHeader {
	return &proto.ChunkHeader{
		Id:       ch.ID.String(),
		Version:  uint32(ch.Version),
		Index:    int32(ch.Index),
		Size:     ch.Size.ToProto(),
		Checksum: ch.Checksum,
	}
}

type ChunkInfo struct {
	Header   ChunkHeader
	Replicas []*NodeInfo // DataNode IDs storing this chunk
}

func ChunkInfoFromProto(pb *proto.ChunkInfo) (ChunkInfo, error) {
	replicas := make([]*NodeInfo, 0, len(pb.Replicas))
	for _, replica := range pb.Replicas {
		replicas = append(replicas, NodeInfoFromProto(replica))
	}
	header, err := ChunkHeaderFromProto(pb.Header)
	if err != nil {
		return ChunkInfo{}, err
	}
	return ChunkInfo{
		Header:   header,
		Replicas: replicas,
	}, nil
}

func (ci ChunkInfo) ToProto() *proto.ChunkInfo {
	protoReplicas := make([]*proto.NodeInfo, 0, len(ci.Replicas))
	for _, replica := range ci.Replicas {
		protoReplicas = append(protoReplicas, replica.ToProto())
	}
	return &proto.ChunkInfo{
		Header:   ci.Header.ToProto(),
		Replicas: protoReplicas,
	}
}

/*
Base types
*/

type ChunkID string

func NewChunkID(id string) (ChunkID, error) {
	parts := strings.Split(id, "_")
	if len(parts) != 2 {
		return "", fmt.Errorf("%w: invalid format: %s", ErrValidation, id)
	}
	_, err := strconv.Atoi(parts[1])
	if err != nil {
		return "", fmt.Errorf("%w: chunk index must be a number: %s", ErrValidation, id)
	}
	return ChunkID(id), nil
}

func (c ChunkID) ValidateChunkID() error {
	parts := strings.Split(c.String(), "_")
	if len(parts) != 2 {
		return errors.New("invalid format")
	}

	// Ensure the first part (path hash) is valid hex and 8 bytes long
	decoded, err := hex.DecodeString(parts[0])
	if err != nil {
		return errors.New("invalid path hash")
	}
	if len(decoded) != 8 {
		return errors.New("invalid path hash length")
	}

	// Second part should be a number
	if _, err := strconv.Atoi(parts[1]); err != nil {
		return errors.New("invalid chunk number")
	}

	return nil
}

func (c ChunkID) GetChunkRelativePath() (string, error) {
	dir1 := c[0:2]
	dir2 := c[2:4]
	return filepath.Join(dir1.String(), dir2.String(), c.String()), nil
}

// FormatChunkID creates a unique identifier for a chunk using a hash of the file path
// and the chunk number. This avoids issues with long paths and special characters.
func FormatChunkID(path FilePath, chunkIndex int) ChunkID {
	// Create a hash of the file path to avoid long filenames
	pathHash := sha256.Sum256([]byte(path))
	pathHashStr := hex.EncodeToString(pathHash[:8]) // Use first 8 bytes for shorter ID
	return ChunkID(fmt.Sprintf("%s_%d", pathHashStr, chunkIndex))
}

func FormatChunkIDs(prefix string, numChunks int) []ChunkID {
	chunkIDs := make([]ChunkID, numChunks)
	for i := range numChunks {
		chunkID := fmt.Sprintf("%s_%d", prefix, i)
		chunkIDs[i] = ChunkID(chunkID)
	}
	return chunkIDs
}

func HashFilepath(path string) string {
	pathHash := sha256.Sum256([]byte(path))
	pathHashStr := hex.EncodeToString(pathHash[:8]) // Use first 8 bytes for shorter ID
	return pathHashStr
}

func (c ChunkID) String() string {
	return string(c)
}

type ChunkSize int

func NewChunkSize(cs int64) (ChunkSize, error) {
	chunkSize := ChunkSize(cs)
	if err := chunkSize.validate(); err != nil {
		return 0, fmt.Errorf("%w: %w", ErrValidation, err)
	}
	return chunkSize, nil
}

func (cs ChunkSize) validate() error {
	errs := make([]error, 0)
	if cs < 1*1024*1024 { // 1MB minimun chunksize allowed
		errs = append(errs, errors.New("chunkSize cannot be lower than 1MB"))
	}
	if cs > 128*1024*1024 { // 128MB maximum chunksize allowed
		errs = append(errs, errors.New("chunksize cannot be larger than 128MB"))
	}
	if len(errs) != 0 {
		return errors.Join(errs...)
	}
	return nil
}
