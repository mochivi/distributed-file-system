package common

import (
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/mochivi/distributed-file-system/pkg/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
)

type FileInfo struct {
	Path       FilePath
	Size       FileSize
	ChunkCount ChunkCount
	Chunks     []ChunkInfo
	CreatedAt  time.Time
	Checksum   string
	Deleted    bool
	DeletedAt  time.Time
}

func FileInfoFromProto(pb *proto.FileInfo) (FileInfo, error) {
	chunks := make([]ChunkInfo, 0, len(pb.Chunks))
	for _, chunk := range pb.Chunks {
		chunkInfo, err := ChunkInfoFromProto(chunk)
		if err != nil {
			return FileInfo{}, fmt.Errorf("%w: %w", ErrValidation, err)
		}
		chunks = append(chunks, chunkInfo)
	}

	path, err := NewFilePath(pb.Path)
	if err != nil {
		return FileInfo{}, fmt.Errorf("%w: %w", ErrValidation, err)
	}
	size, err := NewFileSize(pb.Size)
	if err != nil {
		return FileInfo{}, fmt.Errorf("%w: %w", ErrValidation, err)
	}
	chunkCount, err := NewChunkCount(pb.ChunkCount)
	if err != nil {
		return FileInfo{}, fmt.Errorf("%w: %w", ErrValidation, err)
	}

	return FileInfo{
		Path:       path,
		Size:       size,
		ChunkCount: chunkCount,
		Chunks:     chunks,
		CreatedAt:  pb.CreatedAt.AsTime(),
		Checksum:   pb.Checksum,
		Deleted:    pb.Deleted,
		DeletedAt:  pb.DeletedAt.AsTime(),
	}, nil
}

func (fi FileInfo) ToProto() *proto.FileInfo {
	protoChunks := make([]*proto.ChunkInfo, 0, len(fi.Chunks))
	for _, item := range fi.Chunks {
		protoChunks = append(protoChunks, item.ToProto())
	}

	return &proto.FileInfo{
		Path:       fi.Path.String(),
		Size:       fi.Size.ToProto(),
		ChunkCount: fi.ChunkCount.ToProto(),
		Chunks:     protoChunks,
		CreatedAt:  timestamppb.New(fi.CreatedAt),
		Checksum:   fi.Checksum,
		Deleted:    fi.Deleted,
		DeletedAt:  timestamppb.New(fi.DeletedAt),
	}
}

type FilePath string

func NewFilePath(path string) (FilePath, error) {
	p := FilePath(path)
	if err := p.validate(); err != nil {
		return "", err
	}
	return p, nil
}

func (p FilePath) validate() error {
	errs := make([]error, 0)
	if p == "" {
		errs = append(errs, errors.New("path is required"))
	}
	if !strings.HasPrefix(string(p), "/") {
		errs = append(errs, errors.New("path must start with a /"))
	}
	if len(errs) != 0 {
		return fmt.Errorf("%w: %w", ErrValidation, errors.Join(errs...))
	}
	return nil
}

func (p FilePath) String() string {
	return string(p)
}

func (p FilePath) Hash() string {
	pathHash := sha256.Sum256([]byte(p))
	pathHashStr := hex.EncodeToString(pathHash[:8]) // Use first 8 bytes for shorter ID
	return pathHashStr
}

type FileSize int

func NewFileSize(size int64) (FileSize, error) {
	fs := FileSize(size)
	if err := fs.validate(); err != nil {
		return 0, err
	}
	return fs, nil
}

func (fs FileSize) validate() error {
	if fs < 0 {
		return fmt.Errorf("%w: file size cannot be negative: %d", ErrValidation, fs)
	}
	if fs > 1024*1024*1024*1024 { // TODO: improve later - 1TB MAX FILE SIZE
		return fmt.Errorf("%w: file size exceeds maximum: %d", ErrValidation, fs)
	}
	return nil
}

func (fs FileSize) ToProto() int64 {
	return int64(fs)
}

type ChunkCount int

func NewChunkCount(count int32) (ChunkCount, error) {
	c := ChunkCount(count)
	if err := c.validate(); err != nil {
		return 0, err
	}
	return c, nil
}

func (c ChunkCount) validate() error {
	if c < 0 {
		return fmt.Errorf("%w: chunk count cannot be negative: %d", ErrValidation, c)
	}
	return nil
}

func (c ChunkCount) ToProto() int32 {
	return int32(c)
}
