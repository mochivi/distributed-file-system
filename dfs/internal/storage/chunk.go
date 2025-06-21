package storage

import "github.com/mochivi/distributed-file-system/internal/common"

type ChunkStorage interface {
	Store(chunkHeader common.ChunkHeader, data []byte) error

	GetChunkHeader(chunkID string) (common.ChunkHeader, error)
	GetChunkData(chunkID string) ([]byte, error)
	GetChunk(chunkID string) (common.ChunkHeader, []byte, error)

	Delete(chunkID string) error
	Exists(chunkID string) bool
	List() ([]string, error)
}
