package storage

type ChunkStorage interface {
	Store(chunkID string, data []byte) error
	Retrieve(chunkID string) ([]byte, error)
	Delete(chunkID string) error
	Exists(chunkID string) bool
	List() ([]string, error)
}
