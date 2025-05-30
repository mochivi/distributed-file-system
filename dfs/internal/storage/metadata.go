package storage

import "github.com/mochivi/distributed-file-system/internal/common"

type MetadataStore interface {
	PutFile(path string, info *common.FileInfo) error
	GetFile(path string) (*common.FileInfo, error)
	DeleteFile(path string) error
	ListFiles(directory string) ([]*common.FileInfo, error)
}
