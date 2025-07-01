package metadata

import (
	"github.com/mochivi/distributed-file-system/internal/common"
	"github.com/stretchr/testify/mock"
)

type MockMetadataStore struct {
	mock.Mock
}

func (m *MockMetadataStore) PutFile(path string, info *common.FileInfo) error {
	args := m.Called(path, info)
	return args.Error(0)
}

func (m *MockMetadataStore) GetFile(path string) (*common.FileInfo, error) {
	args := m.Called(path)
	if args.Get(0) == nil {
		return nil, args.Error(1)
	}
	return args.Get(0).(*common.FileInfo), args.Error(1)
}

func (m *MockMetadataStore) DeleteFile(path string) error {
	args := m.Called(path)
	return args.Error(0)
}

func (m *MockMetadataStore) ListFiles(directory string) ([]*common.FileInfo, error) {
	args := m.Called(directory)
	return args.Get(0).([]*common.FileInfo), args.Error(1)
}
