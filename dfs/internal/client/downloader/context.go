package downloader

import (
	"context"
	"log/slog"
	"os"
	"sync"

	"github.com/mochivi/distributed-file-system/internal/common"
)

// downloadContext is shared between all workers in the download session
// coordinates file writing, error handling and the concurrency context
type downloadContext struct {
	// Input params
	chunkLocations []common.ChunkLocation
	chunkHeaders   []common.ChunkHeader

	// Output params
	file *fileWriter

	// Internal state
	ctx      context.Context
	workChan chan downloadWork
	errChan  chan error
	wg       sync.WaitGroup
	logger   *slog.Logger // scoped to the download session
}

func NewDownloadContext(ctx context.Context, chunkLocations []common.ChunkLocation, chunkHeaders []common.ChunkHeader,
	tempFile *os.File, fileInfo common.FileInfo, numWorkers int, logger *slog.Logger) *downloadContext {
	fileWriter := &fileWriter{
		File:  tempFile,
		mutex: sync.Mutex{},
		size:  int64(fileInfo.Size),
	}

	return &downloadContext{
		chunkLocations: chunkLocations,
		chunkHeaders:   chunkHeaders,

		file: fileWriter,

		ctx:      ctx,
		workChan: make(chan downloadWork, numWorkers),
		errChan:  make(chan error, len(chunkLocations)),
		wg:       sync.WaitGroup{},

		logger: logger,
	}

}

type fileWriter struct {
	*os.File
	mutex sync.Mutex
	size  int64
}

func (f *fileWriter) SeekWrite(offset int64, whence int, p []byte) (n int, err error) {
	f.mutex.Lock()
	defer f.mutex.Unlock()
	if _, err := f.File.Seek(offset, whence); err != nil {
		return 0, err
	}
	return f.File.Write(p)
}

func (f *fileWriter) Delete() error {
	return os.Remove(f.Name())
}

func (f *fileWriter) Close() error {
	return f.File.Close()
}
