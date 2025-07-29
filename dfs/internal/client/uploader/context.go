package uploader

import (
	"context"
	"log/slog"
	"sort"
	"sync"

	"github.com/mochivi/distributed-file-system/internal/common"
	"github.com/mochivi/distributed-file-system/pkg/client_pool"
)

// chunkInfoMap stores information about each datanode where chunks are stored
type chunkInfoMap struct {
	chunkInfos map[string]*common.ChunkInfo
	mutex      sync.Mutex
}

func newChunkInfoMap() *chunkInfoMap {
	return &chunkInfoMap{
		chunkInfos: make(map[string]*common.ChunkInfo),
	}
}

func (c *chunkInfoMap) addChunkInfo(chunkInfo *common.ChunkInfo) {
	c.mutex.Lock()
	defer c.mutex.Unlock()
	c.chunkInfos[chunkInfo.Header.ID] = chunkInfo
}

func (c *chunkInfoMap) getChunkInfos() []common.ChunkInfo {
	c.mutex.Lock()
	defer c.mutex.Unlock()
	chunkInfos := make([]common.ChunkInfo, 0, len(c.chunkInfos))
	for _, chunkInfo := range c.chunkInfos {
		chunkInfos = append(chunkInfos, *chunkInfo)
	}

	// Sort chunkInfos by index to ensure the chunks are uploaded in order
	sort.Slice(chunkInfos, func(i, j int) bool {
		return chunkInfos[i].Header.Index < chunkInfos[j].Header.Index
	})

	return chunkInfos
}

// UploadContext is used throughout the upload functionality to coordinate the upload process
type uploadContext struct {
	// Input params
	chunkIDs  []string
	chunksize int

	// Thread safe map to store information of where each chunk is stored as workers upload chunks
	chunkInfos *chunkInfoMap

	// Client pool to get clients for each chunk
	clientPool client_pool.ClientPool

	// Internal state
	ctx      context.Context
	workChan chan UploaderWork
	errChan  chan error
	wg       *sync.WaitGroup
	logger   *slog.Logger // scoped to the upload context
}

func NewUploadContext(ctx context.Context, chunkIDs []string, chunksize int, clientPool client_pool.ClientPool, logger *slog.Logger) *uploadContext {
	return &uploadContext{
		chunkIDs:   chunkIDs,
		chunksize:  chunksize,
		chunkInfos: newChunkInfoMap(),
		clientPool: clientPool,
		ctx:        ctx,
		workChan:   make(chan UploaderWork, 10),
		errChan:    make(chan error), // TODO: add channel buffering
		wg:         &sync.WaitGroup{},
		logger:     logger,
	}
}
