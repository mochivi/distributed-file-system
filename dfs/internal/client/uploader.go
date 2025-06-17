package client

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"os"
	"sync"

	"github.com/mochivi/distributed-file-system/internal/common"
	"github.com/mochivi/distributed-file-system/internal/coordinator"
	"github.com/mochivi/distributed-file-system/internal/datanode"
)

// chunkInfoMap stores information about each datanode where chunks are stored
type chunkInfoMap struct {
	chunkInfos map[string]*common.ChunkInfo
	mutex      sync.Mutex
}

func (c *chunkInfoMap) tryAddReplica(chunkID string, replica *common.DataNodeInfo) bool {
	c.mutex.Lock()
	defer c.mutex.Unlock()
	chunkInfo, ok := c.chunkInfos[chunkID]
	if !ok {
		return false
	}
	chunkInfo.Replicas = append(chunkInfo.Replicas, replica)
	return true
}

func (c *chunkInfoMap) addChunkInfo(chunkInfo *common.ChunkInfo) {
	c.mutex.Lock()
	defer c.mutex.Unlock()
	c.chunkInfos[chunkInfo.ID] = chunkInfo
}

func (c *chunkInfoMap) getChunkInfos() []common.ChunkInfo {
	c.mutex.Lock()
	defer c.mutex.Unlock()
	chunkInfos := make([]common.ChunkInfo, 0, len(c.chunkInfos))
	for _, chunkInfo := range c.chunkInfos {
		chunkInfos = append(chunkInfos, *chunkInfo)
	}
	return chunkInfos
}

// Define the datatype that workers will receive
type UploaderWork struct {
	sessionID string
	chunkInfo common.ChunkInfo
	data      []byte
	client    *datanode.DataNodeClient
}

type UploaderConfig struct {
	NumWorkers      int
	ChunkRetryCount int
}

// Uploads chunks to peer
type Uploader struct {
	streamer *common.Streamer
	config   UploaderConfig
}

type uploadSession struct {
	// Input params
	chunkLocations []coordinator.ChunkLocation
	chunksize      int

	// Internal state
	ctx        context.Context
	workChan   chan UploaderWork
	errChan    chan error
	chunkInfos *chunkInfoMap
	wg         *sync.WaitGroup
	logger     *slog.Logger // scoped to the upload session
}

func NewUploader(streamer *common.Streamer, logger *slog.Logger, config UploaderConfig) *Uploader {
	return &Uploader{
		streamer: streamer,
		config:   config,
	}
}

func (u *Uploader) UploadFile(ctx context.Context, file *os.File, chunkLocations []coordinator.ChunkLocation, sessionID string, logger *slog.Logger) ([]common.ChunkInfo, error) {
	session := &uploadSession{
		ctx:      ctx,
		workChan: make(chan UploaderWork, u.config.NumWorkers),
		errChan:  make(chan error, len(chunkLocations)),
		chunkInfos: &chunkInfoMap{
			chunkInfos: make(map[string]*common.ChunkInfo, len(chunkLocations)),
			mutex:      sync.Mutex{},
		},
		wg:     &sync.WaitGroup{},
		logger: logger,
	}

	// Launch N workers for session
	for i := range u.config.NumWorkers {
		session.wg.Add(1)
		go func(workerID int) {
			defer session.wg.Done()
			for work := range session.workChan {
				if err := u.processWork(work, session); err != nil {
					session.logger.Error(fmt.Sprintf("Worker %d failed", workerID), slog.String("error", err.Error()))
					session.errChan <- err
				}
			}
		}(i)
	}

	// Queue work
	if err := u.QueueWork(ctx, session, file, session.chunksize); err != nil {
		return nil, fmt.Errorf("failed to queue work: %w", err)
	}

	close(session.workChan)

	// Close error channel when all goroutines complete
	// Looks a bit odd to wg.Wait inside a goroutine but the ErrChan blocks until its closed below
	go func() {
		session.wg.Wait()
		close(session.errChan)
	}()

	// Currently, returning on exactly the first error received
	for err := range session.errChan {
		if err != nil {
			session.logger.Error("Error in worker", slog.String("error", err.Error()))
			return nil, err
		}
	}

	// Now, we need to confirm the upload to the coordinator, so that the metadata is updated
	// This is done by sending a ConfirmUploadRequest to the coordinator
	chunkInfos := session.chunkInfos.getChunkInfos()

	return chunkInfos, nil
}

func (u *Uploader) processWork(work UploaderWork, session *uploadSession) error {
	retryCount := 0
	for retryCount < u.config.ChunkRetryCount {
		if err := u.uploadChunk(work, session); err != nil {
			retryCount++
			if retryCount >= u.config.ChunkRetryCount {
				return fmt.Errorf("failed to store chunk %s after %d retries", work.chunkInfo.ID, retryCount)
			}
		} else {
			break
		}
	}

	// Update chunk infos - this information is sent to the coordinator to update the metadata about the chunk
	// Try to add a replica, if that is unsucessful, we need to first add an entire chunkInfo
	if ok := session.chunkInfos.tryAddReplica(work.chunkInfo.ID, work.client.Node); !ok {
		session.chunkInfos.addChunkInfo(&work.chunkInfo)
	}

	return nil
}

func (u *Uploader) uploadChunk(work UploaderWork, session *uploadSession) error {
	session.logger.Info("Streaming chunk")

	// Open stream to send chunk data to the datanode
	stream, err := work.client.UploadChunkStream(session.ctx)
	if err != nil {
		session.logger.Error("Failed to create upload stream")
		return fmt.Errorf("failed to create stream for chunk %s: %w", work.chunkInfo.ID, err)
	}

	// Stream chunk to peer
	if err := u.streamer.StreamChunk(session.ctx, stream, session.logger, common.StreamChunkParams{
		SessionID: work.sessionID, // This sessionID is the streaming sessionID, NOT the metadata sessionID or uploadSessionID
		ChunkInfo: work.chunkInfo,
		Data:      work.data,
	}); err != nil {
		session.logger.Error("Failed to create upload stream")
		return fmt.Errorf("failed to stream chunk %s: %w", work.chunkInfo.ID, err)
	}

	session.logger.Info("Chunk streaming completed")

	return nil
}

// Reads file and queue work
func (u *Uploader) QueueWork(ctx context.Context, session *uploadSession, file *os.File, chunksize int) error {
	for i, chunkUploadLocation := range session.chunkLocations {
		chunkData := make([]byte, chunksize)
		n, err := file.Read(chunkData)
		if err != nil && err != io.EOF {
			close(session.workChan)
			session.logger.Error("Failed to read chunk", slog.Int("chunk_index", i), slog.String("error", err.Error()))
			return fmt.Errorf("failed to read chunk %d: %w", i, err)
		}
		chunkData = chunkData[:n]

		checksum := common.CalculateChecksum(chunkData)
		chunkInfo := common.ChunkInfo{
			ID:       chunkUploadLocation.ChunkID,
			Index:    i,
			Size:     len(chunkData),
			Replicas: []*common.DataNodeInfo{chunkUploadLocation.Node},
			Checksum: checksum,
		}

		client, err := datanode.NewDataNodeClient(chunkUploadLocation.Node)
		if err != nil {
			session.logger.Error("Failed to create connection to datanode client", slog.String("error", err.Error()))
			return fmt.Errorf("failed to create connection to datanode client: %w", err)
		}

		// Check if client accepts to store the chunk
		storeChunkResponse, err := client.StoreChunk(ctx, chunkInfo)
		if err != nil {
			session.logger.Error("Failed to prepare chunk %s for upload: %w", chunkInfo.ID, err)
			return fmt.Errorf("failed to prepare chunk %s for upload: %w", chunkInfo.ID, err)
		}

		if !storeChunkResponse.Accept {
			session.logger.Error("Node did not accept chunk upload %s: %s", chunkInfo.ID, storeChunkResponse.Message)
			return fmt.Errorf("node did not accept chunk upload %s: %s", chunkInfo.ID, storeChunkResponse.Message)
		}
		session.logger.Info("Chunk accepted", slog.String("chunk_id", chunkInfo.ID))

		session.workChan <- UploaderWork{
			sessionID: storeChunkResponse.SessionID, // streaming sessionID, NOT metadata sessionID
			chunkInfo: chunkInfo,
			data:      chunkData,
			client:    client,
		}
	}

	return nil
}
