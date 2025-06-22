package client

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"os"
	"sync"
	"time"

	"github.com/mochivi/distributed-file-system/internal/common"
	"github.com/mochivi/distributed-file-system/internal/coordinator"
	"github.com/mochivi/distributed-file-system/internal/datanode"
)

// chunkInfoMap stores information about each datanode where chunks are stored
type chunkInfoMap struct {
	chunkInfos map[string]*common.ChunkInfo
	mutex      sync.Mutex
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
	return chunkInfos
}

// Define the datatype that workers will receive
type UploaderWork struct {
	sessionID   string
	chunkHeader common.ChunkHeader
	data        []byte
	clientPool  *ClientPool
}

func (w *UploaderWork) GetClient() (*datanode.DataNodeClient, error) {
	client, sessionID, err := w.clientPool.GetClient(w.chunkHeader)
	if err != nil {
		return nil, err
	}
	w.sessionID = sessionID
	return client, nil
}

type ClientPool struct {
	index   int
	clients []*datanode.DataNodeClient
	mu      sync.Mutex
}

func NewClientPool(clients []*datanode.DataNodeClient) *ClientPool {
	return &ClientPool{
		clients: clients,
	}
}

func (c *ClientPool) GetClient(chunkHeader common.ChunkHeader) (*datanode.DataNodeClient, string, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	attempts := 0
	for attempts < len(c.clients) {
		client := c.clients[c.index]
		// Check if client accepts to store the chunk
		storeChunkResponse, err := client.StoreChunk(context.Background(), chunkHeader)
		if err != nil {
			attempts++
			c.RotateClient()
			continue
		}

		if !storeChunkResponse.Accept {
			attempts++
			c.RotateClient()
			continue
		}

		return client, storeChunkResponse.SessionID, nil
	}

	return nil, "", fmt.Errorf("failed to get client")
}

func (c *ClientPool) RotateClient() {
	c.mu.Lock()
	if c.index >= len(c.clients) {
		c.index = 0
	}
	c.index++
	c.mu.Unlock()
}

func (c *ClientPool) AddClient(client *datanode.DataNodeClient) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.clients = append(c.clients, client)
}

type UploaderConfig struct {
	NumWorkers      int
	ChunkRetryCount int
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

// Uploads chunks to peer
type Uploader struct {
	streamer *common.Streamer
	config   UploaderConfig
}

func NewUploader(streamer *common.Streamer, logger *slog.Logger, config UploaderConfig) *Uploader {
	return &Uploader{
		streamer: streamer,
		config:   config,
	}
}

func (u *Uploader) UploadFile(ctx context.Context, file *os.File, chunkLocations []coordinator.ChunkLocation, logger *slog.Logger, chunksize int) ([]common.ChunkInfo, error) {
	session := &uploadSession{
		ctx:            ctx,
		chunkLocations: chunkLocations,
		chunksize:      chunksize,
		workChan:       make(chan UploaderWork, u.config.NumWorkers),
		errChan:        make(chan error, len(chunkLocations)),
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
		chunkHeader := common.ChunkHeader{
			ID:       chunkUploadLocation.ChunkID,
			Index:    i,
			Size:     int64(len(chunkData)),
			Checksum: checksum,
		}

		clientPool := NewClientPool(make([]*datanode.DataNodeClient, 0, len(chunkUploadLocation.Nodes)))
		for _, node := range chunkUploadLocation.Nodes {
			client, err := datanode.NewDataNodeClient(node)
			if err != nil {
				session.logger.Error("Failed to create connection to datanode client", slog.String("error", err.Error()))
				return fmt.Errorf("failed to create connection to datanode client: %w", err)
			}
			clientPool.AddClient(client)
		}

		work := UploaderWork{
			chunkHeader: chunkHeader,
			data:        chunkData,
			clientPool:  clientPool,
		}

		session.workChan <- work
	}

	return nil
}

func (u *Uploader) processWork(work UploaderWork, session *uploadSession) error {
	retryCount := 0
	var replicatedNodes []*common.DataNodeInfo
	for retryCount < u.config.ChunkRetryCount {
		var err error
		replicatedNodes, err = u.uploadChunk(work, session)
		if err != nil {
			retryCount++
			if retryCount >= u.config.ChunkRetryCount {
				return fmt.Errorf("failed to store chunk %s after %d retries", work.chunkHeader.ID, retryCount)
			}
			time.Sleep(time.Duration(retryCount) * time.Second)
			continue
		}
		break
	}

	// Update chunk infos - this information is sent to the coordinator to update the metadata about the chunk
	session.chunkInfos.addChunkInfo(&common.ChunkInfo{
		Header:   work.chunkHeader,
		Replicas: replicatedNodes,
	})

	return nil
}

func (u *Uploader) uploadChunk(work UploaderWork, session *uploadSession) ([]*common.DataNodeInfo, error) {
	session.logger.Info("Streaming chunk")

	// Open stream to send chunk data to the datanode
	client, err := work.GetClient()
	if err != nil {
		session.logger.Error("Failed to get client", slog.String("error", err.Error()))
		return nil, fmt.Errorf("failed to get client: %w", err)
	}

	stream, err := client.UploadChunkStream(session.ctx)
	if err != nil {
		session.logger.Error("Failed to create upload stream")
		return nil, fmt.Errorf("failed to create stream for chunk %s: %w", work.chunkHeader.ID, err)
	}

	// Stream chunk to peer
	replicatedNodes, err := u.streamer.SendChunkStream(session.ctx, stream, session.logger, common.UploadChunkStreamParams{
		SessionID:   work.sessionID, // This sessionID is the streaming sessionID, NOT the metadata sessionID or uploadSessionID
		ChunkHeader: work.chunkHeader,
		Data:        work.data,
	})
	if err != nil {
		session.logger.Error("Failed to stream chunk", slog.String("error", err.Error()))
		return nil, fmt.Errorf("failed to stream chunk %s: %w", work.chunkHeader.ID, err)
	}
	if replicatedNodes == nil {
		session.logger.Error("Datanode failed to replicate chunk", slog.String("chunk_id", work.chunkHeader.ID))
		return nil, fmt.Errorf("datanode failed to replicate chunk %s", work.chunkHeader.ID)
	}

	session.logger.Info("Chunk streaming completed")

	return replicatedNodes, nil
}
