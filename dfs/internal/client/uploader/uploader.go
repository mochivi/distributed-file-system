package uploader

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"time"

	"github.com/mochivi/distributed-file-system/internal/clients"
	"github.com/mochivi/distributed-file-system/internal/common"
	"github.com/mochivi/distributed-file-system/internal/storage/chunk"
	"github.com/mochivi/distributed-file-system/pkg/client_pool"
	"github.com/mochivi/distributed-file-system/pkg/logging"
	"github.com/mochivi/distributed-file-system/pkg/streaming"
)

// Define the datatype that workers will receive
type UploaderWork struct {
	chunkHeader common.ChunkHeader
	data        []byte
}

type UploaderConfig struct {
	NumWorkers      int
	ChunkRetryCount int
}

// Uploads chunks to peer
type Uploader struct {
	streamer streaming.ClientStreamer
	config   UploaderConfig
}

func NewUploader(streamer streaming.ClientStreamer, logger *slog.Logger, config UploaderConfig) *Uploader {
	return &Uploader{
		streamer: streamer,
		config:   config,
	}
}

func (u *Uploader) UploadFile(ctx context.Context, reader io.ReadCloser, clientPool client_pool.ClientPool, chunkIDs []string,
	logger *slog.Logger, chunksize int) ([]common.ChunkInfo, error) {
	defer clientPool.Close()

	// UploadContext is shared by all workers and is used to coordinate the upload process
	uploadCtx := NewUploadContext(ctx, chunkIDs, chunksize, clientPool, logger)

	u.startWorkers(uploadCtx, clientPool)

	if err := u.QueueWork(uploadCtx, reader, uploadCtx.chunksize); err != nil {
		return nil, fmt.Errorf("failed to queue work: %w", err)
	}

	if err := u.waitForCompletion(uploadCtx); err != nil {
		return nil, err
	}

	// Now, we need to confirm the upload to the coordinator, so that the metadata is updated
	// This is done by sending a ConfirmUploadRequest to the coordinator
	return uploadCtx.chunkInfos.getChunkInfos(), nil
}

func (u *Uploader) startWorkers(uploadCtx *uploadContext, clientPool client_pool.ClientPool) {
	for i := range u.config.NumWorkers {
		uploadCtx.wg.Add(1)
		go func(workerID int) {
			defer uploadCtx.wg.Done()
			for work := range uploadCtx.workChan {
				if err := u.processWork(work, clientPool, uploadCtx); err != nil {
					uploadCtx.errChan <- err
				}
			}
		}(i)
	}
}

// If any error is ever written to the errChan, it means a worker exhaused all retries over multiple clients
// So, we should fail fast and return the error
func (u *Uploader) waitForCompletion(uploadCtx *uploadContext) error {
	go func() {
		uploadCtx.wg.Wait()
		close(uploadCtx.errChan)
	}()

	for err := range uploadCtx.errChan {
		if err != nil {
			uploadCtx.logger.Error("Error in worker", slog.String("error", err.Error()))
			return err
		}
	}
	return nil
}

// Reads file and queue work
func (u *Uploader) QueueWork(uploadCtx *uploadContext, reader io.ReadCloser, chunksize int) error {
	defer close(uploadCtx.workChan)
	defer reader.Close()

	// ChunkIDs is ordered from start to end of file
	// example chunkIDs: [faewd4123_0, faewd4123_1]
	for index, chunkID := range uploadCtx.chunkIDs {
		chunkHeader, chunkData, err := makeChunk(reader, chunkID, index, chunksize)
		if err != nil {
			uploadCtx.logger.Error("Failed to read chunk", slog.Int("chunk_index", index), slog.String("error", err.Error()))
			return fmt.Errorf("failed to read chunk %d: %w", index, err)
		}

		work := UploaderWork{
			chunkHeader: chunkHeader,
			data:        chunkData,
		}

		uploadCtx.workChan <- work
	}

	return nil
}

func makeChunk(reader io.Reader, chunkID string, index int, chunksize int) (common.ChunkHeader, []byte, error) {
	chunkData := make([]byte, chunksize)
	n, err := reader.Read(chunkData)
	if err != nil && err != io.EOF {
		return common.ChunkHeader{}, nil, err
	}
	chunkData = chunkData[:n]

	checksum := chunk.CalculateChecksum(chunkData)
	chunkHeader := common.ChunkHeader{ // TODO: version is not needed as it only might impact the chunk storage mechanism, defined by datanode when chunk is received
		ID:       chunkID,
		Index:    index,
		Size:     int64(len(chunkData)),
		Checksum: checksum,
	}

	return chunkHeader, chunkData, nil
}

func (u *Uploader) processWork(work UploaderWork, clientPool client_pool.ClientPool, uploadCtx *uploadContext) error {
	workLogger := logging.OperationLogger(uploadCtx.logger, "upload_chunk", slog.String("chunk_id", work.chunkHeader.ID))

	retryCount := 0
	var replicatedNodes []*common.NodeInfo
	for retryCount < u.config.ChunkRetryCount {
		client, response, err := clientPool.GetClientWithRetry(func(client clients.IDataNodeClient) (bool, any, error) {
			storeChunkResponse, err := client.StoreChunk(context.Background(), work.chunkHeader)
			if err != nil {
				return false, "", err
			}
			return storeChunkResponse.Accept, storeChunkResponse.SessionID, nil
		})
		streamingSessionID := response.(string) // should panic if fails anyway

		if err != nil { // client refused connection
			return fmt.Errorf("failed to connect to all provided clients")
		}

		replicatedNodes, err = u.uploadChunk(work, client, streamingSessionID, uploadCtx)

		if err != nil {
			retryCount++

			if retryCount >= u.config.ChunkRetryCount {
				workLogger.Error("Failed to store chunk after retries", slog.Int("retry_count", retryCount))
				return fmt.Errorf("failed to store chunk %s after %d retries", work.chunkHeader.ID, retryCount)
			}

			time.Sleep(time.Duration(retryCount) * time.Second) // Exponential backoff on upload attempts
			workLogger.Info("Retrying to store chunk", slog.Int("retry_count", retryCount))
			continue
		}

		break
	}

	// Update chunk infos - this information is sent to the coordinator to update the metadata about the chunk
	chunkInfo := &common.ChunkInfo{Header: work.chunkHeader, Replicas: replicatedNodes}
	uploadCtx.chunkInfos.addChunkInfo(chunkInfo)

	return nil
}

func (u *Uploader) uploadChunk(work UploaderWork, client clients.IDataNodeClient, streamingSessionID string,
	uploadCtx *uploadContext) ([]*common.NodeInfo, error) {

	workLogger := logging.OperationLogger(uploadCtx.logger, "upload_chunk", slog.String("chunk_id", work.chunkHeader.ID))
	workLogger.Info("Streaming chunk")

	stream, err := client.UploadChunkStream(uploadCtx.ctx)
	if err != nil {
		workLogger.Error("Failed to create upload stream")
		return nil, fmt.Errorf("failed to create stream for chunk %s: %w", work.chunkHeader.ID, err)
	}

	// Stream chunk to peer
	uploadParams := streaming.UploadChunkStreamParams{
		SessionID:   streamingSessionID,
		ChunkHeader: work.chunkHeader,
		Data:        work.data,
	}
	replicatedNodes, err := u.streamer.SendChunkStream(uploadCtx.ctx, stream, uploadCtx.logger, uploadParams)
	if err != nil {
		workLogger.Error("Failed to stream chunk", slog.String("error", err.Error()))
		return nil, fmt.Errorf("failed to stream chunk %s: %w", work.chunkHeader.ID, err)
	}
	if replicatedNodes == nil {
		workLogger.Error("Datanode failed to replicate chunk", slog.String("chunk_id", work.chunkHeader.ID))
		return nil, fmt.Errorf("datanode failed to replicate chunk %s", work.chunkHeader.ID)
	}

	workLogger.Info("Chunk streaming completed")

	return replicatedNodes, nil
}
