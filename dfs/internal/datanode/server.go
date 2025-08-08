package datanode

import (
	"context"
	"errors"
	"log/slog"

	"github.com/mochivi/distributed-file-system/internal/apperr"
	"github.com/mochivi/distributed-file-system/internal/common"
	"github.com/mochivi/distributed-file-system/internal/storage/chunk"
	"github.com/mochivi/distributed-file-system/pkg/logging"
	"github.com/mochivi/distributed-file-system/pkg/proto"
	"github.com/mochivi/distributed-file-system/pkg/streaming"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
)

// TODO: make this configurable by the client
const (
	N_REPLICAS int = 3
	N_NODES    int = (N_REPLICAS-1)*2 + 1
)

// PrepareChunkUpload is received only if the node is a primary receiver, should reply to accept the chunk
// and create a streaming session for the chunk data stream
func (s *DataNodeServer) PrepareChunkUpload(ctx context.Context, pb *proto.UploadChunkRequest) (*proto.NodeReady, error) {
	req := common.UploadChunkRequestFromProto(pb)
	ctx, _ = logging.FromContextWithOperation(ctx, common.OpUpload, slog.String(common.LogChunkID, req.ChunkHeader.ID))

	resp, err := s.service.prepareChunkUpload(ctx, req)
	if err != nil {
		if errors.Is(err, streaming.ErrSessionAlreadyExists) {
			return nil, apperr.Internal(err) // Tried to create a duplicate session -- the session ID is server generated, so something went wrong
		}
		return nil, err
	}
	return resp.ToProto(), nil
}

// TODO: add a different session for download, as it is not the same as the upload session
func (s *DataNodeServer) PrepareChunkDownload(ctx context.Context, pb *proto.DownloadChunkRequest) (*proto.DownloadReady, error) {
	req := common.DownloadChunkRequestFromProto(pb)
	ctx, _ = logging.FromContextWithOperation(ctx, common.OpDownload, slog.String(common.LogChunkID, req.ChunkID))

	resp, err := s.service.prepareChunkDownload(ctx, req)
	if err != nil {
		if errors.Is(err, chunk.ErrInvalidChunkID) {
			return nil, apperr.InvalidArgument("invalid chunk ID", err)
		}
		if errors.Is(err, streaming.ErrSessionAlreadyExists) {
			return nil, apperr.Internal(err) // Tried to create a duplicate session -- the session ID is server generated, so something went wrong
		}
		return nil, err
	}
	return resp.ToProto(), nil
}

func (s *DataNodeServer) DeleteChunk(ctx context.Context, pb *proto.DeleteChunkRequest) (*proto.DeleteChunkResponse, error) {
	req := common.DeleteChunkRequestFromProto(pb)
	ctx, _ = logging.FromContextWithOperation(ctx, common.OpDelete, slog.String(common.LogChunkID, req.ChunkID))

	resp, err := s.service.deleteChunk(ctx, req)
	if err != nil {
		if errors.Is(err, chunk.ErrInvalidChunkID) {
			return nil, apperr.InvalidArgument("invalid chunk ID", err)
		}
		return nil, err
	}
	return resp.ToProto(), nil
}

func (s *DataNodeServer) BulkDeleteChunk(ctx context.Context, pb *proto.BulkDeleteChunkRequest) (*proto.BulkDeleteChunkResponse, error) {
	req := common.BulkDeleteChunkRequestFromProto(pb)
	ctx, _ = logging.FromContextWithOperation(ctx, common.OpBulkDelete, slog.Int(common.LogNumChunks, len(req.ChunkIDs)))

	resp, err := s.service.bulkDeleteChunk(ctx, req)
	if err != nil {
		return nil, err
	}
	return resp.ToProto(), nil
}

// This is the side that is responsible for receiving the chunk data from some peer (client or another node)
// TODO: not taking into account the partialChecksum on each chunk, so it would be a good idea to ask for a retry in case that any stream frame fails.
// TODO: context awareness for cancelling upload request?
// TODO: add logging to session? So that we can pull the context from the prepare chunk upload request
func (s *DataNodeServer) UploadChunkStream(stream grpc.BidiStreamingServer[proto.ChunkDataStream, proto.ChunkDataAck]) error {
	if err := s.service.uploadChunkStream(stream); err != nil {
		if errors.Is(err, streaming.ErrSessionNotFound) {
			return apperr.Wrap(codes.NotFound, "session not found", err)
		}
		if errors.Is(err, streaming.ErrSessionExpired) {
			return apperr.Wrap(codes.NotFound, "session expired", err)
		}
		if errors.Is(err, streaming.ErrChecksumMismatch) {
			return apperr.Internal(err)
		}
		if errors.Is(err, streaming.ErrAckSendFailed) {
			return apperr.Internal(err)
		}
		return err
	}
	return nil
}

func (s *DataNodeServer) DownloadChunkStream(pb *proto.DownloadStreamRequest, stream grpc.ServerStreamingServer[proto.ChunkDataStream]) error {
	req, err := common.DownloadStreamRequestFromProto(pb)
	if err != nil {
		return apperr.InvalidArgument("invalid download stream request", err)
	}
	if err := s.service.downloadChunkStream(req, stream); err != nil {
		if errors.Is(err, streaming.ErrSessionNotFound) {
			return apperr.Wrap(codes.NotFound, "session not found", err)
		}
		if errors.Is(err, streaming.ErrSessionExpired) {
			return apperr.Wrap(codes.NotFound, "session expired", err)
		}
		return err
	}
	return nil
}

// Even though the general operation is based off of heartbeat requests from the datanode to the coordiantor
// It might be useful to still have a healthcheck endpoint for the datanode
func (s *DataNodeServer) HealthCheck(ctx context.Context, pb *proto.HealthCheckRequest) (*proto.HealthCheckResponse, error) {
	return nil, nil
}
