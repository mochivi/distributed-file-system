package coordinator

import (
	"context"
	"errors"
	"fmt"
	"log/slog"

	"github.com/mochivi/distributed-file-system/internal/apperr"
	"github.com/mochivi/distributed-file-system/internal/cluster/state"
	"github.com/mochivi/distributed-file-system/internal/common"
	"github.com/mochivi/distributed-file-system/internal/storage/metadata"
	"github.com/mochivi/distributed-file-system/pkg/logging"
	"github.com/mochivi/distributed-file-system/pkg/proto"
	"google.golang.org/grpc/codes"
)

// Upload
// 1. Validate if requested Path is available for upload.
// 2. Idenfity candidate data nodes for each data chunk.
// 3. Store metadata about the file. Will set metadata with a validation wrappper, expects confirmation from client
// 3. Reply with where the client should upload each chunk (primary + replicas).
func (c *Coordinator) UploadFile(ctx context.Context, pb *proto.UploadRequest) (*proto.UploadResponse, error) {
	req := common.UploadRequestFromProto(pb)
	ctx, _ = logging.FromContextWithOperation(ctx, common.OpUpload,
		slog.String(common.LogFilePath, req.Path),
		slog.Int(common.LogFileSize, req.Size),
		slog.Int(common.LogChunkSize, req.ChunkSize))

	resp, err := c.service.uploadFile(ctx, req)
	if err != nil {
		if errors.Is(err, state.ErrNoAvailableNodes) {
			return nil, apperr.Wrap(codes.NotFound, "no available nodes", err)
		}
		return nil, err
	}

	return resp.ToProto(), nil
}

// Client request for a file download
func (c *Coordinator) DownloadFile(ctx context.Context, pb *proto.DownloadRequest) (*proto.DownloadResponse, error) {
	req := common.DownloadRequestFromProto(pb)
	ctx, _ = logging.FromContextWithOperation(ctx, common.OpDownload,
		slog.String(common.LogFilePath, req.Path))

	resp, err := c.service.downloadFile(ctx, req)
	if err != nil {
		if errors.Is(err, metadata.ErrNotFound) {
			return nil, apperr.Wrap(codes.NotFound, "file not found in metadata store", err)
		}
		if errors.Is(err, state.ErrNoAvailableNodes) {
			return nil, apperr.Wrap(codes.NotFound, "no available nodes", err)
		}
		return nil, err
	}

	return resp.ToProto(), nil
}

// Client request to list files from some directory
func (c *Coordinator) ListFiles(ctx context.Context, pb *proto.ListRequest) (*proto.ListResponse, error) {
	req := common.ListRequestFromProto(pb)
	ctx, _ = logging.FromContextWithOperation(ctx, common.OpList,
		slog.String(common.LogDirectory, req.Directory))

	resp, err := c.service.listFiles(ctx, req)
	if err != nil {
		return nil, err
	}

	return resp.ToProto(), nil
}

// Client request to delete a file
// 1. Client sends delete request to coordinator
// 2. Coordinator deletes the file from metadata store
// 3. Coordinator replies with success or error
// 4. Background gc process deletes the files from storage
func (c *Coordinator) DeleteFile(ctx context.Context, pb *proto.DeleteRequest) (*proto.DeleteResponse, error) {
	req := common.DeleteRequestFromProto(pb)
	ctx, _ = logging.FromContextWithOperation(ctx, common.OpDelete,
		slog.String(common.LogFilePath, req.Path))

	resp, err := c.service.deleteFile(ctx, req)
	if err != nil {
		if errors.Is(err, metadata.ErrNotFound) {
			return nil, apperr.Wrap(codes.NotFound, "file not found", err)
		}
		return nil, fmt.Errorf("failed to delete file: %w", err)
	}

	return resp.ToProto(), nil
}

func (c *Coordinator) ConfirmUpload(ctx context.Context, pb *proto.ConfirmUploadRequest) (*proto.ConfirmUploadResponse, error) {
	req := common.ConfirmUploadRequestFromProto(pb)
	ctx, _ = logging.FromContextWithOperation(ctx, common.OpCommit,
		slog.String(common.LogMetadataSessionID, req.SessionID))

	resp, err := c.service.confirmUpload(ctx, req)
	if err != nil {
		return nil, err
	}

	return resp.ToProto(), nil
}

// node -> coordinator requests below

// DataNode ingress mechanism
func (c *Coordinator) RegisterDataNode(ctx context.Context, pb *proto.RegisterDataNodeRequest) (*proto.RegisterDataNodeResponse, error) {
	req := common.RegisterDataNodeRequestFromProto(pb)
	ctx, _ = logging.FromContextWithOperation(ctx, common.OpRegister,
		slog.String(common.LogNodeID, req.NodeInfo.ID))

	resp, err := c.service.registerDataNode(ctx, req)
	if err != nil {
		return nil, err
	}

	return resp.ToProto(), nil
}

// DataNodes periodically communicate their status to the coordinator
func (c *Coordinator) DataNodeHeartbeat(ctx context.Context, pb *proto.HeartbeatRequest) (*proto.HeartbeatResponse, error) {
	req := common.HeartbeatRequestFromProto(pb)

	ctx, _ = logging.FromContextWithOperation(ctx, common.OpHeartbeat,
		slog.String(common.LogNodeID, req.NodeID))

	resp, err := c.service.heartbeat(ctx, req)
	if err != nil {
		if errors.Is(err, state.ErrNotFound) {
			return common.HeartbeatResponse{
				Success: false,
				Message: "node is not registered",
			}.ToProto(), nil
		}
		if errors.Is(err, state.ErrVersionTooOld) {
			return &proto.HeartbeatResponse{
				Success:            true,
				Message:            "version too old",
				RequiresFullResync: true,
			}, nil
		}
		return nil, err
	}

	return resp.ToProto(), nil
}

// ListNodes returns a list of all registered data nodes
func (c *Coordinator) ListNodes(ctx context.Context, pb *proto.ListNodesRequest) (*proto.ListNodesResponse, error) {
	req := common.ListNodesRequestFromProto(pb)
	ctx, _ = logging.FromContextWithOperation(ctx, common.OpListNodes)

	resp, err := c.service.listNodes(ctx, req)
	if err != nil {
		return nil, err
	}

	return resp.ToProto(), nil
}

// Todo: this will be replaced by getting chunks for node from the distributed metadata store, not by asking the coordinator
func (c *Coordinator) GetChunksForNode(ctx context.Context, pb *proto.GetChunksForNodeRequest) (*proto.GetChunksForNodeResponse, error) {
	return nil, nil
}
