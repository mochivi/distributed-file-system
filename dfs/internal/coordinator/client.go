package coordinator

import (
	"context"
	"fmt"

	"google.golang.org/grpc"
)

func (c *CoordinatorClient) UploadFile(ctx context.Context, req UploadRequest, opts ...grpc.CallOption) (UploadResponse, error) {
	resp, err := c.client.UploadFile(ctx, req.ToProto(), opts...)
	if err != nil {
		return UploadResponse{}, fmt.Errorf("failed to upload file: %w", err)
	}
	return UploadResponseFromProto(resp), nil
}

func (c *CoordinatorClient) DownloadFile(ctx context.Context, req DownloadRequest, opts ...grpc.CallOption) (DownloadResponse, error) {
	resp, err := c.client.DownloadFile(ctx, req.ToProto(), opts...)
	if err != nil {
		return DownloadResponse{}, fmt.Errorf("failed to upload file: %w", err)
	}
	return DownloadResponseFromProto(resp), nil
}

func (c *CoordinatorClient) DeleteFile(ctx context.Context, req DeleteRequest, opts ...grpc.CallOption) (DeleteResponse, error) {
	resp, err := c.client.DeleteFile(ctx, req.ToProto(), opts...)
	if err != nil {
		return DeleteResponse{}, fmt.Errorf("failed to upload file: %w", err)
	}
	return DeleteResponseFromProto(resp), nil
}

func (c *CoordinatorClient) ListFiles(ctx context.Context, req ListRequest, opts ...grpc.CallOption) (ListResponse, error) {
	resp, err := c.client.ListFiles(ctx, req.ToProto(), opts...)
	if err != nil {
		return ListResponse{}, fmt.Errorf("failed to upload file: %w", err)
	}
	return ListResponseFromProto(resp), nil
}

func (c *CoordinatorClient) RegisterDataNode(ctx context.Context, req RegisterDataNodeRequest, opts ...grpc.CallOption) (RegisterDataNodeResponse, error) {
	resp, err := c.client.RegisterDataNode(ctx, req.ToProto(), opts...)
	if err != nil {
		return RegisterDataNodeResponse{}, fmt.Errorf("failed to upload file: %w", err)
	}
	return RegisterDataNodeResponseFromProto(resp), nil
}

func (c *CoordinatorClient) DataNodeHeartbeat(ctx context.Context, req HeartbeatRequest, opts ...grpc.CallOption) (HeartbeatResponse, error) {
	resp, err := c.client.DataNodeHeartbeat(ctx, req.ToProto(), opts...)
	if err != nil {
		return HeartbeatResponse{}, fmt.Errorf("failed to upload file: %w", err)
	}
	return HeartbeatResponseFromProto(resp), nil
}
