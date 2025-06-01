package datanode

import (
	"github.com/mochivi/distributed-file-system/internal/common"
	"github.com/mochivi/distributed-file-system/pkg/proto"
)

// Store all requests, responses and conversions defined in the proto package
// StoreChunk

type StoreChunkResponse struct {
	Success bool
	Message string
}

func (scr StoreChunkResponse) ToProto() *proto.StoreChunkResponse {
	return &proto.StoreChunkResponse{
		Success: scr.Success,
		Message: scr.Message,
	}
}

// RetrieveChunk

type RetrieveChunkResponse struct {
	Data     []byte
	Checksum string
}

func (rcr RetrieveChunkResponse) ToProto() *proto.RetrieveChunkResponse {
	return &proto.RetrieveChunkResponse{
		Data:     rcr.Data,
		Checksum: rcr.Checksum,
	}
}

type DeleteChunkResponse struct {
	Success bool
	Message string
}

func (dcr DeleteChunkResponse) ToProto() *proto.DeleteChunkResponse {
	return &proto.DeleteChunkResponse{
		Success: dcr.Success,
		Message: dcr.Message,
	}
}

type ReplicateChunkResponse struct {
	Accept    bool
	Message   string
	SessionID string
}

func (rcr ReplicateChunkResponse) ToProto() *proto.ReplicateChunkResponse {
	return &proto.ReplicateChunkResponse{
		Accept:    rcr.Accept,
		Message:   rcr.Message,
		SessionId: rcr.SessionID,
	}
}

type ChunkDataAck struct {
	SessionID     string
	Success       bool
	Message       string
	BytesReceived int
	ReadyForNext  bool
}

func (cda ChunkDataAck) ToProto() *proto.ChunkDataAck {
	return &proto.ChunkDataAck{
		SessionId:     cda.SessionID,
		Success:       cda.Success,
		Message:       cda.Message,
		BytesReceived: int64(cda.BytesReceived),
		ReadyForNext:  cda.ReadyForNext,
	}
}

type HealthCheckResponse struct {
	Status common.HealthStatus
}

func (hcr HealthCheckResponse) ToProto() *proto.HealthCheckResponse {
	return &proto.HealthCheckResponse{Status: hcr.Status.ToProto()}
}
