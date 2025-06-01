package datanode

import (
	"github.com/mochivi/distributed-file-system/internal/common"
	"github.com/mochivi/distributed-file-system/pkg/proto"
)

// Store all requests, responses and conversions defined in the proto package
// StoreChunk
type StoreChunkRequest struct {
	ChunkID  string
	Data     []byte
	Checksum string
}

func StoreChunkRequestFromProto(pb *proto.StoreChunkRequest) StoreChunkRequest {
	return StoreChunkRequest{
		ChunkID:  pb.ChunkID,
		Data:     pb.Data,
		Checksum: pb.Checksum,
	}
}

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
type RetrieveChunkRequest struct {
	ChunkID string
}

func RetrieveChunkRequestFromProto(pb *proto.RetrieveChunkRequest) RetrieveChunkRequest {
	return RetrieveChunkRequest{ChunkID: pb.ChunkId}
}

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

type DeleteChunkRequest struct {
	ChunkID string
}

func DeleteChunkRequestFromProto(pb *proto.DeleteChunkRequest) DeleteChunkRequest {
	return DeleteChunkRequest{ChunkID: pb.ChunkId}
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

type ReplicateChunkRequest struct {
	ChunkID   string
	ChunkSize int
	Checksum  string
}

func ReplicateChunkRequestFromProto(pb *proto.ReplicateChunkRequest) ReplicateChunkRequest {
	return ReplicateChunkRequest{
		ChunkID:   pb.ChunkId,
		ChunkSize: int(pb.ChunkSize),
		Checksum:  pb.Checksum,
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

type ChunkDataStream struct {
	SessionID       string
	ChunkID         string
	Data            []byte
	Offset          int
	IsFinal         bool
	PartialChecksum string
}

func ChunkDataStreamFromProto(pb *proto.ChunkDataStream) ChunkDataStream {
	return ChunkDataStream{
		SessionID:       pb.SessionId,
		ChunkID:         pb.ChunkId,
		Data:            pb.Data,
		Offset:          int(pb.Offset),
		IsFinal:         pb.IsFinal,
		PartialChecksum: pb.PartialChecksum,
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

type HealthCheckRequest struct{}
type HealthCheckResponse struct {
	Status common.HealthStatus
}

func (hcr HealthCheckResponse) ToProto() *proto.HealthCheckResponse {
	return &proto.HealthCheckResponse{Status: hcr.Status.ToProto()}
}
