package common

import "github.com/mochivi/distributed-file-system/pkg/proto"

type StoreChunkRequest struct {
	ChunkID  string
	Data     []byte
	Checksum string
}

func StoreChunkRequestFromProto(pb *proto.StoreChunkRequest) StoreChunkRequest {
	return StoreChunkRequest{
		ChunkID:  pb.ChunkId,
		Data:     pb.Data,
		Checksum: pb.Checksum,
	}
}

func (str StoreChunkRequest) ToProto() *proto.StoreChunkRequest {
	return &proto.StoreChunkRequest{
		ChunkId:  str.ChunkID,
		Data:     str.Data,
		Checksum: str.Checksum,
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

type RetrieveChunkRequest struct {
	ChunkID string
}

func RetrieveChunkRequestFromProto(pb *proto.RetrieveChunkRequest) RetrieveChunkRequest {
	return RetrieveChunkRequest{ChunkID: pb.ChunkId}
}
func (rtr RetrieveChunkRequest) ToProto() *proto.RetrieveChunkRequest {
	return &proto.RetrieveChunkRequest{ChunkId: rtr.ChunkID}
}

type RetrieveChunkResponse struct {
	Data     []byte
	Checksum string
}

func RetrieveChunkResponseFromProto(pb *proto.RetrieveChunkResponse) RetrieveChunkResponse {
	return RetrieveChunkResponse{
		Data:     pb.Data,
		Checksum: pb.Checksum,
	}
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

func (dcr DeleteChunkRequest) ToProto() *proto.DeleteChunkRequest {
	return &proto.DeleteChunkRequest{ChunkId: dcr.ChunkID}
}

type DeleteChunkResponse struct {
	Success bool
	Message string
}

func DeleteChunkResponseFromProto(pb *proto.DeleteChunkResponse) DeleteChunkResponse {
	return DeleteChunkResponse{
		Success: pb.Success,
		Message: pb.Message,
	}
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

func (rpr ReplicateChunkRequest) ToProto() *proto.ReplicateChunkRequest {
	return &proto.ReplicateChunkRequest{
		ChunkId:   rpr.ChunkID,
		ChunkSize: int64(rpr.ChunkSize),
		Checksum:  rpr.Checksum,
	}
}

type ReplicateChunkResponse struct {
	Accept    bool
	Message   string
	SessionID string
}

func ReplicateChunkResponseFromProto(pb *proto.ReplicateChunkResponse) ReplicateChunkResponse {
	return ReplicateChunkResponse{
		Accept:    pb.Accept,
		Message:   pb.Message,
		SessionID: pb.SessionId,
	}
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

func (cds ChunkDataStream) ToProto() *proto.ChunkDataStream {
	return &proto.ChunkDataStream{
		SessionId:       cds.SessionID,
		ChunkId:         cds.ChunkID,
		Data:            cds.Data,
		Offset:          int64(cds.Offset),
		IsFinal:         cds.IsFinal,
		PartialChecksum: cds.PartialChecksum,
	}
}

type ChunkDataAck struct {
	SessionID     string
	Success       bool
	Message       string
	BytesReceived int
	ReadyForNext  bool
}

func ChunkDataAckFromProto(pb *proto.ChunkDataAck) ChunkDataAck {
	return ChunkDataAck{
		SessionID:     pb.SessionId,
		Success:       pb.Success,
		Message:       pb.Message,
		BytesReceived: int(pb.BytesReceived),
		ReadyForNext:  pb.ReadyForNext,
	}
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

func HealthCheckRequestFromProto(pb *proto.HealthCheckRequest) HealthCheckRequest {
	return HealthCheckRequest{}
}

func (hcr HealthCheckRequest) ToProto() *proto.HealthCheckRequest {
	return &proto.HealthCheckRequest{}
}

type HealthCheckResponse struct {
	Status HealthStatus
}

func HealthCheckResponseFromProto(pb *proto.HealthCheckResponse) HealthCheckResponse {
	return HealthCheckResponse{Status: HealthStatusFromProto(pb.Status)}
}

func (hcr HealthCheckResponse) ToProto() *proto.HealthCheckResponse {
	return &proto.HealthCheckResponse{Status: hcr.Status.ToProto()}
}
