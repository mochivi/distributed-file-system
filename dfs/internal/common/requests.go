package common

import "github.com/mochivi/distributed-file-system/pkg/proto"

// Coordinates the chunk upload process, if propagate is true, the peer must replicate the chunk to other nodes
type ChunkMeta struct {
	ChunkID   string
	ChunkSize int
	Checksum  string
	Propagate bool
}

func ChunkMetaFromProto(pb *proto.ChunkMeta) ChunkMeta {
	return ChunkMeta{
		ChunkID:   pb.ChunkId,
		ChunkSize: int(pb.ChunkSize),
		Checksum:  pb.Checksum,
		Propagate: pb.Propagate,
	}
}

func (cm ChunkMeta) ToProto() *proto.ChunkMeta {
	return &proto.ChunkMeta{
		ChunkId:   cm.ChunkID,
		ChunkSize: int64(cm.ChunkSize),
		Checksum:  cm.Checksum,
		Propagate: cm.Propagate,
	}
}

// Coordinates the chunk upload process, if accept is true, the peer can start streaming the chunk data
type ChunkUploadReady struct {
	Accept    bool
	Message   string
	SessionID string
}

func ChunkUploadReadyFromProto(pb *proto.ChunkUploadReady) ChunkUploadReady {
	return ChunkUploadReady{
		Accept:    pb.Accept,
		Message:   pb.Message,
		SessionID: pb.SessionId,
	}
}

func (cuu ChunkUploadReady) ToProto() *proto.ChunkUploadReady {
	return &proto.ChunkUploadReady{
		Accept:    cuu.Accept,
		Message:   cuu.Message,
		SessionId: cuu.SessionID,
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
