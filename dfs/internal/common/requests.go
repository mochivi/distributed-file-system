package common

import "github.com/mochivi/distributed-file-system/pkg/proto"

type UploadChunkRequest struct {
	ChunkInfo ChunkInfo
	Propagate bool
}

func UploadChunkRequestFromProto(pb *proto.UploadChunkRequest) UploadChunkRequest {
	return UploadChunkRequest{
		ChunkInfo: ChunkInfoFromProto(pb.ChunkInfo),
		Propagate: pb.Propagate,
	}
}

func (ucr UploadChunkRequest) ToProto() *proto.UploadChunkRequest {
	return &proto.UploadChunkRequest{
		ChunkInfo: ucr.ChunkInfo.ToProto(),
		Propagate: ucr.Propagate,
	}
}

// Coordinates the chunk upload process, if accept is true, the peer can start streaming the chunk data
type NodeReady struct {
	Accept    bool
	Message   string
	SessionID string
}

func NodeReadyFromProto(pb *proto.NodeReady) NodeReady {
	return NodeReady{
		Accept:    pb.Accept,
		Message:   pb.Message,
		SessionID: pb.SessionId,
	}
}

func (cuu NodeReady) ToProto() *proto.NodeReady {
	return &proto.NodeReady{
		Accept:    cuu.Accept,
		Message:   cuu.Message,
		SessionId: cuu.SessionID,
	}
}

type DownloadChunkRequest struct {
	ChunkID string
}

func DownloadChunkRequestFromProto(pb *proto.DownloadChunkRequest) DownloadChunkRequest {
	return DownloadChunkRequest{ChunkID: pb.ChunkId}
}
func (r DownloadChunkRequest) ToProto() *proto.DownloadChunkRequest {
	return &proto.DownloadChunkRequest{ChunkId: r.ChunkID}
}

type DownloadStreamRequest struct {
	SessionID string
}

func DownloadStreamRequestFromProto(pb *proto.DownloadStreamRequest) DownloadStreamRequest {
	return DownloadStreamRequest{SessionID: pb.SessionId}
}

func (r DownloadStreamRequest) ToProto() *proto.DownloadStreamRequest {
	return &proto.DownloadStreamRequest{SessionId: r.SessionID}
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
