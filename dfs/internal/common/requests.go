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

type RetrieveChunkRequest struct {
	ChunkID string
}

func RetrieveChunkRequestFromProto(pb *proto.RetrieveChunkRequest) RetrieveChunkRequest {
	return RetrieveChunkRequest{ChunkID: pb.ChunkId}
}
func (rtr RetrieveChunkRequest) ToProto() *proto.RetrieveChunkRequest {
	return &proto.RetrieveChunkRequest{ChunkId: rtr.ChunkID}
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

type HealthCheckRequest struct{}

func HealthCheckRequestFromProto(pb *proto.HealthCheckRequest) HealthCheckRequest {
	return HealthCheckRequest{}
}

func (hcr HealthCheckRequest) ToProto() *proto.HealthCheckRequest {
	return &proto.HealthCheckRequest{}
}
