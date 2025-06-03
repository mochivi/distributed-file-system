package coordinator

import (
	"github.com/mochivi/distributed-file-system/internal/common"
	"github.com/mochivi/distributed-file-system/pkg/proto"
)

// Request/response pairs

// Upload
type UploadRequest struct {
	Path      string // Path to upload the file
	Size      int    // File size in bytes
	ChunkSize int    // What chunkSize to break it down into
	Checksum  string // File sha256 checksum
}

func newUploadRequestFromProto(pb *proto.UploadRequest) UploadRequest {
	return UploadRequest{
		Path:      pb.Path,
		Size:      int(pb.Size),
		ChunkSize: int(pb.ChunkSize),
		Checksum:  pb.Checksum,
	}
}

func (ur UploadRequest) ToProto() *proto.UploadRequest {
	return &proto.UploadRequest{
		Path:      ur.Path,
		Size:      int64(ur.Size),
		ChunkSize: int64(ur.ChunkSize),
		Checksum:  ur.Checksum,
	}
}

type UploadResponse struct {
	ChunkLocations []ChunkLocation //
}

func UploadResponseFromProto(pb *proto.UploadResponse) UploadResponse {
	chunkLocations := make([]ChunkLocation, 0, len(pb.ChunkLocations))
	for _, chunkLocation := range pb.ChunkLocations {
		chunkLocations = append(chunkLocations, ChunkLocationFromProto(chunkLocation))
	}
	return UploadResponse{ChunkLocations: chunkLocations}
}

func (ur UploadResponse) ToProto() *proto.UploadResponse {
	chunkLocations := make([]*proto.ChunkLocation, len(ur.ChunkLocations))
	for _, item := range ur.ChunkLocations {
		chunkLocations = append(chunkLocations, item.ToProto())
	}
	return &proto.UploadResponse{
		ChunkLocations: chunkLocations,
	}
}

// Download
type DownloadRequest struct {
	Path string
}

func DownloadRequestFromProto(pb *proto.DownloadRequest) DownloadRequest {
	return DownloadRequest{Path: pb.Path}
}

func (dr DownloadRequest) ToProto() *proto.DownloadRequest {
	return &proto.DownloadRequest{Path: dr.Path}
}

type DownloadResponse struct {
	fileInfo       common.FileInfo
	chunkLocations []ChunkLocation
}

func DownloadResponseFromProto(pb *proto.DownloadResponse) DownloadResponse {
	chunkLocations := make([]ChunkLocation, 0, len(pb.ChunkLocations))
	for _, chunkLocation := range pb.ChunkLocations {
		chunkLocations = append(chunkLocations, ChunkLocationFromProto(chunkLocation))
	}
	return DownloadResponse{
		fileInfo:       common.FileInfoFromProto(pb.FileInfo),
		chunkLocations: chunkLocations,
	}
}

func (dr DownloadResponse) ToProto() *proto.DownloadResponse {
	protoChunkLocations := make([]*proto.ChunkLocation, len(dr.chunkLocations))
	for _, item := range dr.chunkLocations {
		protoChunkLocations = append(protoChunkLocations, item.ToProto())
	}
	return &proto.DownloadResponse{
		FileInfo:       dr.fileInfo.ToProto(),
		ChunkLocations: protoChunkLocations,
	}
}

// Delete
type DeleteRequest struct {
	Path string
}

func DeleteRequestFromProto(pb *proto.DeleteRequest) DeleteRequest {
	return DeleteRequest{Path: pb.Path}
}

func (dr DeleteRequest) ToProto() *proto.DeleteRequest {
	return &proto.DeleteRequest{Path: dr.Path}
}

type DeleteResponse struct {
	Success bool
	Message string
}

func DeleteResponseFromProto(pb *proto.DeleteResponse) DeleteResponse {
	return DeleteResponse{
		Success: pb.Success,
		Message: pb.Message,
	}
}

func (dr DeleteResponse) ToProto() *proto.DeleteResponse {
	return &proto.DeleteResponse{
		Success: dr.Success,
		Message: dr.Message,
	}
}

// List
type ListRequest struct {
	Directory string
}

func ListRequestFromProto(pb *proto.ListRequest) ListRequest {
	return ListRequest{Directory: pb.Directory}
}

func (dr ListRequest) ToProto() *proto.ListRequest {
	return &proto.ListRequest{Directory: dr.Directory}
}

type ListResponse struct {
	Files []common.FileInfo
}

func ListResponseFromProto(pb *proto.ListResponse) ListResponse {
	files := make([]common.FileInfo, 0, len(pb.Files))
	for _, file := range pb.Files {
		files = append(files, common.FileInfoFromProto(file))
	}
	return ListResponse{Files: files}
}

func (lr ListResponse) ToProto() *proto.ListResponse {
	files := make([]*proto.FileInfo, 0, len(lr.Files))
	for _, file := range lr.Files {
		files = append(files, file.ToProto())
	}
	return &proto.ListResponse{Files: files}
}

// Data nodes registration
type RegisterDataNodeRequest struct {
	NodeInfo common.DataNodeInfo
}

func RegisterDataNodeRequestFromProto(pb *proto.RegisterDataNodeRequest) RegisterDataNodeRequest {
	return RegisterDataNodeRequest{NodeInfo: common.DataNodeInfoFromProto(pb.NodeInfo)}
}

func (rr RegisterDataNodeRequest) ToProto() *proto.RegisterDataNodeRequest {
	return &proto.RegisterDataNodeRequest{NodeInfo: rr.NodeInfo.ToProto()}
}

type RegisterDataNodeResponse struct {
	Success bool
	Message string
}

func RegisterDataNodeResponseFromProto(pb *proto.RegisterDataNodeResponse) RegisterDataNodeResponse {
	return RegisterDataNodeResponse{
		Success: pb.Success,
		Message: pb.Message,
	}
}

func (r RegisterDataNodeResponse) ToProto() *proto.RegisterDataNodeResponse {
	return &proto.RegisterDataNodeResponse{
		Success: r.Success,
		Message: r.Message,
	}
}

// Heartbeat
type HeartbeatRequest struct {
	NodeID string
	Status common.HealthStatus
}

func HeartbeatRequestFromProto(pb *proto.HeartbeatRequest) HeartbeatRequest {
	return HeartbeatRequest{
		NodeID: pb.NodeId,
		Status: common.HealthStatusFromProto(pb.Status),
	}
}

func (hr HeartbeatRequest) ToProto() *proto.HeartbeatRequest {
	return &proto.HeartbeatRequest{
		NodeId: hr.NodeID,
		Status: hr.Status.ToProto(),
	}
}

type HeartbeatResponse struct {
	Success bool
}

func HeartbeatResponseFromProto(pb *proto.HeartbeatResponse) HeartbeatResponse {
	return HeartbeatResponse{Success: pb.Success}
}

func (hr HeartbeatResponse) ToProto() *proto.HeartbeatResponse {
	return &proto.HeartbeatResponse{Success: hr.Success}
}
