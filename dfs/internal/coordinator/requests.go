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
	ChunkLocations []ChunkLocation
}

func UploadResponseFromProto(pb *proto.UploadResponse) UploadResponse {
	chunkLocations := make([]ChunkLocation, 0, len(pb.ChunkLocations))
	for _, chunkLocation := range pb.ChunkLocations {
		chunkLocations = append(chunkLocations, ChunkLocationFromProto(chunkLocation))
	}
	return UploadResponse{ChunkLocations: chunkLocations}
}

func (ur UploadResponse) ToProto() *proto.UploadResponse {
	chunkLocations := make([]*proto.ChunkLocation, 0, len(ur.ChunkLocations))
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
	Success        bool
	Message        string
	FullNodeList   []*common.DataNodeInfo
	CurrentVersion int64
}

func RegisterDataNodeResponseFromProto(pb *proto.RegisterDataNodeResponse) RegisterDataNodeResponse {
	fullNodeList := make([]*common.DataNodeInfo, 0, len(pb.FullNodeList))
	for _, protoNode := range pb.FullNodeList {
		node := common.DataNodeInfoFromProto(protoNode)
		fullNodeList = append(fullNodeList, &node)
	}

	return RegisterDataNodeResponse{
		Success:        pb.Success,
		Message:        pb.Message,
		FullNodeList:   fullNodeList,
		CurrentVersion: pb.CurrentVersion,
	}
}

func (r RegisterDataNodeResponse) ToProto() *proto.RegisterDataNodeResponse {
	fullNodeList := make([]*proto.DataNodeInfo, 0, len(r.FullNodeList))
	for _, node := range r.FullNodeList {
		fullNodeList = append(fullNodeList, node.ToProto())
	}

	return &proto.RegisterDataNodeResponse{
		Success:        r.Success,
		Message:        r.Message,
		FullNodeList:   fullNodeList,
		CurrentVersion: r.CurrentVersion,
	}
}

// Heartbeat
type HeartbeatRequest struct {
	NodeID          string
	Status          common.HealthStatus
	LastSeenVersion int64
}

func HeartbeatRequestFromProto(pb *proto.HeartbeatRequest) HeartbeatRequest {
	return HeartbeatRequest{
		NodeID:          pb.NodeId,
		Status:          common.HealthStatusFromProto(pb.Status),
		LastSeenVersion: pb.LastSeenVersion,
	}
}

func (hr HeartbeatRequest) ToProto() *proto.HeartbeatRequest {
	return &proto.HeartbeatRequest{
		NodeId:          hr.NodeID,
		Status:          hr.Status.ToProto(),
		LastSeenVersion: hr.LastSeenVersion,
	}
}

type HeartbeatResponse struct {
	Success            bool
	Message            string
	Updates            []common.NodeUpdate
	FromVersion        int64
	ToVersion          int64
	RequiresFullResync bool
}

func HeartbeatResponseFromProto(pb *proto.HeartbeatResponse) HeartbeatResponse {
	updates := make([]common.NodeUpdate, 0, len(pb.Updates))
	for _, update := range pb.Updates {
		updates = append(updates, common.NodeUpdateFromProto(update))
	}

	return HeartbeatResponse{
		Success:            pb.Success,
		Message:            pb.Message,
		Updates:            updates,
		FromVersion:        pb.FromVersion,
		ToVersion:          pb.ToVersion,
		RequiresFullResync: pb.RequiresFullResync,
	}
}

func (hr HeartbeatResponse) ToProto() *proto.HeartbeatResponse {
	updates := make([]*proto.NodeUpdate, 0, len(hr.Updates))
	for _, update := range hr.Updates {
		updates = append(updates, update.ToProto())
	}

	return &proto.HeartbeatResponse{
		Success:            hr.Success,
		Message:            hr.Message,
		Updates:            updates,
		FromVersion:        hr.FromVersion,
		ToVersion:          hr.ToVersion,
		RequiresFullResync: hr.RequiresFullResync,
	}
}

type ListNodesRequest struct {
}

func ListNodesRequestFromProto(pb *proto.ListNodesRequest) ListNodesRequest {
	return ListNodesRequest{}
}

func (lnr ListNodesRequest) ToProto() *proto.ListNodesRequest { return &proto.ListNodesRequest{} }

type ListNodesResponse struct {
	Nodes          []*common.DataNodeInfo
	CurrentVersion int64
}

func ListNodesResponseFromProto(pb *proto.ListNodesResponse) ListNodesResponse {
	nodes := make([]*common.DataNodeInfo, 0, len(pb.Nodes))
	for _, node := range pb.Nodes {
		nodeInfo := common.DataNodeInfoFromProto(node)
		nodes = append(nodes, &nodeInfo)
	}
	return ListNodesResponse{
		Nodes:          nodes,
		CurrentVersion: pb.CurrentVersion,
	}
}

func (lnr ListNodesResponse) ToProto() *proto.ListNodesResponse {
	nodes := make([]*proto.DataNodeInfo, 0, len(lnr.Nodes))
	for _, node := range lnr.Nodes {
		nodes = append(nodes, (*node).ToProto())
	}
	return &proto.ListNodesResponse{
		Nodes:          nodes,
		CurrentVersion: lnr.CurrentVersion,
	}
}
