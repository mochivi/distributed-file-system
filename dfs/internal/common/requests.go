package common

import (
	"fmt"
	"time"

	"github.com/mochivi/distributed-file-system/pkg/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
)

type UploadChunkRequest struct {
	ChunkHeader ChunkHeader
	Propagate   bool
}

func UploadChunkRequestFromProto(pb *proto.UploadChunkRequest) UploadChunkRequest {
	return UploadChunkRequest{
		ChunkHeader: ChunkHeaderFromProto(pb.ChunkHeader),
		Propagate:   pb.Propagate,
	}
}

func (ucr UploadChunkRequest) ToProto() *proto.UploadChunkRequest {
	return &proto.UploadChunkRequest{
		ChunkHeader: ucr.ChunkHeader.ToProto(),
		Propagate:   ucr.Propagate,
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

type DownloadReady struct {
	NodeReady
	ChunkHeader ChunkHeader
}

func DownloadReadyFromProto(pb *proto.DownloadReady) DownloadReady {
	return DownloadReady{
		NodeReady:   NodeReadyFromProto(pb.Ready),
		ChunkHeader: ChunkHeaderFromProto(pb.ChunkHeader),
	}
}

func (dr DownloadReady) ToProto() *proto.DownloadReady {
	return &proto.DownloadReady{
		Ready:       dr.NodeReady.ToProto(),
		ChunkHeader: dr.ChunkHeader.ToProto(),
	}
}

type DownloadStreamRequest struct {
	SessionID       string
	ChunkStreamSize int
}

func DownloadStreamRequestFromProto(pb *proto.DownloadStreamRequest) (DownloadStreamRequest, error) {
	req := DownloadStreamRequest{SessionID: pb.SessionId, ChunkStreamSize: int(pb.ChunkStreamSize)}

	// Limit stream frame size to maximum 1MB
	if req.ChunkStreamSize > 1024*1024 {
		return DownloadStreamRequest{}, fmt.Errorf("chunk stream size is too large")
	}

	// if req.ChunkStreamSize <= 0 {
	// 	pb.ChunkStreamSize = int32(config.DefaultStreamerConfig().ChunkStreamSize)
	// }
	return req, nil
}

func (r DownloadStreamRequest) ToProto() *proto.DownloadStreamRequest {
	return &proto.DownloadStreamRequest{SessionId: r.SessionID, ChunkStreamSize: int32(r.ChunkStreamSize)}
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

type BulkDeleteChunkRequest struct {
	ChunkIDs []string
	Reason   string
}

func BulkDeleteChunkRequestFromProto(pb *proto.BulkDeleteChunkRequest) BulkDeleteChunkRequest {
	return BulkDeleteChunkRequest{
		ChunkIDs: pb.ChunkIds,
		Reason:   pb.Reason,
	}
}

func (bcr BulkDeleteChunkRequest) ToProto() *proto.BulkDeleteChunkRequest {
	return &proto.BulkDeleteChunkRequest{
		ChunkIds: bcr.ChunkIDs,
		Reason:   bcr.Reason,
	}
}

type BulkDeleteChunkResponse struct {
	Success bool
	Message string
	Failed  []string
}

func BulkDeleteChunkResponseFromProto(pb *proto.BulkDeleteChunkResponse) BulkDeleteChunkResponse {
	return BulkDeleteChunkResponse{
		Success: pb.Success,
		Message: pb.Message,
		Failed:  pb.Failed,
	}
}

func (bcr BulkDeleteChunkResponse) ToProto() *proto.BulkDeleteChunkResponse {
	return &proto.BulkDeleteChunkResponse{
		Success: bcr.Success,
		Message: bcr.Message,
		Failed:  bcr.Failed,
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
	Replicas      []*NodeInfo
}

func ChunkDataAckFromProto(pb *proto.ChunkDataAck) ChunkDataAck {
	replicas := make([]*NodeInfo, len(pb.Replicas))
	for i, replica := range pb.Replicas {
		replicas[i] = NodeInfoFromProto(replica)
	}
	return ChunkDataAck{
		SessionID:     pb.SessionId,
		Success:       pb.Success,
		Message:       pb.Message,
		BytesReceived: int(pb.BytesReceived),
		ReadyForNext:  pb.ReadyForNext,
		Replicas:      replicas,
	}
}

func (cda ChunkDataAck) ToProto() *proto.ChunkDataAck {
	replicas := make([]*proto.NodeInfo, len(cda.Replicas))
	for i, replica := range cda.Replicas {
		replicas[i] = replica.ToProto()
	}
	if len(replicas) == 0 {
		replicas = nil
	}

	return &proto.ChunkDataAck{
		SessionId:     cda.SessionID,
		Success:       cda.Success,
		Message:       cda.Message,
		BytesReceived: int64(cda.BytesReceived),
		ReadyForNext:  cda.ReadyForNext,
		Replicas:      replicas,
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

type NodeUpdateType int

const (
	NODE_ADDED NodeUpdateType = iota
	NODE_REMOVED
	NODE_UPDATED
)

type NodeUpdate struct {
	Version   int64
	Type      NodeUpdateType
	Node      *NodeInfo
	Timestamp time.Time
}

func NodeUpdateFromProto(pb *proto.NodeUpdate) NodeUpdate {
	return NodeUpdate{
		Version:   pb.Version,
		Type:      NodeUpdateType(pb.Type),
		Node:      NodeInfoFromProto(pb.Node),
		Timestamp: pb.Timestamp.AsTime(),
	}
}

func (nu NodeUpdate) ToProto() *proto.NodeUpdate {
	return &proto.NodeUpdate{
		Version:   nu.Version,
		Type:      proto.NodeUpdate_UpdateType(nu.Type),
		Node:      nu.Node.ToProto(),
		Timestamp: timestamppb.New(nu.Timestamp),
	}
}

// Coordinator requests
// Upload
type UploadRequest struct {
	Path      string // Path to upload the file
	Size      int    // File size in bytes
	ChunkSize int    // What chunkSize to break it down into
	Checksum  string // File sha256 checksum
}

func UploadRequestFromProto(pb *proto.UploadRequest) (UploadRequest, error) {
	uploadRequest := UploadRequest{
		Path:      pb.Path,
		Size:      int(pb.Size),
		ChunkSize: int(pb.ChunkSize),
		Checksum:  pb.Checksum,
	}
	if err := uploadRequest.validate(); err != nil {
		return UploadRequest{}, err
	}
	return uploadRequest, nil
}

func (r *UploadRequest) ToProto() *proto.UploadRequest {
	return &proto.UploadRequest{
		Path:      r.Path,
		Size:      int64(r.Size),
		ChunkSize: int64(r.ChunkSize),
		Checksum:  r.Checksum,
	}
}

func (r UploadRequest) validate() error {
	if r.ChunkSize < 1*1024*1024 { // 1MB minimun chunksize allowed
		return fmt.Errorf("%w: chunkSize cannot be lower than 1MB", ErrValidation)
	}
	if r.ChunkSize > 128*1024*1024 { // 128MB maximum chunksize allowed
		return fmt.Errorf("%w: chunksize cannot be larger than 128MB", ErrValidation)
	}
	return nil
}

type UploadResponse struct {
	ChunkIDs  []string
	Nodes     []*NodeInfo
	SessionID string
}

func UploadResponseFromProto(pb *proto.UploadResponse) UploadResponse {
	nodes := make([]*NodeInfo, 0, len(pb.Nodes))
	for _, node := range pb.Nodes {
		nodes = append(nodes, NodeInfoFromProto(node))
	}
	return UploadResponse{
		ChunkIDs:  pb.ChunkIds,
		Nodes:     nodes,
		SessionID: pb.SessionId}
}

func (ur UploadResponse) ToProto() *proto.UploadResponse {
	protoNodes := make([]*proto.NodeInfo, 0, len(ur.Nodes))
	for _, node := range ur.Nodes {
		protoNodes = append(protoNodes, node.ToProto())
	}
	return &proto.UploadResponse{
		ChunkIds:  ur.ChunkIDs,
		Nodes:     protoNodes,
		SessionId: ur.SessionID,
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
	FileInfo       FileInfo
	ChunkLocations []ChunkLocation
	SessionID      string
}

func DownloadResponseFromProto(pb *proto.DownloadResponse) DownloadResponse {
	chunkLocations := make([]ChunkLocation, 0, len(pb.ChunkLocations))
	for _, chunkLocation := range pb.ChunkLocations {
		chunkLocations = append(chunkLocations, ChunkLocationFromProto(chunkLocation))
	}
	return DownloadResponse{
		FileInfo:       FileInfoFromProto(pb.FileInfo),
		ChunkLocations: chunkLocations,
		SessionID:      pb.SessionId,
	}
}

func (dr DownloadResponse) ToProto() *proto.DownloadResponse {
	protoChunkLocations := make([]*proto.ChunkLocation, 0, len(dr.ChunkLocations))
	for _, item := range dr.ChunkLocations {
		protoChunkLocations = append(protoChunkLocations, item.ToProto())
	}
	return &proto.DownloadResponse{
		FileInfo:       dr.FileInfo.ToProto(),
		ChunkLocations: protoChunkLocations,
		SessionId:      dr.SessionID,
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

type ConfirmUploadRequest struct {
	SessionID  string
	ChunkInfos []ChunkInfo
}

func ConfirmUploadRequestFromProto(pb *proto.ConfirmUploadRequest) ConfirmUploadRequest {
	chunkInfos := make([]ChunkInfo, 0, len(pb.ChunkInfos))
	for _, chunkInfo := range pb.ChunkInfos {
		chunkInfos = append(chunkInfos, ChunkInfoFromProto(chunkInfo))
	}
	return ConfirmUploadRequest{SessionID: pb.SessionId, ChunkInfos: chunkInfos}
}

func (cur ConfirmUploadRequest) ToProto() *proto.ConfirmUploadRequest {
	chunkInfos := make([]*proto.ChunkInfo, 0, len(cur.ChunkInfos))
	for _, chunkInfo := range cur.ChunkInfos {
		chunkInfos = append(chunkInfos, chunkInfo.ToProto())
	}
	return &proto.ConfirmUploadRequest{SessionId: cur.SessionID, ChunkInfos: chunkInfos}
}

type ConfirmUploadResponse struct {
	Success bool
	Message string
}

func ConfirmUploadResponseFromProto(pb *proto.ConfirmUploadResponse) ConfirmUploadResponse {
	return ConfirmUploadResponse{Success: pb.Success, Message: pb.Message}
}

func (cur ConfirmUploadResponse) ToProto() *proto.ConfirmUploadResponse {
	return &proto.ConfirmUploadResponse{Success: cur.Success, Message: cur.Message}
}

// List
type ListRequest struct {
	Directory string
}

func ListRequestFromProto(pb *proto.ListRequest) ListRequest {
	directory := pb.Directory
	if directory == "" {
		directory = "/"
	}
	return ListRequest{Directory: directory}
}

func (dr ListRequest) ToProto() *proto.ListRequest {
	return &proto.ListRequest{Directory: dr.Directory}
}

type ListResponse struct {
	Files []*FileInfo
}

func ListResponseFromProto(pb *proto.ListResponse) ListResponse {
	files := make([]*FileInfo, 0, len(pb.Files))
	for _, file := range pb.Files {
		info := FileInfoFromProto(file)
		files = append(files, &info)
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
	NodeInfo *NodeInfo
}

func RegisterDataNodeRequestFromProto(pb *proto.RegisterDataNodeRequest) RegisterDataNodeRequest {
	return RegisterDataNodeRequest{NodeInfo: NodeInfoFromProto(pb.NodeInfo)}
}

func (rr RegisterDataNodeRequest) ToProto() *proto.RegisterDataNodeRequest {
	return &proto.RegisterDataNodeRequest{NodeInfo: rr.NodeInfo.ToProto()}
}

type RegisterDataNodeResponse struct {
	Success        bool
	Message        string
	FullNodeList   []*NodeInfo
	CurrentVersion int64
}

func RegisterDataNodeResponseFromProto(pb *proto.RegisterDataNodeResponse) RegisterDataNodeResponse {
	fullNodeList := make([]*NodeInfo, 0, len(pb.FullNodeList))
	for _, protoNode := range pb.FullNodeList {
		fullNodeList = append(fullNodeList, NodeInfoFromProto(protoNode))
	}

	return RegisterDataNodeResponse{
		Success:        pb.Success,
		Message:        pb.Message,
		FullNodeList:   fullNodeList,
		CurrentVersion: pb.CurrentVersion,
	}
}

func (r RegisterDataNodeResponse) ToProto() *proto.RegisterDataNodeResponse {
	fullNodeList := make([]*proto.NodeInfo, 0, len(r.FullNodeList))
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
	Status          HealthStatus
	LastSeenVersion int64
}

func HeartbeatRequestFromProto(pb *proto.HeartbeatRequest) HeartbeatRequest {
	return HeartbeatRequest{
		NodeID:          pb.NodeId,
		Status:          HealthStatusFromProto(pb.Status),
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
	Updates            []NodeUpdate
	FromVersion        int64
	ToVersion          int64
	RequiresFullResync bool
}

func HeartbeatResponseFromProto(pb *proto.HeartbeatResponse) HeartbeatResponse {
	updates := make([]NodeUpdate, 0, len(pb.Updates))
	for _, update := range pb.Updates {
		updates = append(updates, NodeUpdateFromProto(update))
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
	Nodes          []*NodeInfo
	CurrentVersion int64
}

func ListNodesResponseFromProto(pb *proto.ListNodesResponse) ListNodesResponse {
	nodes := make([]*NodeInfo, 0, len(pb.Nodes))
	for _, node := range pb.Nodes {
		nodes = append(nodes, NodeInfoFromProto(node))
	}
	return ListNodesResponse{
		Nodes:          nodes,
		CurrentVersion: pb.CurrentVersion,
	}
}

func (lnr ListNodesResponse) ToProto() *proto.ListNodesResponse {
	nodes := make([]*proto.NodeInfo, 0, len(lnr.Nodes))
	for _, node := range lnr.Nodes {
		nodes = append(nodes, (*node).ToProto())
	}
	return &proto.ListNodesResponse{
		Nodes:          nodes,
		CurrentVersion: lnr.CurrentVersion,
	}
}

// ChunkLocation represents where some chunk should be stored (primary node + endpoint)
type ChunkLocation struct {
	ChunkID string
	Nodes   []*NodeInfo
}

func ChunkLocationFromProto(pb *proto.ChunkLocation) ChunkLocation {
	nodes := make([]*NodeInfo, 0, len(pb.Nodes))
	for _, node := range pb.Nodes {
		nodes = append(nodes, NodeInfoFromProto(node))
	}
	return ChunkLocation{
		ChunkID: pb.ChunkId,
		Nodes:   nodes,
	}
}

func (cs *ChunkLocation) ToProto() *proto.ChunkLocation {
	nodes := make([]*proto.NodeInfo, 0, len(cs.Nodes))
	for _, node := range cs.Nodes {
		nodes = append(nodes, node.ToProto())
	}
	return &proto.ChunkLocation{
		ChunkId: cs.ChunkID,
		Nodes:   nodes,
	}
}

type GetChunksForNodeRequest struct {
	NodeID string
}

func GetChunksForNodeRequestFromProto(pb *proto.GetChunksForNodeRequest) GetChunksForNodeRequest {
	return GetChunksForNodeRequest{NodeID: pb.NodeId}
}

func (gcr GetChunksForNodeRequest) ToProto() *proto.GetChunksForNodeRequest {
	return &proto.GetChunksForNodeRequest{NodeId: gcr.NodeID}
}

type GetChunksForNodeResponse struct {
	ChunkIDs []string
}

func GetChunksForNodeResponseFromProto(pb *proto.GetChunksForNodeResponse) GetChunksForNodeResponse {
	return GetChunksForNodeResponse{ChunkIDs: pb.ChunkIds}
}

func (gcr GetChunksForNodeResponse) ToProto() *proto.GetChunksForNodeResponse {
	return &proto.GetChunksForNodeResponse{ChunkIds: gcr.ChunkIDs}
}
