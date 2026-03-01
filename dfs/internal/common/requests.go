package common

import (
	"errors"
	"fmt"
	"time"

	"github.com/mochivi/distributed-file-system/pkg/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
)

type UploadChunkRequest struct {
	ChunkHeader ChunkHeader
	Propagate   bool
}

func UploadChunkRequestFromProto(pb *proto.UploadChunkRequest) (UploadChunkRequest, error) {
	header, err := ChunkHeaderFromProto(pb.ChunkHeader)
	if err != nil {
		return UploadChunkRequest{}, err
	}
	return UploadChunkRequest{
		ChunkHeader: header,
		Propagate:   pb.Propagate,
	}, nil
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
	SessionID StreamingSessionID
}

func NodeReadyFromProto(pb *proto.NodeReady) NodeReady {
	return NodeReady{
		Accept:    pb.Accept,
		Message:   pb.Message,
		SessionID: StreamingSessionID(pb.SessionId),
	}
}

func (cuu NodeReady) ToProto() *proto.NodeReady {
	return &proto.NodeReady{
		Accept:    cuu.Accept,
		Message:   cuu.Message,
		SessionId: cuu.SessionID.String(),
	}
}

type DownloadChunkRequest struct {
	ChunkID ChunkID
}

func DownloadChunkRequestFromProto(pb *proto.DownloadChunkRequest) (DownloadChunkRequest, error) {
	chunkID, err := NewChunkID(pb.ChunkId)
	if err != nil {
		return DownloadChunkRequest{}, err
	}
	return DownloadChunkRequest{ChunkID: chunkID}, nil
}

func (r DownloadChunkRequest) ToProto() *proto.DownloadChunkRequest {
	return &proto.DownloadChunkRequest{ChunkId: r.ChunkID.String()}
}

type DownloadReady struct {
	NodeReady
	ChunkHeader ChunkHeader
}

func DownloadReadyFromProto(pb *proto.DownloadReady) (DownloadReady, error) {
	header, err := ChunkHeaderFromProto(pb.ChunkHeader)
	if err != nil {
		return DownloadReady{}, err
	}
	return DownloadReady{
		NodeReady:   NodeReadyFromProto(pb.Ready),
		ChunkHeader: header,
	}, nil
}

func (r DownloadReady) ToProto() *proto.DownloadReady {
	return &proto.DownloadReady{
		Ready:       r.NodeReady.ToProto(),
		ChunkHeader: r.ChunkHeader.ToProto(),
	}
}

type DownloadStreamRequest struct {
	SessionID       StreamingSessionID
	ChunkStreamSize int
}

func DownloadStreamRequestFromProto(pb *proto.DownloadStreamRequest) (DownloadStreamRequest, error) {
	req := DownloadStreamRequest{SessionID: StreamingSessionID(pb.SessionId), ChunkStreamSize: int(pb.ChunkStreamSize)}

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
	return &proto.DownloadStreamRequest{SessionId: r.SessionID.String(), ChunkStreamSize: int32(r.ChunkStreamSize)}
}

type DeleteChunkRequest struct {
	ChunkID ChunkID
}

func DeleteChunkRequestFromProto(pb *proto.DeleteChunkRequest) (DeleteChunkRequest, error) {
	chunkID, err := NewChunkID(pb.ChunkId)
	if err != nil {
		return DeleteChunkRequest{}, err
	}
	return DeleteChunkRequest{ChunkID: chunkID}, nil
}

func (r DeleteChunkRequest) ToProto() *proto.DeleteChunkRequest {
	return &proto.DeleteChunkRequest{ChunkId: r.ChunkID.String()}
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

func (r DeleteChunkResponse) ToProto() *proto.DeleteChunkResponse {
	return &proto.DeleteChunkResponse{
		Success: r.Success,
		Message: r.Message,
	}
}

type BulkDeleteChunkRequest struct {
	ChunkIDs []ChunkID
	Reason   string
}

func BulkDeleteChunkRequestFromProto(pb *proto.BulkDeleteChunkRequest) BulkDeleteChunkRequest {
	chunkIDs := make([]ChunkID, 0, len(pb.ChunkIds))
	for _, chunkID := range pb.ChunkIds {
		chunkIDs = append(chunkIDs, ChunkID(chunkID))
	}
	return BulkDeleteChunkRequest{
		ChunkIDs: chunkIDs,
		Reason:   pb.Reason,
	}
}

func (r BulkDeleteChunkRequest) ToProto() *proto.BulkDeleteChunkRequest {
	chunkIDs := make([]string, 0, len(r.ChunkIDs))
	for _, chunkID := range r.ChunkIDs {
		chunkIDs = append(chunkIDs, chunkID.String())
	}
	return &proto.BulkDeleteChunkRequest{
		ChunkIds: chunkIDs,
		Reason:   r.Reason,
	}
}

type BulkDeleteChunkResponse struct {
	Success bool
	Message string
	Failed  []ChunkID
}

func BulkDeleteChunkResponseFromProto(pb *proto.BulkDeleteChunkResponse) BulkDeleteChunkResponse {
	chunkIDs := make([]ChunkID, 0, len(pb.Failed))
	for _, chunkID := range pb.Failed {
		chunkIDs = append(chunkIDs, ChunkID(chunkID))
	}
	return BulkDeleteChunkResponse{
		Success: pb.Success,
		Message: pb.Message,
		Failed:  chunkIDs,
	}
}

func (r BulkDeleteChunkResponse) ToProto() *proto.BulkDeleteChunkResponse {
	chunkIDs := make([]string, 0, len(r.Failed))
	for _, chunkID := range r.Failed {
		chunkIDs = append(chunkIDs, chunkID.String())
	}
	return &proto.BulkDeleteChunkResponse{
		Success: r.Success,
		Message: r.Message,
		Failed:  chunkIDs,
	}
}

type ChunkDataStream struct {
	SessionID       StreamingSessionID
	ChunkID         ChunkID
	Data            []byte
	Offset          int
	IsFinal         bool
	PartialChecksum string
}

func ChunkDataStreamFromProto(pb *proto.ChunkDataStream) ChunkDataStream {
	return ChunkDataStream{
		SessionID:       StreamingSessionID(pb.SessionId),
		ChunkID:         ChunkID(pb.ChunkId), // We don't check on every loop as this is validated in the initial stream request
		Data:            pb.Data,
		Offset:          int(pb.Offset),
		IsFinal:         pb.IsFinal,
		PartialChecksum: pb.PartialChecksum,
	}
}

func (s ChunkDataStream) ToProto() *proto.ChunkDataStream {
	return &proto.ChunkDataStream{
		SessionId:       s.SessionID.String(),
		ChunkId:         s.ChunkID.String(),
		Data:            s.Data,
		Offset:          int64(s.Offset),
		IsFinal:         s.IsFinal,
		PartialChecksum: s.PartialChecksum,
	}
}

type ChunkDataAck struct {
	SessionID     StreamingSessionID
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
		SessionID:     StreamingSessionID(pb.SessionId),
		Success:       pb.Success,
		Message:       pb.Message,
		BytesReceived: int(pb.BytesReceived),
		ReadyForNext:  pb.ReadyForNext,
		Replicas:      replicas,
	}
}

func (a ChunkDataAck) ToProto() *proto.ChunkDataAck {
	replicas := make([]*proto.NodeInfo, len(a.Replicas))
	for i, replica := range a.Replicas {
		replicas[i] = replica.ToProto()
	}
	if len(replicas) == 0 {
		replicas = nil
	}

	return &proto.ChunkDataAck{
		SessionId:     a.SessionID.String(),
		Success:       a.Success,
		Message:       a.Message,
		BytesReceived: int64(a.BytesReceived),
		ReadyForNext:  a.ReadyForNext,
		Replicas:      replicas,
	}
}

type HealthCheckRequest struct{}

func HealthCheckRequestFromProto(pb *proto.HealthCheckRequest) HealthCheckRequest {
	return HealthCheckRequest{}
}

func (r HealthCheckRequest) ToProto() *proto.HealthCheckRequest {
	return &proto.HealthCheckRequest{}
}

type HealthCheckResponse struct {
	Status HealthStatus
}

func HealthCheckResponseFromProto(pb *proto.HealthCheckResponse) HealthCheckResponse {
	return HealthCheckResponse{Status: HealthStatusFromProto(pb.Status)}
}

func (r HealthCheckResponse) ToProto() *proto.HealthCheckResponse {
	return &proto.HealthCheckResponse{Status: r.Status.ToProto()}
}

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

func (r NodeUpdate) ToProto() *proto.NodeUpdate {
	return &proto.NodeUpdate{
		Version:   r.Version,
		Type:      proto.NodeUpdate_UpdateType(r.Type),
		Node:      r.Node.ToProto(),
		Timestamp: timestamppb.New(r.Timestamp),
	}
}

// Coordinator requests
// Upload
type UploadRequest struct {
	Path      FilePath
	Size      FileSize
	ChunkSize ChunkSize
	Checksum  string
}

func UploadRequestFromProto(pb *proto.UploadRequest) (UploadRequest, error) {
	filepath, err := NewFilePath(pb.Path)
	if err != nil {
		return UploadRequest{}, err
	}
	filesize, err := NewFileSize(pb.Size)
	if err != nil {
		return UploadRequest{}, err
	}
	chunksize, err := NewChunkSize(pb.ChunkSize)
	if err != nil {
		return UploadRequest{}, err
	}

	uploadRequest := UploadRequest{
		Path:      filepath,
		Size:      filesize,
		ChunkSize: chunksize,
		Checksum:  pb.Checksum,
	}
	return uploadRequest, nil
}

func (r *UploadRequest) ToProto() *proto.UploadRequest {
	return &proto.UploadRequest{
		Path:      r.Path.String(),
		Size:      int64(r.Size),
		ChunkSize: int64(r.ChunkSize),
		Checksum:  r.Checksum,
	}
}

type UploadResponse struct {
	ChunkIDs  []ChunkID
	Nodes     []*NodeInfo
	SessionID MetadataSessionID
}

func UploadResponseFromProto(pb *proto.UploadResponse) UploadResponse {
	nodes := make([]*NodeInfo, 0, len(pb.Nodes))
	for _, node := range pb.Nodes {
		nodes = append(nodes, NodeInfoFromProto(node))
	}
	chunkIDs := make([]ChunkID, 0, len(pb.ChunkIds))
	for _, chunkID := range pb.ChunkIds {
		chunkIDs = append(chunkIDs, ChunkID(chunkID))
	}
	return UploadResponse{
		ChunkIDs:  chunkIDs,
		Nodes:     nodes,
		SessionID: MetadataSessionID(pb.SessionId),
	}
}

func (r UploadResponse) ToProto() *proto.UploadResponse {
	protoNodes := make([]*proto.NodeInfo, 0, len(r.Nodes))
	for _, node := range r.Nodes {
		protoNodes = append(protoNodes, node.ToProto())
	}
	chunkIDs := make([]string, 0, len(r.ChunkIDs))
	for _, chunkID := range r.ChunkIDs {
		chunkIDs = append(chunkIDs, chunkID.String())
	}
	return &proto.UploadResponse{
		ChunkIds:  chunkIDs,
		Nodes:     protoNodes,
		SessionId: r.SessionID.String(),
	}
}

func (r UploadResponse) validate() error {
	errs := make([]error, 0)
	if len(r.Nodes) == 0 {
		errs = append(errs, fmt.Errorf("no nodes to upload to"))
	}
	if len(r.ChunkIDs) == 0 {
		errs = append(errs, fmt.Errorf("no chunks to upload"))
	}
	if err := r.SessionID.validate(); err != nil {
		errs = append(errs, err)
	}
	if len(errs) != 0 {
		return errors.Join(errs...)
	}
	return nil
}

// Download
type DownloadRequest struct {
	Path FilePath
}

func DownloadRequestFromProto(pb *proto.DownloadRequest) (DownloadRequest, error) {
	downloadRequest := DownloadRequest{Path: FilePath(pb.Path)}
	if err := downloadRequest.validate(); err != nil {
		return DownloadRequest{}, fmt.Errorf("%w: %w", ErrValidation, err)
	}
	return downloadRequest, nil
}

func (r DownloadRequest) validate() error {
	if err := r.Path.validate(); err != nil {
		return err
	}
	return nil
}

func (r DownloadRequest) ToProto() *proto.DownloadRequest {
	return &proto.DownloadRequest{Path: r.Path.String()}
}

type DownloadResponse struct {
	FileInfo       FileInfo
	ChunkLocations []ChunkLocation
	SessionID      MetadataSessionID
}

func DownloadResponseFromProto(pb *proto.DownloadResponse) (DownloadResponse, error) {
	chunkLocations := make([]ChunkLocation, 0, len(pb.ChunkLocations))
	for _, chunkLocation := range pb.ChunkLocations {
		chunkLocations = append(chunkLocations, ChunkLocationFromProto(chunkLocation))
	}

	// Validate
	errs := make([]error, 0)
	fileInfo, err := FileInfoFromProto(pb.FileInfo)
	if err != nil {
		errs = append(errs, err)
	}
	if len(errs) != 0 {
		return DownloadResponse{}, errors.Join(errs...)
	}

	// Create and return download response
	downloadResponse := DownloadResponse{
		FileInfo:       fileInfo,
		ChunkLocations: chunkLocations,
		SessionID:      MetadataSessionID(pb.SessionId),
	}

	return downloadResponse, nil
}

func (r DownloadResponse) ToProto() *proto.DownloadResponse {
	protoChunkLocations := make([]*proto.ChunkLocation, 0, len(r.ChunkLocations))
	for _, item := range r.ChunkLocations {
		protoChunkLocations = append(protoChunkLocations, item.ToProto())
	}

	return &proto.DownloadResponse{
		FileInfo:       r.FileInfo.ToProto(),
		ChunkLocations: protoChunkLocations,
		SessionId:      r.SessionID.String(),
	}
}

// Delete
type DeleteRequest struct {
	Path FilePath
}

func DeleteRequestFromProto(pb *proto.DeleteRequest) DeleteRequest {
	return DeleteRequest{Path: FilePath(pb.Path)}
}

func (r DeleteRequest) ToProto() *proto.DeleteRequest {
	return &proto.DeleteRequest{Path: r.Path.String()}
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

func (r DeleteResponse) ToProto() *proto.DeleteResponse {
	return &proto.DeleteResponse{
		Success: r.Success,
		Message: r.Message,
	}
}

type ConfirmUploadRequest struct {
	SessionID  MetadataSessionID
	ChunkInfos []ChunkInfo
}

func ConfirmUploadRequestFromProto(pb *proto.ConfirmUploadRequest) (ConfirmUploadRequest, error) {
	chunkInfos := make([]ChunkInfo, 0, len(pb.ChunkInfos))
	for _, chunkInfo := range pb.ChunkInfos {
		chunkinfo, err := ChunkInfoFromProto(chunkInfo)
		if err != nil {
			return ConfirmUploadRequest{}, err
		}
		chunkInfos = append(chunkInfos, chunkinfo)
	}
	return ConfirmUploadRequest{SessionID: MetadataSessionID(pb.SessionId), ChunkInfos: chunkInfos}, nil
}

func (r ConfirmUploadRequest) ToProto() *proto.ConfirmUploadRequest {
	chunkInfos := make([]*proto.ChunkInfo, 0, len(r.ChunkInfos))
	for _, chunkInfo := range r.ChunkInfos {
		chunkInfos = append(chunkInfos, chunkInfo.ToProto())
	}
	return &proto.ConfirmUploadRequest{SessionId: r.SessionID.String(), ChunkInfos: chunkInfos}
}

type ConfirmUploadResponse struct {
	Success bool
	Message string
}

func ConfirmUploadResponseFromProto(pb *proto.ConfirmUploadResponse) ConfirmUploadResponse {
	return ConfirmUploadResponse{Success: pb.Success, Message: pb.Message}
}

func (r ConfirmUploadResponse) ToProto() *proto.ConfirmUploadResponse {
	return &proto.ConfirmUploadResponse{Success: r.Success, Message: r.Message}
}

// List
type ListRequest struct {
	Directory FilePath
}

func ListRequestFromProto(pb *proto.ListRequest) ListRequest {
	return ListRequest{Directory: FilePath(pb.Directory)}
}

func (r ListRequest) ToProto() *proto.ListRequest {
	return &proto.ListRequest{Directory: r.Directory.String()}
}

type ListResponse struct {
	Files []*FileInfo
}

func ListResponseFromProto(pb *proto.ListResponse) (ListResponse, error) {
	files := make([]*FileInfo, 0, len(pb.Files))
	for _, file := range pb.Files {
		info, err := FileInfoFromProto(file)
		if err != nil {
			return ListResponse{}, err
		}
		files = append(files, &info)
	}
	return ListResponse{Files: files}, nil
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

func (r RegisterDataNodeRequest) ToProto() *proto.RegisterDataNodeRequest {
	return &proto.RegisterDataNodeRequest{NodeInfo: r.NodeInfo.ToProto()}
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

func (r HeartbeatRequest) ToProto() *proto.HeartbeatRequest {
	return &proto.HeartbeatRequest{
		NodeId:          r.NodeID,
		Status:          r.Status.ToProto(),
		LastSeenVersion: r.LastSeenVersion,
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

func (r HeartbeatResponse) ToProto() *proto.HeartbeatResponse {
	updates := make([]*proto.NodeUpdate, 0, len(r.Updates))
	for _, update := range r.Updates {
		updates = append(updates, update.ToProto())
	}

	return &proto.HeartbeatResponse{
		Success:            r.Success,
		Message:            r.Message,
		Updates:            updates,
		FromVersion:        r.FromVersion,
		ToVersion:          r.ToVersion,
		RequiresFullResync: r.RequiresFullResync,
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

func (r ListNodesResponse) ToProto() *proto.ListNodesResponse {
	nodes := make([]*proto.NodeInfo, 0, len(r.Nodes))
	for _, node := range r.Nodes {
		nodes = append(nodes, (*node).ToProto())
	}
	return &proto.ListNodesResponse{
		Nodes:          nodes,
		CurrentVersion: r.CurrentVersion,
	}
}

// ChunkLocation represents where some chunk should be stored (primary node + endpoint)
type ChunkLocation struct {
	ChunkID ChunkID
	Nodes   []*NodeInfo
}

func ChunkLocationFromProto(pb *proto.ChunkLocation) ChunkLocation {
	nodes := make([]*NodeInfo, 0, len(pb.Nodes))
	for _, node := range pb.Nodes {
		nodes = append(nodes, NodeInfoFromProto(node))
	}
	id, err := NewChunkID(pb.ChunkId)
	if err != nil {
		return ChunkLocation{}
	}
	return ChunkLocation{
		ChunkID: id,
		Nodes:   nodes,
	}
}

func (l *ChunkLocation) ToProto() *proto.ChunkLocation {
	nodes := make([]*proto.NodeInfo, 0, len(l.Nodes))
	for _, node := range l.Nodes {
		nodes = append(nodes, node.ToProto())
	}
	return &proto.ChunkLocation{
		ChunkId: l.ChunkID.String(),
		Nodes:   nodes,
	}
}

type GetChunksForNodeRequest struct {
	NodeID string
}

func GetChunksForNodeRequestFromProto(pb *proto.GetChunksForNodeRequest) GetChunksForNodeRequest {
	return GetChunksForNodeRequest{NodeID: pb.NodeId}
}

func (r GetChunksForNodeRequest) ToProto() *proto.GetChunksForNodeRequest {
	return &proto.GetChunksForNodeRequest{NodeId: r.NodeID}
}

type GetChunksForNodeResponse struct {
	ChunkIDs []string
}

func GetChunksForNodeResponseFromProto(pb *proto.GetChunksForNodeResponse) GetChunksForNodeResponse {
	return GetChunksForNodeResponse{ChunkIDs: pb.ChunkIds}
}

func (r GetChunksForNodeResponse) ToProto() *proto.GetChunksForNodeResponse {
	return &proto.GetChunksForNodeResponse{ChunkIds: r.ChunkIDs}
}
