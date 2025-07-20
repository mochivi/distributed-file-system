package coordinator_controllers

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"testing"
	"time"

	"github.com/mochivi/distributed-file-system/internal/clients"
	"github.com/mochivi/distributed-file-system/internal/common"
	"github.com/mochivi/distributed-file-system/pkg/logging"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
)

// Helper function, creates a metadata representation of all files stored in the cluster
// metadata is a description of each file, how many chunks each file has and where each chunk is replicated
// nNodes decides how many nodes this cluster should have
// nNodes must be at minimum equal to the number of replicas the chunk with the most replicas has.
// example:
//
//	clusterMetadata := [][][]string{
//		{ // file 1
//			{"node-1", "node-2", "node-3"}, // chunk1 replicated on nodes 1, 2 and 3 -> we require AT LEAST 3 nodes in the cluster to replicate in 3 places
//			{"node-1", "node-2"}, // chunk 2 replicated only on node 1
//		},
//		{ // file 2
//			{"node-4", "node-1", "node-5"}, // chunk1 replicated on nodes 1, 2 and 3
//		},
//	}
func setupSampleMetadata(nNodes int, metadata [][][]string) (map[string]*common.NodeInfo, []*common.FileInfo) {
	clusterNodes := map[string]*common.NodeInfo{}
	for i := 1; i <= nNodes; i++ {
		nodeID := fmt.Sprintf("node-%d", i)
		nodePort := 8080 + i
		clusterNodes[nodeID] = &common.NodeInfo{ID: nodeID, Host: "localhost", Port: nodePort}
	}

	filesMetadata := []*common.FileInfo{}
	for fileNum := range metadata {
		chunkCount := len(metadata[fileNum])
		chunks := make([]common.ChunkInfo, 0, chunkCount)
		fileReplicas := metadata[fileNum]

		fileInfo := &common.FileInfo{Deleted: true, ChunkCount: chunkCount, Chunks: chunks}

		for chunkNum := range chunkCount {
			// Create the replicas slice
			chunkReplicasIDs := fileReplicas[chunkNum]
			chunkReplicas := make([]*common.NodeInfo, 0, len(chunkReplicasIDs))

			for _, replica := range chunkReplicasIDs {
				chunkReplicas = append(chunkReplicas, clusterNodes[replica])
			}

			// Create the chunk representation
			chunk := common.ChunkInfo{
				Header: common.ChunkHeader{
					ID: fmt.Sprintf("f%d-c%d", fileNum, chunkNum),
				},
				Replicas: chunkReplicas,
			}

			// Add the chunk to file metadata
			fileInfo.Chunks = append(fileInfo.Chunks, chunk)
		}

		filesMetadata = append(filesMetadata, fileInfo)
	}

	return clusterNodes, filesMetadata
}

func makeChunks(n int) []string {
	chunks := make([]string, n)
	for i := range n {
		chunks[i] = fmt.Sprintf("c%d", i)
	}
	return chunks
}

func TestDeletedFilesGCController_prepareChunkMappings(t *testing.T) {
	tests := []struct {
		name                 string
		filesMetadata        [][][]string
		nClusterNodes        int
		usedClusterNodes     int
		expectedNodeToChunks map[string][]string
		modifyMetadata       func([]*common.FileInfo)
	}{
		{
			name: "2-files 6-nodes",
			filesMetadata: [][][]string{
				{ // file0
					{"node-1", "node-2", "node-3"}, // chunk1 replicated on nodes 1, 2 and 3
					{"node-1", "node-2", "node-6"}, // chunk 2 replicated only on node 1
				},
				{ // file1
					{"node-4", "node-1", "node-5"}, // chunk1 replicated on nodes 1, 2 and 3
				},
			},
			nClusterNodes:    9,
			usedClusterNodes: 6,
			expectedNodeToChunks: map[string][]string{
				"node-1": {"f0-c0", "f0-c1", "f1-c0"}, // node-1 contains chunk0 from file0, chunk1 from file0, chunk0 from file1 ...
				"node-2": {"f0-c0", "f0-c1"},
				"node-3": {"f0-c0"},
				"node-4": {"f1-c0"},
				"node-5": {"f1-c0"},
				"node-6": {"f0-c1"},
			},
		},
		{
			name: "5-files 25-nodes",
			filesMetadata: [][][]string{
				{ // file 0
					{"node-1", "node-2", "node-3"},    // f0-c0
					{"node-1", "node-2", "node-6"},    // f0-c1
					{"node-10", "node-15", "node-12"}, // f0-c2
					{"node-1", "node-12", "node-16"},  // f0-c3
					{"node-9", "node-8", "node-23"},   // f0-c4
					{"node-1", "node-14", "node-15"},  // f0-c5
				},
				{ // file 1
					{"node-4", "node-1", "node-5"},    // f1-c0
					{"node-14", "node-22", "node-5"},  // f1-c1
					{"node-9", "node-8", "node-7"},    // f1-c2
					{"node-6", "node-21", "node-20"},  // f1-c3
					{"node-10", "node-18", "node-19"}, // f1-c4
				},
				{ // file 2
					{"node-4", "node-1", "node-5"}, // f2-c0
				},
				{ // file 3
					{"node-4", "node-1", "node-5"}, // f3-c0
				},
				{ // file 4
					{"node-4", "node-1", "node-5"}, // f4-c0
				},
			},
			nClusterNodes:    25,
			usedClusterNodes: 20,
			expectedNodeToChunks: map[string][]string{
				"node-1":  {"f0-c0", "f0-c1", "f0-c3", "f0-c5", "f1-c0", "f2-c0", "f3-c0", "f4-c0"},
				"node-2":  {"f0-c0", "f0-c1"},
				"node-3":  {"f0-c0"},
				"node-4":  {"f1-c0", "f2-c0", "f3-c0", "f4-c0"},
				"node-5":  {"f1-c0", "f1-c1", "f2-c0", "f3-c0", "f4-c0"},
				"node-6":  {"f0-c1", "f1-c3"},
				"node-7":  {"f1-c2"},
				"node-8":  {"f0-c4", "f1-c2"},
				"node-9":  {"f0-c4", "f1-c2"},
				"node-10": {"f0-c2", "f1-c4"},
				"node-12": {"f0-c2", "f0-c3"},
				"node-14": {"f0-c5", "f1-c1"},
				"node-15": {"f0-c2", "f0-c5"},
				"node-16": {"f0-c3"},
				"node-18": {"f1-c4"},
				"node-19": {"f1-c4"},
				"node-20": {"f1-c3"},
				"node-21": {"f1-c3"},
				"node-22": {"f1-c1"},
				"node-23": {"f0-c4"},
			},
		},
		{
			name:                 "no-deleted-files",
			filesMetadata:        [][][]string{},
			nClusterNodes:        5,
			usedClusterNodes:     0,
			expectedNodeToChunks: map[string][]string{},
		},
		{
			name: "one-file-not-deleted",
			filesMetadata: [][][]string{
				{ // file0 - deleted
					{"node-1", "node-2"},
				},
				{ // file1 - will be marked as not deleted
					{"node-1", "node-3"},
				},
			},
			nClusterNodes:    3,
			usedClusterNodes: 2,
			expectedNodeToChunks: map[string][]string{
				"node-1": {"f0-c0"},
				"node-2": {"f0-c0"},
			},
			modifyMetadata: func(files []*common.FileInfo) {
				if len(files) > 1 {
					files[1].Deleted = false
				}
			},
		},
		{
			name: "file-with-no-chunks",
			filesMetadata: [][][]string{
				{ // file0 - deleted with chunks
					{"node-1", "node-2"},
				},
				{ // file1 - deleted, but no chunks
				},
			},
			nClusterNodes:    3,
			usedClusterNodes: 2,
			expectedNodeToChunks: map[string][]string{
				"node-1": {"f0-c0"},
				"node-2": {"f0-c0"},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Convert from simple representation to actual filesMetadata representation
			_, filesMetadata := setupSampleMetadata(tt.nClusterNodes, tt.filesMetadata)
			if tt.modifyMetadata != nil {
				tt.modifyMetadata(filesMetadata)
			}

			nodeToChunks, nodeToClient := prepareChunkMappings(filesMetadata, logging.NewTestLogger(slog.LevelError))

			// t.Logf("%+v", nodeToChunks)

			require.Equal(t, len(nodeToChunks), tt.usedClusterNodes) // Ensure only nodes that actually have chunks were mapped to chunks
			require.Equal(t, len(nodeToClient), tt.usedClusterNodes) // Ensure all clients have been created

			// Expected node chunk distribution

			// Check if the values in each string slice are the same as expected
			for nodeID, chunkIDs := range tt.expectedNodeToChunks {
				require.ElementsMatch(t, chunkIDs, nodeToChunks[nodeID])
			}
		})
	}

}

func TestDeletedFilesGCController_queueWork(t *testing.T) {
	tests := []struct {
		name              string
		nodeToChunks      map[string][]string
		nodeToClient      map[string]clients.IDataNodeClient
		batchSize         int
		expectedWorkCount int
	}{
		{
			name: "success: queue-no-batching",
			nodeToChunks: map[string][]string{
				"node-1": {"f0-c0", "f0-c1", "f1-c0"},
				"node-2": {"f0-c0", "f0-c1"},
				"node-3": {"f0-c0"},
				"node-4": {"f1-c0"},
				"node-5": {"f1-c0"},
				"node-6": {"f0-c1"},
			},
			nodeToClient: map[string]clients.IDataNodeClient{
				"node-1": &clients.MockDataNodeClient{},
				"node-2": &clients.MockDataNodeClient{},
				"node-3": &clients.MockDataNodeClient{},
				"node-4": &clients.MockDataNodeClient{},
				"node-5": &clients.MockDataNodeClient{},
				"node-6": &clients.MockDataNodeClient{},
			},
			batchSize:         10, // 10 chunkIDs per request
			expectedWorkCount: 6,  // 6 nodes holding chunks and no node has more chunkIDs per request than batchSize allows
		},
		{
			name: "success: queue-with-batching",
			nodeToChunks: map[string][]string{
				"node-1": makeChunks(15), // 2 batches
				"node-2": makeChunks(5),  // 1 batch
				"node-3": makeChunks(20), // 2 batches
			},
			nodeToClient: map[string]clients.IDataNodeClient{
				"node-1": &clients.MockDataNodeClient{},
				"node-2": &clients.MockDataNodeClient{},
				"node-3": &clients.MockDataNodeClient{},
			},
			batchSize:         10,
			expectedWorkCount: 5, // 2 + 1 + 2
		},
		{
			name:              "success: empty-inputs",
			nodeToChunks:      map[string][]string{},
			nodeToClient:      map[string]clients.IDataNodeClient{},
			batchSize:         10,
			expectedWorkCount: 0,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			workCh := make(chan *deleteWork, tt.expectedWorkCount+1)
			go queueWork(workCh, tt.nodeToClient, tt.nodeToChunks, tt.batchSize)

			ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
			defer cancel()

			workCount := 0

		forLoop:
			for {
				select {
				case _, ok := <-workCh:
					if !ok {
						break forLoop
					}
					workCount++
				case <-ctx.Done():
					t.Fatal(ctx.Err().Error())
				}
			}

			require.Equal(t, tt.expectedWorkCount, workCount)
		})
	}
}

func TestDeletedFilesGCController_doWork(t *testing.T) {
	tests := []struct {
		name          string
		setup         func(t *testing.T) (context.Context, context.CancelFunc, <-chan *deleteWork, []*clients.MockDataNodeClient)
		expectedError error
	}{
		{
			name: "success: process all work",
			setup: func(t *testing.T) (context.Context, context.CancelFunc, <-chan *deleteWork, []*clients.MockDataNodeClient) {
				workCh := make(chan *deleteWork, 2)
				mockClient1 := new(clients.MockDataNodeClient)
				mockClient2 := new(clients.MockDataNodeClient)

				mockClient1.On("BulkDeleteChunk", mock.Anything, mock.Anything).Return(common.BulkDeleteChunkResponse{}, nil).Once()
				mockClient1.On("Close").Return(nil).Once()
				mockClient2.On("BulkDeleteChunk", mock.Anything, mock.Anything).Return(common.BulkDeleteChunkResponse{}, nil).Once()
				mockClient2.On("Close").Return(nil).Once()

				workCh <- &deleteWork{client: mockClient1, chunkIDs: []string{"c1"}}
				workCh <- &deleteWork{client: mockClient2, chunkIDs: []string{"c2"}}
				close(workCh)

				return context.Background(), nil, workCh, []*clients.MockDataNodeClient{mockClient1, mockClient2}
			},
		},
		{
			name: "error: processing fails, stops work",
			setup: func(t *testing.T) (context.Context, context.CancelFunc, <-chan *deleteWork, []*clients.MockDataNodeClient) {
				workCh := make(chan *deleteWork, 2)
				mockClient1 := new(clients.MockDataNodeClient)
				mockClient2 := new(clients.MockDataNodeClient) // This one will not be called

				failErr := errors.New("delete failed")
				mockClient1.On("BulkDeleteChunk", mock.Anything, mock.Anything).Return(common.BulkDeleteChunkResponse{}, failErr).Once()
				mockClient1.On("Close").Return(nil).Once()

				workCh <- &deleteWork{client: mockClient1, chunkIDs: []string{"c1"}}
				workCh <- &deleteWork{client: mockClient2, chunkIDs: []string{"c2"}}
				close(workCh)

				return context.Background(), nil, workCh, []*clients.MockDataNodeClient{mockClient1, mockClient2}
			},
			expectedError: errors.New("failed to delete 1 chunks: delete failed"),
		},
		{
			name: "error: context cancelled",
			setup: func(t *testing.T) (context.Context, context.CancelFunc, <-chan *deleteWork, []*clients.MockDataNodeClient) {
				workCh := make(chan *deleteWork, 1)
				// Note: Don't close the channel to simulate work being interrupted
				ctx, cancel := context.WithCancel(context.Background())
				return ctx, cancel, workCh, nil
			},
			expectedError: context.Canceled,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx, cancel, workCh, mockClients := tt.setup(t)

			if cancel != nil {
				cancel()
			}

			err := doWork(ctx, workCh)

			if tt.expectedError != nil {
				require.Error(t, err)
				require.Contains(t, err.Error(), tt.expectedError.Error())
			} else {
				require.NoError(t, err)
			}

			for _, mockClient := range mockClients {
				mockClient.AssertExpectations(t)
			}
		})
	}
}

func TestDeletedFilesGCController_processWork(t *testing.T) {
	tests := []struct {
		name      string
		chunkIDs  []string
		clientErr error
	}{
		{
			name:      "success",
			chunkIDs:  []string{"chunk-1", "chunk-2"},
			clientErr: nil,
		},
		{
			name:      "error: delete failed",
			chunkIDs:  []string{"chunk-1", "chunk-2"},
			clientErr: errors.New("failed to delete chunks"),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx := context.Background()
			mockClient := new(clients.MockDataNodeClient)

			work := &deleteWork{
				client:   mockClient,
				chunkIDs: tt.chunkIDs,
			}

			req := common.BulkDeleteChunkRequest{ChunkIDs: tt.chunkIDs}
			mockClient.On("BulkDeleteChunk", ctx, req).Return(common.BulkDeleteChunkResponse{}, tt.clientErr).Once()
			mockClient.On("Close").Return(nil).Once()

			err := processWork(ctx, work)

			if tt.clientErr != nil {
				require.Error(t, err)
				require.ErrorIs(t, err, tt.clientErr)
			} else {
				require.NoError(t, err)
			}

			mockClient.AssertExpectations(t)
		})
	}
}
