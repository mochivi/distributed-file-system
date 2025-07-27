package datanode_controllers

import (
	"context"
	"errors"
	"log/slog"
	"testing"

	"github.com/mochivi/distributed-file-system/internal/cluster/shared"
	"github.com/mochivi/distributed-file-system/internal/common"
	"github.com/mochivi/distributed-file-system/internal/config"
	chunkstore "github.com/mochivi/distributed-file-system/internal/storage/chunk"
	"github.com/mochivi/distributed-file-system/pkg/logging"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
)

func TestFindOrphanedChunks(t *testing.T) {
	tests := []struct {
		name           string
		expectedChunks map[string]*common.ChunkHeader
		actualChunks   map[string]common.ChunkHeader
		expectedOrphan []string
	}{
		{
			name: "no-orphans",
			expectedChunks: map[string]*common.ChunkHeader{
				"chunk-1": {ID: "chunk-1"},
				"chunk-2": {ID: "chunk-2"},
			},
			actualChunks: map[string]common.ChunkHeader{
				"chunk-1": {ID: "chunk-1"},
				"chunk-2": {ID: "chunk-2"},
			},
			expectedOrphan: []string{},
		},
		{
			name: "some-orphans",
			expectedChunks: map[string]*common.ChunkHeader{
				"chunk-1": {ID: "chunk-1"},
			},
			actualChunks: map[string]common.ChunkHeader{
				"chunk-1": {ID: "chunk-1"},
				"chunk-3": {ID: "chunk-3"},
			},
			expectedOrphan: []string{"chunk-3"},
		},
		{
			name:           "all-orphans",
			expectedChunks: map[string]*common.ChunkHeader{},
			actualChunks: map[string]common.ChunkHeader{
				"chunk-1": {ID: "chunk-1"},
			},
			expectedOrphan: []string{"chunk-1"},
		},
		{
			name:           "empty-actual",
			expectedChunks: map[string]*common.ChunkHeader{"chunk-1": {ID: "chunk-1"}},
			actualChunks:   map[string]common.ChunkHeader{},
			expectedOrphan: []string{},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			orphaned := findOrphanedChunks(tt.expectedChunks, tt.actualChunks)
			require.ElementsMatch(t, tt.expectedOrphan, orphaned)
		})
	}
}

func TestGetChunks(t *testing.T) {
	sampleHeader := common.ChunkHeader{ID: "chunk1"}
	expected := map[string]*common.ChunkHeader{"chunk1": &sampleHeader}
	actual := map[string]common.ChunkHeader{"chunk1": sampleHeader}

	tests := []struct {
		name        string
		scannerErr  error
		storeErr    error
		expectedErr error
	}{
		{
			name:        "success",
			scannerErr:  nil,
			storeErr:    nil,
			expectedErr: nil,
		},
		{
			name:        "scanner-error",
			scannerErr:  errors.New("scanner failure"),
			storeErr:    nil,
			expectedErr: errors.New("scanner failure"),
		},
		{
			name:        "store-error",
			scannerErr:  nil,
			storeErr:    errors.New("store failure"),
			expectedErr: errors.New("store failure"),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx := context.Background()
			scanner := &shared.MockMetadataScanner{}
			store := &chunkstore.MockChunkStore{}

			scanner.On("GetChunksForNode", mock.Anything, "node-1").Return(expected, tt.scannerErr).Once()
			store.On("GetHeaders", mock.Anything).Return(actual, tt.storeErr).Once()

			exp, act, err := getChunks(ctx, "node-1", scanner, store)

			if tt.expectedErr != nil {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
				require.Equal(t, expected, exp)
				require.Equal(t, actual, act)
			}

			scanner.AssertExpectations(t)
			store.AssertExpectations(t)
		})
	}
}

func TestOrphanedChunksGCController_run(t *testing.T) {
	// Prepare common objects
	header1 := common.ChunkHeader{ID: "chunk-1"}
	header2 := common.ChunkHeader{ID: "chunk-2"}

	expectedChunks := map[string]*common.ChunkHeader{"chunk-1": &header1}
	actualChunks := map[string]common.ChunkHeader{"chunk-1": header1, "chunk-2": header2}

	cfg := config.DefaultOrphanedChunksGCControllerConfig()
	logger := logging.NewTestLogger(slog.LevelError, true)

	tests := []struct {
		name           string
		setupMocks     func(scanner *shared.MockMetadataScanner, store *chunkstore.MockChunkStore)
		expectedErr    error
		expectedFail   []string
		alreadyRunning bool
	}{
		{
			name: "success",
			setupMocks: func(scanner *shared.MockMetadataScanner, store *chunkstore.MockChunkStore) {
				scanner.On("GetChunksForNode", mock.Anything, "node-1").Return(expectedChunks, nil).Once()
				store.On("GetHeaders", mock.Anything).Return(actualChunks, nil).Once()
				// Expect BulkDelete called with orphaned ["chunk-2"]
				store.On("BulkDelete", mock.Anything, cfg.MaxConcurrentDeletes, []string{"chunk-2"}).Return([]string{}, nil).Once()
			},
			expectedErr:  nil,
			expectedFail: []string{},
		},
		{
			name: "bulk-delete-error",
			setupMocks: func(scanner *shared.MockMetadataScanner, store *chunkstore.MockChunkStore) {
				scanner.On("GetChunksForNode", mock.Anything, "node-1").Return(expectedChunks, nil).Once()
				store.On("GetHeaders", mock.Anything).Return(actualChunks, nil).Once()
				store.On("BulkDelete", mock.Anything, cfg.MaxConcurrentDeletes, []string{"chunk-2"}).Return(nil, errors.New("delete failed")).Once()
			},
			expectedErr:  errors.New("delete failed"),
			expectedFail: nil,
		},
		{
			name:           "already-running",
			alreadyRunning: true,
			setupMocks: func(scanner *shared.MockMetadataScanner, store *chunkstore.MockChunkStore) {
				// No expectations; run should exit early
			},
			expectedErr:  nil,
			expectedFail: nil,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			scanner := &shared.MockMetadataScanner{}
			store := &chunkstore.MockChunkStore{}
			if tt.setupMocks != nil {
				tt.setupMocks(scanner, store)
			}

			ctx := context.Background()
			ctrl := NewOrphanedChunksGCController(ctx, scanner, store, cfg, "node-1", logger)
			if tt.alreadyRunning {
				ctrl.running = true
			}

			failed, err := ctrl.run(ctx)

			if tt.expectedErr != nil {
				require.Error(t, err)
				require.Contains(t, err.Error(), tt.expectedErr.Error())
			} else {
				require.NoError(t, err)
				require.ElementsMatch(t, tt.expectedFail, failed)
			}

			scanner.AssertExpectations(t)
			store.AssertExpectations(t)
		})
	}
}
