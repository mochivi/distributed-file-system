package chunk

import (
	"context"
	"testing"

	"github.com/mochivi/distributed-file-system/internal/common"
	"github.com/mochivi/distributed-file-system/internal/config"
	"github.com/mochivi/distributed-file-system/internal/storage/encoding"
	"github.com/spf13/afero"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// helper to create a new ChunkDiskStorage backed by an in-memory filesystem
func newTestStorage(t *testing.T) *ChunkDiskStorage {
	t.Helper()
	memFs := afero.NewMemMapFs()
	serializer := encoding.NewProtoSerializer()
	store, err := NewChunkDiskStorage(memFs, config.DiskStorageConfig{RootDir: "/chunks"}, serializer)
	require.NoError(t, err)
	return store
}

func TestChunkDiskStorage_StoreAndGet(t *testing.T) {
	tests := []struct {
		name        string
		header      common.ChunkHeader
		data        []byte
		expectErr   bool
		expectedErr error
	}{
		{
			name:   "success: store and get chunk",
			header: common.ChunkHeader{ID: "0102030405060708_0", Index: 0, Size: int64(5), Checksum: "abc"},
			data:   []byte("hello"),
		},
		{
			name:        "error: invalid id format",
			header:      common.ChunkHeader{ID: "invalid", Index: 0, Size: int64(3)},
			data:        []byte("bad"),
			expectErr:   true,
			expectedErr: ErrInvalidChunkID,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			store := newTestStorage(t)
			tc.header.Size = int64(len(tc.data))
			err := store.Store(context.Background(), tc.header, tc.data)
			if tc.expectErr {
				assert.Error(t, err)
				assert.ErrorIs(t, err, tc.expectedErr)
				return
			}
			require.NoError(t, err)

			// happy-path: verify Get returns same header and data
			gotHeader, gotData, err := store.Get(context.Background(), tc.header.ID)
			require.NoError(t, err)
			assert.Equal(t, tc.header.ID, gotHeader.ID)
			assert.Equal(t, tc.header.Size, gotHeader.Size)
			assert.Equal(t, tc.data, gotData)
		})
	}
}

func TestChunkDiskStorage_DeleteAndExists(t *testing.T) {
	store := newTestStorage(t)
	header := common.ChunkHeader{ID: "0a0b0c0d0e0f1011_1", Index: 1, Size: int64(4)}
	data := []byte("data")
	require.NoError(t, store.Store(context.Background(), header, data))

	assert.NoError(t, store.Exists(context.Background(), header.ID))

	// delete
	require.NoError(t, store.Delete(context.Background(), header.ID))
	assert.Error(t, store.Exists(context.Background(), header.ID))

	// deleting again should error (fs remove no such file)
	err := store.Delete(context.Background(), header.ID)
	assert.Error(t, err)
}

func TestChunkDiskStorage_List(t *testing.T) {
	store := newTestStorage(t)
	ids := []string{"aaaaaaaaaaaaaaaa_0", "bbbbbbbbbbbbbbbb_1", "cccccccccccccccc_2"}
	for _, id := range ids {
		hdr := common.ChunkHeader{ID: id, Index: 0, Size: int64(1)}
		require.NoError(t, store.Store(context.Background(), hdr, []byte("x")))
	}

	listed, err := store.List(context.Background())
	require.NoError(t, err)
	assert.ElementsMatch(t, ids, listed)
}

func TestChunkDiskStorage_GetHeaders(t *testing.T) {
	store := newTestStorage(t)
	ids := []string{"1111111111111111_0", "2222222222222222_1"}
	for _, id := range ids {
		hdr := common.ChunkHeader{ID: id, Index: 0, Size: int64(1)}
		require.NoError(t, store.Store(context.Background(), hdr, []byte("z")))
	}

	ctx := context.Background()
	headers, err := store.GetHeaders(ctx)
	require.NoError(t, err)
	assert.Len(t, headers, len(ids))
	for _, id := range ids {
		_, ok := headers[id]
		assert.True(t, ok)
	}

	// cancel context early, expect context error
	ctxCancel, cancel := context.WithCancel(context.Background())
	cancel()
	_, err = store.GetHeaders(ctxCancel)
	assert.ErrorIs(t, err, context.Canceled)
}

func TestChunkDiskStorage_BulkDelete(t *testing.T) {
	store := newTestStorage(t)
	presentID := "9999999999999999_0"
	hdr := common.ChunkHeader{ID: presentID, Index: 0, Size: int64(1)}
	require.NoError(t, store.Store(context.Background(), hdr, []byte("a")))

	ids := []string{presentID, "deadbeefdeadbeef_0"}
	failed, err := store.BulkDelete(context.Background(), 2, ids)

	// expect one failure (non-existent id)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "failed to delete")
	assert.ElementsMatch(t, []string{"deadbeefdeadbeef_0"}, failed)
	// presentID should be gone
	assert.Error(t, store.Exists(context.Background(), presentID))
}
