package common

import "fmt"

func FormatChunkID(path string, chunkID int) string {
	return fmt.Sprintf("%s_chunk_%d", path, chunkID)
}
