package chunk

type DiskStorageConfig struct {
	Enabled bool
	Kind    string // block storage, etc..
	RootDir string // full path must be used
}
