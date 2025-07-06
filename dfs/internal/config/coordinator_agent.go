package config

import "time"

// Configuration for the coordinator agent, which runs controller loops and provides cluster services
type CoordinatorAgentConfig struct {
	DeletedFilesGC DeletedFilesGCControllerConfig `mapstructure:"deleted_files_gc" validate:"required"`
}

func DefaultCoordinatorAgentConfig() CoordinatorAgentConfig {
	return CoordinatorAgentConfig{
		DeletedFilesGC: DefaultDeletedFilesGCControllerConfig(),
	}
}

type DeletedFilesGCControllerConfig struct {
	Interval           time.Duration `mapstructure:"interval" validate:"required,gt=0"`
	Timeout            time.Duration `mapstructure:"timeout" validate:"required,gt=0"` // Timeout until will pause the GC cycle, work resumes in next cycle
	RecoveryTimeout    time.Duration `mapstructure:"recovery_timeout" validate:"required,gt=0"`
	BatchSize          int           `mapstructure:"batch_size" validate:"required,gt=0"`
	ConcurrentRequests int           `mapstructure:"concurrent_requests" validate:"required,gt=0"`
}

func DefaultDeletedFilesGCControllerConfig() DeletedFilesGCControllerConfig {
	return DeletedFilesGCControllerConfig{
		Interval:           1 * time.Hour,
		RecoveryTimeout:    2 * time.Hour,
		BatchSize:          1000,
		ConcurrentRequests: 5,
	}
}
