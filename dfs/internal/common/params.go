package common

// Logging params
const (
	// Interceptor mostly uses these
	LoggingParamMethod     = "method"
	LoggingParamTimestamp  = "timestamp"
	LoggingParamGrpcStatus = "grpc_status"
	LoggingParamDuration   = "duration"

	// General, used across the board
	LoggingParamError = "error"
)

// Component names
const (
	ComponentCoordinator = "coordinator"
	ComponentDatanode    = "datanode"
)
