package main

import (
	"context"
	"fmt"
	"log"
	"log/slog"
	"net"
	"sync"
	"time"

	"github.com/mochivi/distributed-file-system/internal/cluster"
	coordinator_controllers "github.com/mochivi/distributed-file-system/internal/cluster/coordinator/controllers"
	"github.com/mochivi/distributed-file-system/internal/cluster/shared"
	"github.com/mochivi/distributed-file-system/internal/cluster/state"
	"github.com/mochivi/distributed-file-system/internal/common"
	"github.com/mochivi/distributed-file-system/internal/config"
	"github.com/mochivi/distributed-file-system/internal/coordinator"
	"github.com/mochivi/distributed-file-system/internal/launcher"
	"github.com/mochivi/distributed-file-system/internal/storage"
	"github.com/mochivi/distributed-file-system/internal/storage/metadata"
	"github.com/mochivi/distributed-file-system/pkg/logging"
	"github.com/mochivi/distributed-file-system/pkg/proto"
	"github.com/mochivi/distributed-file-system/pkg/utils"
	"google.golang.org/grpc"
)

// Contains all required dependencies
type container struct {
	// gRPC server dependencies
	clusterStateHistoryManager state.ClusterStateHistoryManager
	selector                   cluster.NodeSelector
	metaStore                  storage.MetadataStore
	metadataManager            coordinator.MetadataSessionManager // Coordinates when to actually commit metadata

	// Node Agent dependencies
	metadataScanner shared.MetadataScannerProvider
	nodeInfo        *common.NodeInfo
}

func setupDependencies(ctx context.Context, cfg *config.CoordinatorAppConfig, logger *slog.Logger) *container {

	// gRPC server dependencies
	metadataStore := metadata.NewMetadataLocalStorage()
	metadataManager := coordinator.NewMetadataSessionManager(cfg.Coordinator.Metadata.CommitTimeout, logger)
	clusterStateHistoryManager := state.NewClusterStateHistoryManager(cfg.Coordinator.State)
	nodeSelector := cluster.NewNodeSelector(clusterStateHistoryManager)

	// NodeAgent dependencies
	metadataScanner := shared.NewMetadataScannerService(ctx, metadataStore, logger)

	// TODO: Improve this as its not the best way to get the coordinator host and port
	coordinatorHost := utils.GetEnvString("COORDINATOR_HOST", "0.0.0.0")
	coordinatorPort := utils.GetEnvInt("COORDINATOR_PORT", 8080)
	nodeInfo := &common.NodeInfo{
		ID:   "coordinator",
		Host: coordinatorHost,
		Port: coordinatorPort,
	}

	return &container{
		clusterStateHistoryManager: clusterStateHistoryManager,
		selector:                   nodeSelector,
		metaStore:                  metadataStore,
		metadataManager:            metadataManager,

		metadataScanner: metadataScanner,
		nodeInfo:        nodeInfo,
	}
}

func setupNodeAgent(ctx context.Context, container *container, cfg *config.CoordinatorAppConfig, logger *slog.Logger) *coordinator_controllers.CoordinatorNodeAgentControllers {

	gc := coordinator_controllers.NewDeletedFilesGCController(ctx, container.metadataScanner, &cfg.Agent.DeletedFilesGC, logger)
	controllers := coordinator_controllers.NewCoordinatorNodeAgentControllers(gc)

	return controllers
}

func main() {
	// Load configuration
	appConfig, err := config.LoadCoordinatorConfig(".")
	if err != nil {
		log.Fatalf("Failed to load configuration: %v", err)
	}

	// Coordinator dependencies
	rootLogger, err := logging.InitLogger()
	if err != nil {
		log.Fatalf("Failed to initialize logger: %v", err)
	}
	logger := logging.ExtendLogger(rootLogger, slog.String("node_id", appConfig.Coordinator.ID))

	// Root context and cancel function for coordinated shutdown
	ctx, cancel := context.WithCancel(context.Background())

	// Setup dependencies
	container := setupDependencies(ctx, appConfig, logger)

	// gRPC server setup
	setupGrpcFunc := func(wg *sync.WaitGroup, errChan chan error) (*grpc.Server, net.Listener) {
		serverContainer := coordinator.NewContainer(container.metaStore, container.metadataManager, container.clusterStateHistoryManager, container.selector)
		server := coordinator.NewCoordinator(&appConfig.Coordinator, serverContainer, logger)

		// gRPC server and register
		grpcServer := grpc.NewServer()
		proto.RegisterCoordinatorServiceServer(grpcServer, server)

		listener, err := net.Listen("tcp", fmt.Sprintf(":%d", appConfig.Coordinator.Port))
		if err != nil {
			log.Fatalf("Failed to listen: %v", err)
		}

		return grpcServer, listener
	}

	// NodeAgent setup
	controllers := setupNodeAgent(ctx, container, appConfig, logger)
	nodeAgent := cluster.NewCoordinatorNodeAgent(ctx, cancel, &appConfig.Agent, container.nodeInfo, controllers, logger)

	launchNodeAgentFunc := func(wg *sync.WaitGroup, errChan chan error) {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if err := nodeAgent.Run(); err != nil {
				errChan <- fmt.Errorf("node agent failed: %w", err)
			}
		}()
	}

	// Launch gRPC server and node agent -- automatically handles graceful shutdown
	if err := launcher.Launch(ctx, cancel, logger, setupGrpcFunc, launchNodeAgentFunc, 15*time.Second); err != nil {
		log.Fatalf("Failed to launch coordinator: %v", err)
	}
}
