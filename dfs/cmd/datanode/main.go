package main

import (
	"context"
	"fmt"
	"log"
	"log/slog"
	"net"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/mochivi/distributed-file-system/internal/clients"
	"github.com/mochivi/distributed-file-system/internal/cluster"
	datanode_controllers "github.com/mochivi/distributed-file-system/internal/cluster/datanode/controllers"
	datanode_services "github.com/mochivi/distributed-file-system/internal/cluster/datanode/services"
	"github.com/mochivi/distributed-file-system/internal/cluster/shared"
	"github.com/mochivi/distributed-file-system/internal/cluster/state"
	"github.com/mochivi/distributed-file-system/internal/common"
	"github.com/mochivi/distributed-file-system/internal/config"
	"github.com/mochivi/distributed-file-system/internal/datanode"
	"github.com/mochivi/distributed-file-system/internal/grpcutil"
	"github.com/mochivi/distributed-file-system/internal/storage/chunk"
	"github.com/mochivi/distributed-file-system/internal/storage/encoding"
	"github.com/mochivi/distributed-file-system/internal/storage/metadata"
	"github.com/mochivi/distributed-file-system/pkg/client_pool"
	"github.com/mochivi/distributed-file-system/pkg/logging"
	"github.com/mochivi/distributed-file-system/pkg/proto"
	"github.com/mochivi/distributed-file-system/pkg/streaming"
	"github.com/mochivi/distributed-file-system/pkg/utils"
	"github.com/spf13/afero"
	"google.golang.org/grpc"
)

type container struct {
	// shared or grpc server dependencies
	chunkStore          chunk.ChunkStorage
	replicationManager  datanode.ReplicationProvider
	sessionManager      streaming.SessionManager
	clusterStateManager state.ClusterStateManager
	coordinatorFinder   state.CoordinatorFinder
	nodeSelector        state.NodeSelector
	streamerFactory     streaming.ServerStreamerFactory
	clientPoolFactory   client_pool.ClientPoolFactory

	// Node Agent dependencies
	metadataStore   metadata.MetadataStore
	metadataScanner shared.MetadataScannerProvider
}

func setupDependencies(ctx context.Context, cfg *config.DatanodeAppConfig, logger *slog.Logger) *container {
	chunkSerializer := encoding.NewProtoSerializer()
	chunkStore, err := chunk.NewChunkDiskStorage(afero.NewOsFs(), cfg.Node.DiskStorage, chunkSerializer)
	if err != nil {
		log.Fatal(err.Error())
	}

	clusterStateManager := state.NewClusterStateManager()
	nodeSelector := state.NewNodeSelector(clusterStateManager)
	streamer := streaming.NewClientStreamer(cfg.Node.Streamer)
	replicationManager := datanode.NewParalellReplicationService(cfg.Node.Replication, streamer, logger)
	sessionManager := streaming.NewStreamingSessionManager(cfg.Node.StreamingSession, logger)
	coordinatorFinder := state.NewCoordinatorFinder()

	// Node Agent dependencies

	// This is the temporary solution until some sort of service/node discovery is implemented
	coordinatorHost := utils.GetEnvString("COORDINATOR_HOST", "coordinator")
	coordinatorPort := utils.GetEnvInt("COORDINATOR_PORT", 8080)
	coordinatorNode := &common.NodeInfo{
		ID:     "coordinator", // TODO: change to coordinator ID when implemented, should be received from some service discovery/config storage system
		Host:   coordinatorHost,
		Port:   coordinatorPort,
		Status: common.NodeHealthy,
	}
	coordinatorClient, err := clients.NewCoordinatorClient(coordinatorNode)
	if err != nil {
		log.Fatalf("failed to connect to coordinator")
	}
	metadataStore := metadata.NewDatanodeMetadataStore(coordinatorClient)
	metadataScanner := shared.NewMetadataScannerService(ctx, metadataStore, logger)

	streamerFactory := func(sessionManager streaming.SessionManager, config config.StreamerConfig) streaming.ServerStreamer {
		return streaming.NewServerStreamer(sessionManager, config)
	}

	clientPoolFactory := func(nodes []*common.NodeInfo) (client_pool.ClientPool, error) {
		return client_pool.NewRotatingClientPool(nodes)
	}

	return &container{
		chunkStore:          chunkStore,
		replicationManager:  replicationManager,
		sessionManager:      sessionManager,
		clusterStateManager: clusterStateManager,
		coordinatorFinder:   coordinatorFinder,
		nodeSelector:        nodeSelector,
		streamerFactory:     streamerFactory,
		clientPoolFactory:   clientPoolFactory,
		metadataStore:       metadataStore,
		metadataScanner:     metadataScanner,
	}
}

func setupNodeAgent(ctx context.Context, container *container, cfg *config.DatanodeAppConfig, nodeID string, logger *slog.Logger) (*datanode_controllers.NodeAgentControllers, *datanode_services.NodeAgentServices) {

	// Controllers
	heartbeatController := datanode_controllers.NewHeartbeatController(ctx, cfg.Agent.Heartbeat, logger)
	gcController := datanode_controllers.NewOrphanedChunksGCController(ctx, container.metadataScanner, container.chunkStore, cfg.Agent.OrphanedChunksGC, nodeID, logger)

	controllers := datanode_controllers.NewNodeAgentControllers(
		heartbeatController,
		gcController,
	)

	// Services
	registerService := datanode_services.NewRegisterService(logger)
	services := datanode_services.NewNodeAgentServices(container.coordinatorFinder, registerService)

	return &controllers, &services
}

func main() {
	// Load configuration
	appConfig, err := config.LoadDatanodeConfig(".") // Load datanode-specific config
	if err != nil {
		log.Fatalf("failed to load configuration: %v", err)
	}

	// Load datanode info
	datanodeHost := utils.GetEnvString("DATANODE_HOST", "0.0.0.0")
	datanodePort := utils.GetEnvInt("DATANODE_PORT", 8081)
	datanodeInfo := common.NodeInfo{
		ID:       uuid.NewString(),
		Host:     datanodeHost,
		Port:     datanodePort,
		Capacity: 10 * 1024 * 1024 * 1024, // gB
		Used:     0,
		Status:   common.NodeHealthy,
		LastSeen: time.Now(),
	}

	// Setup structured logging
	rootLogger, err := logging.InitLogger()
	if err != nil {
		log.Fatal(err.Error())
	}
	logger := logging.ExtendLogger(rootLogger,
		slog.String(common.LogComponent, common.ComponentDatanode),
		slog.String(common.LogNodeID, datanodeInfo.ID))

	// Root context and cancel function for coordinated shutdown
	ctx, cancel := context.WithCancel(context.Background())

	// Setup dependencies
	container := setupDependencies(ctx, appConfig, logger)

	// Setup gRPC server
	setupGrpcFunc := func(wg *sync.WaitGroup, errChan chan error) (*grpc.Server, net.Listener) {
		serverContainer := datanode.NewContainer(container.chunkStore, container.replicationManager, container.sessionManager,
			container.clusterStateManager, container.coordinatorFinder, container.nodeSelector, container.streamerFactory, container.clientPoolFactory)
		server := datanode.NewDataNodeServer(&datanodeInfo, appConfig.Node, serverContainer, logger)

		grpcServer := grpc.NewServer(
			grpc.ChainUnaryInterceptor(
				grpcutil.NewLoggingInterceptor(rootLogger),
				grpcutil.ErrorsInterceptor,
			),
		)
		proto.RegisterDataNodeServiceServer(grpcServer, server)

		listener, err := net.Listen("tcp", fmt.Sprintf(":%d", datanodePort))
		if err != nil {
			log.Fatalf("Failed to listen: %v", err)
		}

		return grpcServer, listener
	}

	// NodeAgent - runs control loops, provides services and supervises gRPC server
	controllers, services := setupNodeAgent(ctx, container, appConfig, datanodeInfo.ID, logger)
	nodeAgent := cluster.NewNodeAgent(ctx, cancel, &appConfig.Agent, &datanodeInfo, container.clusterStateManager, services, controllers, logger)

	launchNodeAgentFunc := func(wg *sync.WaitGroup, errChan chan error) {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if err := nodeAgent.Run(); err != nil {
				errChan <- fmt.Errorf("node agent failed: %w", err)
			}
		}()
	}

	if err := grpcutil.Launch(ctx, cancel, logger, setupGrpcFunc, launchNodeAgentFunc, 15*time.Second); err != nil {
		log.Fatalf("Failed to launch datanode: %v", err)
	}
}
