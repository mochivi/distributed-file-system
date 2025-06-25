# Cluster & Management API Refactoring Plan

This document outlines the plan for refactoring the DFS to improve modularity, testability, and observability. The core of this plan is to formalize the architecture into two distinct communication "planes".

## 1. Proposed Architecture: A Two-Plane Model

The system's architecture will be divided into two planes to create a clean separation of concerns.

### 1.1. Data & Core Control Plane (gRPC)

This plane is the high-performance engine of the distributed file system, handling all core DFS operations over a single, unified gRPC server on each node.

*   **Technology:** gRPC
*   **Responsibilities:**
    *   **Data Transfer:** Client chunk uploads and downloads.
    *   **Replication:** Inter-data-node chunk replication.
    *   **Core State Management:** Data node registration and periodic heartbeats.

### 1.2. Management Plane (HTTP/REST)

This plane acts as a "sidecar" management and observability agent for each node, running on a separate, dedicated HTTP port. It does not handle any primary DFS data transfer.

*   **Technology:** HTTP/REST with JSON
*   **Responsibilities:**
    *   **Lifecycle Management:** Exposing simple HTTP endpoints (e.g., `/status`, `/health`, `/shutdown`) for external process control.
    *   **Observability & Telemetry:** Providing endpoints to expose logs (`/logs/stream`) and metrics (`/metrics`).
    *   **Cluster Intelligence:** Handling complex logic like tracking the active coordinator via a leader election mechanism, ensuring the core gRPC client is always aware of the correct coordinator to talk to.
    * **Control Plane** The coordinator management sidecar will act as the control plane and talk to each node's management API when the user requests it. Labels will be added to the struct to allow for group selection by label pattern. The node's management API can still receive requests from other sources, allowing for debugging activities by admins. 

## 2. Actionable Refactoring Steps

The refactoring will be executed in a phased approach.

### Phase 1: Establish the Cluster Package

The immediate priority is to centralize cluster state management.

1.  **Create `internal/cluster` Package:** A new package will be created to encapsulate all logic related to cluster membership and inter-node communication.
2.  **Migrate `NodeManager`:** The existing `NodeManager` will be moved from `internal/common` to `internal/cluster`.
3.  **Refactor Dependencies:** The `coordinator` and `datanode` packages will be refactored to delegate all cluster state management and peer communication logic to the new `cluster` package.

### Phase 2: Introduce the Management Plane

Once the cluster logic is centralized, the management plane can be introduced.

1.  **Create `internal/management` Package:** A new package will be created for the HTTP management server.
2.  **Implement Basic HTTP Server:** A barebones HTTP server will be implemented with placeholder endpoints for status (`/status`) and metrics (`/metrics`).
3.  **Integrate into `main`:** The `cmd/datanode/main.go` and `cmd/coordinator/main.go` entry points will be updated to launch the new Management Plane server as a separate goroutine alongside the primary gRPC server.

### Phase 3: Future Work (Post-Refactoring)

Once the new architecture is in place, the following high-value features can be implemented on the new foundation:

*   Implement a robust persistence layer for the coordinator's metadata store.
*   Add comprehensive, resilient error handling (retries, timeouts) to the control path gRPC clients within the `cluster` package.
*   Build out advanced features in the `management` package, such as leader election tracking for a highly-available coordinator setup.
*   Implement the `DeleteFile` and `ListFiles` gRPC API methods.
*   Consolidate all configuration into a unified system (e.g., using a library like Viper).