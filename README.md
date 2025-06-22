# Distributed File System (DFS) – Go implementation

[![Go](https://img.shields.io/badge/go-1.22+-00ADD8?logo=go)](https://golang.org/) [![CI](https://github.com/mochivi/distributed-file-system/actions/workflows/integration-tests.yml/badge.svg)](https://github.com/mochivi/distributed-file-system/actions) [![License](https://img.shields.io/github/license/mochivi/distributed-file-system)](LICENSE)

A **self-contained distributed file-system** written in Go.  
It is a learning project that demonstrates chunk-based storage, streaming gRPC replication, and metadata coordination – all designed to run locally via Docker.

> 📖 Detailed design notes are kept in [`docs/design.md`](docs/design.md).

---

## Table of contents
1. [Features](#features)
2. [Prerequisites](#prerequisites)
3. [Installation](#installation)
4. [Quick start](#quick-start)
5. [Configuration](#configuration)
6. [Project layout](#project-layout)
7. [Usage examples](#usage-examples)
8. [Testing](#testing)
9. [Troubleshooting](#troubleshooting)
10. [Roadmap](#roadmap)
11. [Contributing](#contributing)
12. [License](#license)

---

## Features
* **Coordinator** service that stores *metadata only* (file → chunk map, node membership).
* **DataNode** that stores chunk bytes on local disk and replicates to peers.
* **Client SDK** that splits files, uploads chunks in parallel and confirms the upload.
* Bidirectional streaming gRPC (`ChunkDataStream`) with back-pressure and checksums.
* Default **replication factor = 3** (1 primary + 2 replicas).
* Integration test-bed – `docker-compose` spins up 1 × Coordinator + 6 × DataNodes.
* Pluggable chunk storage – local filesystem today; S3 / GCS adapters planned.
* Zero external dependencies beyond Go stdlib + gRPC.

---

## Prerequisites
| Tool | Version | Notes |
|------|---------|-------|
| Go   | 1.22 or newer | for building binaries & running unit tests |
| Docker & Compose | 20.10+ | used for local multi-node cluster |
| `protoc` + `protoc-gen-go` | optional | only required when modifying `.proto` files |

---

## Installation
Clone the repository and pull sub-modules (none right now but good habit):

```bash
$ git clone https://github.com/mochivi/distributed-file-system.git
$ cd distributed-file-system
```

Generate protobuf stubs (optional):
```bash
make proto
```

Build binaries & Docker images:
```bash
make build          # go build ./...
make docker-build   # docker build coordinator & datanode images
```

---

## Quick start
Run the integration cluster locally (1 coordinator + 6 datanodes):

```bash
make integration        # same target used by CI
```

Logs are tailed to `./logs/*.log`.  Shut everything down with:
```bash
make integration-down
```

> **Tip:** The compose file [`docker-compose.test.yml`](docker-compose.test.yml) can be run directly with `docker compose up` if you prefer.

---

## Configuration
Configuration is currently environment-variable driven.  The most common knobs are:

| Variable | Default | Description |
|----------|---------|-------------|
| `COORDINATOR_HOST` | `coordinator` | Hostname used by DataNodes & client to reach the coordinator |
| `COORDINATOR_PORT` | `8080` | gRPC port exposed by coordinator |
| `DATANODE_HOST` | container name | Advertised host for the DataNode |
| `DATANODE_PORT` | `8081` | gRPC port for DataNode |
| `REPLICATE_TIMEOUT` | `2m` | How long the primary waits for each replica during upload |
| `N_REPLICAS` | **constant 3** | Replication factor (configurable at build-time – TODO make runtime cfg) |

A complete reference with examples lives in [`docs/configuration.md`](docs/configuration.md).

---

## Project layout
```text
cmd/            Entrypoints (main.go for coordinator & datanode)
internal/       Core libraries (client, common, coordinator, datanode, storage)
pkg/proto/      Generated protobuf & gRPC stubs
deploy/         Dockerfiles, compose, CI scripts
tests/          Unit + integration tests
```

---

## Usage examples
Upload a file (from the repository root):
```bash
# assuming the compose environment is running
$ go run ./cmd/client upload --file ./README.md --path /user/docs/readme.txt
```

Download it back:
```bash
$ go run ./cmd/client download --path /user/docs/readme.txt --out /tmp/readme.txt
```

List chunks on a DataNode:
```bash
$ grpcurl -plaintext localhost:8081 datanode.DataNodeService/ListChunks
```

More examples live in [`tests/integration`](tests/integration/) and the Makefile.

---

## Testing
* **Unit tests** – `make test` runs `go test ./...` with race detector.
* **Integration** – `make integration` spins up the full cluster and runs end-to-end scenarios.
* **Lint** – `make lint` executes `golangci-lint run` (requires the tool installed).

CI executes *all* of the above on every pull request.

---

## Troubleshooting
| Symptom | Likely cause | Fix |
|---------|--------------|-----|
| `rpc error: code = Canceled desc = context canceled` in DataNode logs | Client closed the stream (timeout or test runner exited) while node was still in `Recv()` | Increase `REPLICATE_TIMEOUT` or wait for goroutines before exiting |
| `failed to receive replicas response: EOF` on client | Replication exceeded timeout | Tune `REPLICATE_TIMEOUT` or reduce chunk size |

See [`docs/troubleshooting.md`](docs/troubleshooting.md) for more.

---

## Roadmap
See open issues labelled **`roadmap`** – major items include garbage cleaning, TLS, access control and a richer CLI.
