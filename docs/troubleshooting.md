# Troubleshooting

Common issues and their resolutions while running DFS locally.

| Error / Symptom | Root cause | Resolution |
|-----------------|------------|------------|
| `rpc error: code = Canceled desc = context canceled` in DataNode log | The peer (client or upstream node) closed the gRPC stream while this node was still reading. Most often happens when the test process exits before replicas finish. | Increase `REPLICATE_TIMEOUT`, wait for goroutines (`sync.WaitGroup`) before exiting, or set `WAIT_REPLICAS=false` during tests. |
| `failed to receive replicas response: EOF` on client | The primary DataNode closed the stream because replication did not complete within `REPLICATE_TIMEOUT`. | Tune `REPLICATE_TIMEOUT` or reduce chunk size / increase network throughput. |
| `insufficient replicas` error inside DataNode | Not enough healthy nodes available to satisfy `N_REPLICAS-1` replicas. | Start more DataNodes or lower replication factor (compile-time constant for now). |
| `address already in use` when starting compose | Ports 8080 / 8081 are occupied. | Stop previous cluster (`make e2e-down`) or change ports in compose file. |
| `checksum mismatch` during upload | File changed between checksum computation and streaming or data corrupted in transit. | Ensure file is immutable during upload; verify network; check for concurrent writes. |
| `dial tcp ... i/o timeout` from client | Coordinator / DataNode not reachable (container died, wrong host). | `docker compose ps` to inspect, restart containers, verify `COORDINATOR_HOST`. |

If the issue is not listed open a GitHub issue with logs from `./logs/`.  Most
services use [slog](https://pkg.go.dev/log/slog) – increase verbosity by setting
`LOG_LEVEL=debug` on the container. 