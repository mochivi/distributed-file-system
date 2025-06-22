# Deployment guide

This document shows a few ways to run DFS beyond the local docker-compose
cluster used during development.

---

## 1. Docker Compose (development / CI)
The file [`docker-compose.test.yml`](../docker-compose.test.yml) declares:
* 1 × Coordinator (`8080`)
* 6 × DataNodes (`8081`) each with an ephemeral volume mounted at `/app/data`
* 1 × integration-runner service that runs `go test`

Start:
```bash
docker compose -f docker-compose.test.yml up -d
```

Tear down:
```bash
docker compose -f docker-compose.test.yml down --volumes
```

Override environment variables (see `docs/configuration.md`) using an `.env`
file or `-e` flags.

---

## 2. Stand-alone binaries
Build the coordinator and datanode binaries:
```bash
make build
```

Run coordinator:
```bash
./bin/coordinator --port 8080
```

Run a datanode that registers with the coordinator:
```bash
export COORDINATOR_HOST=localhost
export DATANODE_HOST=127.0.0.1
./bin/datanode --port 8081 --data-dir /var/dfs
```

> 🚨**Note:** You need at least **3** DataNodes to satisfy the default
> replication factor.

---

## 3. Kubernetes
An example Helm chart is planned.  Until then you can run the
containers directly:

```yaml
apiVersion: apps/v1
kind: Deployment
metadata:
  name: coordinator
spec:
  replicas: 1
  selector: {matchLabels: {app: coordinator}}
  template:
    metadata: {labels: {app: coordinator}}
    spec:
      containers:
        - name: coordinator
          image: mochivi/dfs-coordinator:latest
          ports: [{containerPort: 8080}]
---
apiVersion: apps/v1
kind: StatefulSet
metadata:
  name: datanode
spec:
  serviceName: datanode
  replicas: 3
  selector: {matchLabels: {app: datanode}}
  template:
    metadata: {labels: {app: datanode}}
    spec:
      containers:
        - name: datanode
          image: mochivi/dfs-datanode:latest
          env:
            - name: COORDINATOR_HOST
              value: coordinator
          ports: [{containerPort: 8081}]
          volumeMounts:
            - name: data
              mountPath: /app/data
  volumeClaimTemplates:
    - metadata: {name: data}
      spec:
        accessModes: ["ReadWriteOnce"]
        resources: {requests: {storage: 20Gi}}
```
Apply:
```bash
kubectl apply -f dfs-cluster.yaml
```

---

## 4. Systemd service (bare-metal)
Install binaries under `/usr/local/bin/dfs-*` and create:

`/etc/dfs/datanode.env`
```ini
COORDINATOR_HOST=coordinator.service.local
DATANODE_HOST=$(hostname -f)
REPLICATE_TIMEOUT=5m
```

`/etc/systemd/system/dfs-datanode.service`
```ini
[Unit]
Description=DFS DataNode
After=network.target dfs-coordinator.service

[Service]
EnvironmentFile=/etc/dfs/datanode.env
ExecStart=/usr/local/bin/dfs-datanode --port 8081 --data-dir /var/lib/dfs
Restart=on-failure

[Install]
WantedBy=multi-user.target
```

Enable & start:
```bash
sudo systemctl daemon-reload
sudo systemctl enable --now dfs-datanode
```

---

## Monitoring & metrics
• All services export JSON logs and can be collected by Fluent-bit.  
• Prometheus metrics endpoint (`/metrics`) is planned.
