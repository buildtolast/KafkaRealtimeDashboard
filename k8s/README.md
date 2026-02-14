# Kubernetes / Minikube Deployment Guide

This directory contains Kubernetes manifests for deploying the Kafka Real-time Dashboard on Minikube.

## Architecture

The deployment consists of three main components:

1. **Kafka Broker** - StatefulSet with persistent storage (5Gi)
   - Single-node KRaft mode (no Zookeeper required)
   - Ports: 9092 (internal), 9094 (external via NodePort 30094)

2. **Dashboard** - Deployment with the Rust backend + React frontend
   - Built from the project's multi-stage Dockerfile (~85MB)
   - Exposed via NodePort on port 30001
   - Includes init container to wait for Kafka readiness

3. **Seed Producer** - Deployment for continuous test data generation
   - Creates 4 topics: orders, users, notifications, logs
   - Produces JSON messages every 2 seconds

## Prerequisites

- Minikube installed and running
- kubectl configured
- Docker installed
- Minimum resources: 4 CPU cores, 4GB RAM, 20GB disk

## Quick Start

### 1. Start Minikube

```bash
minikube start --cpus=4 --memory=4096 --disk-size=20g
```

### 2. Deploy Everything

```bash
# From the project root directory
./k8s/deploy.sh
```

The script will:
- Build the dashboard Docker image using Minikube's Docker daemon
- Create the `kafka-dashboard` namespace
- Deploy Kafka, Dashboard, and Seed Producer
- Wait for all components to be ready
- Display the dashboard URL

### 3. Access the Dashboard

After deployment completes, you can access the dashboard at:

```
http://$(minikube ip):30001
```

Or use the Minikube service command to open it automatically:

```bash
minikube service dashboard -n kafka-dashboard
```

Or port-forward to localhost:

```bash
kubectl port-forward -n kafka-dashboard service/dashboard 3001:3001
# Then open http://localhost:3001
```

## Manual Deployment

If you prefer to deploy manually:

```bash
# Configure Docker for Minikube
eval $(minikube docker-env)

# Build the dashboard image
docker build -t kafka-dashboard:latest .

# Apply manifests in order
kubectl apply -f k8s/namespace.yaml
kubectl apply -f k8s/kafka/kafka-configmap.yaml
kubectl apply -f k8s/dashboard/dashboard-configmap.yaml
kubectl apply -f k8s/seed/seed-configmap.yaml

# Deploy Kafka
kubectl apply -f k8s/kafka/kafka-statefulset.yaml
kubectl apply -f k8s/kafka/kafka-service-headless.yaml
kubectl apply -f k8s/kafka/kafka-service.yaml

# Wait for Kafka to be ready
kubectl wait --for=condition=ready pod/kafka-0 -n kafka-dashboard --timeout=180s

# Deploy Dashboard
kubectl apply -f k8s/dashboard/dashboard-deployment.yaml
kubectl apply -f k8s/dashboard/dashboard-service.yaml

# Wait for Dashboard to be ready
kubectl wait --for=condition=available deployment/dashboard -n kafka-dashboard --timeout=120s

# Deploy Seed Producer
kubectl apply -f k8s/seed/seed-deployment.yaml
```

## Directory Structure

```
k8s/
├── README.md                           # This file
├── deploy.sh                           # Automated deployment script
├── cleanup.sh                          # Cleanup script
├── namespace.yaml                      # Namespace definition
├── kafka/
│   ├── kafka-configmap.yaml           # Kafka environment variables
│   ├── kafka-statefulset.yaml         # Kafka StatefulSet with PVC
│   ├── kafka-service-headless.yaml    # Headless service for StatefulSet
│   └── kafka-service.yaml             # ClusterIP + NodePort services
├── dashboard/
│   ├── dashboard-configmap.yaml       # Dashboard environment variables
│   ├── dashboard-deployment.yaml      # Dashboard Deployment
│   └── dashboard-service.yaml         # NodePort service (port 30001)
└── seed/
    ├── seed-configmap.yaml            # Seed script as ConfigMap
    └── seed-deployment.yaml           # Seed producer Deployment
```

## Verification

### Check Pod Status

```bash
kubectl get pods -n kafka-dashboard
```

Expected output:
```
NAME                        READY   STATUS    RESTARTS   AGE
kafka-0                     1/1     Running   0          2m
dashboard-xxxxxxxxxx-xxxxx  1/1     Running   0          1m
seed-xxxxxxxxxx-xxxxx       1/1     Running   0          30s
```

### Verify Kafka Topics

```bash
kubectl exec -it kafka-0 -n kafka-dashboard -- \
  /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --list
```

Expected output:
```
orders
users
notifications
logs
```

### Test Dashboard API

```bash
# From host machine
curl "http://$(minikube ip):30001/api/topics"
```

### View Logs

```bash
# Dashboard logs
kubectl logs -n kafka-dashboard deployment/dashboard -f

# Kafka logs
kubectl logs -n kafka-dashboard kafka-0 -f

# Seed producer logs
kubectl logs -n kafka-dashboard deployment/seed -f
```

## Configuration

### Kafka Configuration

Kafka is configured via [kafka/kafka-configmap.yaml](kafka/kafka-configmap.yaml):

- **KAFKA_NODE_ID**: 1 (single node)
- **KAFKA_PROCESS_ROLES**: broker,controller (KRaft mode)
- **KAFKA_LISTENERS**: PLAINTEXT (9092), CONTROLLER (9093), EXTERNAL (9094)
- **KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR**: 1 (single node)

### Dashboard Configuration

Dashboard is configured via [dashboard/dashboard-configmap.yaml](dashboard/dashboard-configmap.yaml):

- **KAFKA_BROKERS**: kafka:9092 (internal service)
- **SERVER_HOST**: 0.0.0.0
- **SERVER_PORT**: 3001
- **RUST_LOG**: info

### Resource Limits

Current resource allocations:

- **Kafka**: 1Gi-2Gi memory, 500m-1000m CPU
- **Dashboard**: 256Mi-512Mi memory, 250m-500m CPU
- **Seed**: 128Mi-256Mi memory, 100m-200m CPU

Adjust these in the respective deployment/statefulset files if needed.

## Scaling

### Scale Dashboard

The dashboard can be scaled horizontally:

```bash
kubectl scale deployment dashboard --replicas=3 -n kafka-dashboard
```

Each replica will create its own Kafka consumer connections.

### Scale Kafka

For multi-node Kafka setup:

1. Update `kafka-statefulset.yaml` to increase replicas
2. Update `KAFKA_CONTROLLER_QUORUM_VOTERS` in `kafka-configmap.yaml`
3. Update replication factors to match node count

## Troubleshooting

### Kafka Pod Not Starting

```bash
# Check logs
kubectl logs kafka-0 -n kafka-dashboard

# Check PVC binding
kubectl get pvc -n kafka-dashboard

# Describe pod for events
kubectl describe pod kafka-0 -n kafka-dashboard
```

### Dashboard Can't Connect to Kafka

```bash
# Test DNS resolution
kubectl run -it --rm debug --image=busybox:1.36 --restart=Never -n kafka-dashboard -- \
  nslookup kafka

# Test connectivity
kubectl run -it --rm debug --image=busybox:1.36 --restart=Never -n kafka-dashboard -- \
  nc -zv kafka 9092

# Check dashboard logs for errors
kubectl logs -n kafka-dashboard deployment/dashboard | grep -i error
```

### Seed Topics Not Created

```bash
# Check seed logs
kubectl logs -n kafka-dashboard deployment/seed

# Manually create topics if needed
kubectl exec -it kafka-0 -n kafka-dashboard -- \
  /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 \
  --create --topic orders --partitions 2 --replication-factor 1
```

### Image Pull Errors

If you see `ImagePullBackOff` errors:

1. Ensure you ran `eval $(minikube docker-env)` before building
2. Verify image exists: `docker images | grep kafka-dashboard`
3. Check `imagePullPolicy: Never` is set in deployment

## Cleanup

### Delete All Resources

```bash
./k8s/cleanup.sh
```

Or manually:

```bash
kubectl delete namespace kafka-dashboard
```

### Stop Minikube

```bash
minikube stop
```

### Complete Reset

```bash
minikube delete
```

## External Access to Kafka

If you need to access Kafka from your host machine (for testing with kafka-console-producer, etc.):

The Kafka broker is exposed via NodePort on port 30094:

```bash
# Produce a test message
echo "test message" | docker run -i --rm apache/kafka:3.7.0 \
  /opt/kafka/bin/kafka-console-producer.sh \
  --bootstrap-server $(minikube ip):30094 \
  --topic orders

# Consume messages
docker run -it --rm apache/kafka:3.7.0 \
  /opt/kafka/bin/kafka-console-consumer.sh \
  --bootstrap-server $(minikube ip):30094 \
  --topic orders \
  --from-beginning
```

## Persistent Data

Kafka uses a PersistentVolumeClaim for data storage:

- **Size**: 5Gi
- **Access Mode**: ReadWriteOnce
- **Storage Class**: Minikube default (hostPath)

Data persists across pod restarts but not cluster deletion. To preserve data:

```bash
# View PVCs
kubectl get pvc -n kafka-dashboard

# Backup (example)
kubectl exec kafka-0 -n kafka-dashboard -- tar czf /tmp/kafka-data.tar.gz /var/lib/kafka/data
kubectl cp kafka-dashboard/kafka-0:/tmp/kafka-data.tar.gz ./kafka-backup.tar.gz
```

## Notes

- **Build Time**: First Docker build takes ~5-8 minutes (compiles Rust + librdkafka)
- **Startup Time**: Kafka readiness ~30-60 seconds, Dashboard ~10-20 seconds
- **WebSocket**: Works seamlessly over NodePort, no special configuration needed
- **Runtime Config**: Dashboard supports changing Kafka broker via `/api/broker` endpoint

## Alternative Configurations

### Using Ingress

Enable Minikube Ingress addon:

```bash
minikube addons enable ingress
```

Create an Ingress resource:

```yaml
apiVersion: networking.k8s.io/v1
kind: Ingress
metadata:
  name: dashboard-ingress
  namespace: kafka-dashboard
spec:
  rules:
  - host: kafka-dashboard.local
    http:
      paths:
      - path: /
        pathType: Prefix
        backend:
          service:
            name: dashboard
            port:
              number: 3001
```

Add to `/etc/hosts`:

```
<minikube-ip> kafka-dashboard.local
```

### Using LoadBalancer with Minikube Tunnel

```bash
# In a separate terminal
minikube tunnel

# Change dashboard-service.yaml type to LoadBalancer
```

## Support

For issues or questions:
- Check the main [project README](../README.md)
- Review Kubernetes events: `kubectl get events -n kafka-dashboard`
- Examine pod logs as shown in the Verification section above
