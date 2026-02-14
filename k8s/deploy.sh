#!/bin/bash
# Automated deployment script for Kafka Real-time Dashboard on Minikube

set -e

echo "======================================"
echo "Kafka Dashboard - Minikube Deployment"
echo "======================================"
echo ""

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

# Check if minikube is running
echo -e "${YELLOW}[1/8] Checking Minikube status...${NC}"
if ! minikube status > /dev/null 2>&1; then
  echo -e "${RED}Error: Minikube is not running!${NC}"
  echo "Please start Minikube with: minikube start --cpus=4 --memory=4096 --disk-size=20g"
  exit 1
fi
echo -e "${GREEN}✓ Minikube is running${NC}"
echo ""

# Configure Docker to use Minikube's daemon
echo -e "${YELLOW}[2/8] Configuring Docker environment for Minikube...${NC}"
eval $(minikube docker-env)
echo -e "${GREEN}✓ Docker environment configured${NC}"
echo ""

# Build the dashboard image
echo -e "${YELLOW}[3/8] Building dashboard Docker image...${NC}"
echo "This may take 5-8 minutes on first build..."
cd "$(dirname "$0")/.."
docker build -t kafka-dashboard:latest . --quiet
if [ $? -eq 0 ]; then
  echo -e "${GREEN}✓ Dashboard image built successfully${NC}"
else
  echo -e "${RED}Error: Failed to build dashboard image${NC}"
  exit 1
fi
echo ""

# Create namespace
echo -e "${YELLOW}[4/8] Creating namespace...${NC}"
kubectl apply -f k8s/namespace.yaml
echo ""

# Apply ConfigMaps
echo -e "${YELLOW}[5/8] Creating ConfigMaps...${NC}"
kubectl apply -f k8s/kafka/kafka-configmap.yaml
kubectl apply -f k8s/dashboard/dashboard-configmap.yaml
kubectl apply -f k8s/seed/seed-configmap.yaml
echo ""

# Deploy Kafka
echo -e "${YELLOW}[6/8] Deploying Kafka...${NC}"
kubectl apply -f k8s/kafka/kafka-statefulset.yaml
kubectl apply -f k8s/kafka/kafka-service-headless.yaml
kubectl apply -f k8s/kafka/kafka-service.yaml

echo "Waiting for Kafka to be ready (this may take 30-60 seconds)..."
kubectl wait --for=condition=ready pod/kafka-0 -n kafka-dashboard --timeout=180s
if [ $? -eq 0 ]; then
  echo -e "${GREEN}✓ Kafka is ready${NC}"
else
  echo -e "${RED}Error: Kafka failed to start${NC}"
  echo "Check logs with: kubectl logs kafka-0 -n kafka-dashboard"
  exit 1
fi
echo ""

# Deploy Dashboard
echo -e "${YELLOW}[7/8] Deploying Dashboard...${NC}"
kubectl apply -f k8s/dashboard/dashboard-deployment.yaml
kubectl apply -f k8s/dashboard/dashboard-service.yaml

echo "Waiting for Dashboard to be ready..."
kubectl wait --for=condition=available deployment/dashboard -n kafka-dashboard --timeout=120s
if [ $? -eq 0 ]; then
  echo -e "${GREEN}✓ Dashboard is ready${NC}"
else
  echo -e "${RED}Error: Dashboard failed to start${NC}"
  echo "Check logs with: kubectl logs -n kafka-dashboard deployment/dashboard"
  exit 1
fi
echo ""

# Deploy Seed Producer
echo -e "${YELLOW}[8/8] Deploying Seed Producer...${NC}"
kubectl apply -f k8s/seed/seed-deployment.yaml
echo -e "${GREEN}✓ Seed producer deployed${NC}"
echo ""

# Get Minikube IP and display access information
MINIKUBE_IP=$(minikube ip)
echo "======================================"
echo -e "${GREEN}Deployment Complete!${NC}"
echo "======================================"
echo ""
echo "Dashboard URL: http://$MINIKUBE_IP:30001"
echo ""
echo "Quick Commands:"
echo "  - View pods:           kubectl get pods -n kafka-dashboard"
echo "  - View services:       kubectl get svc -n kafka-dashboard"
echo "  - Dashboard logs:      kubectl logs -n kafka-dashboard deployment/dashboard -f"
echo "  - Kafka logs:          kubectl logs -n kafka-dashboard kafka-0 -f"
echo "  - Seed logs:           kubectl logs -n kafka-dashboard deployment/seed -f"
echo "  - List topics:         kubectl exec -it kafka-0 -n kafka-dashboard -- /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --list"
echo "  - Open dashboard:      minikube service dashboard -n kafka-dashboard"
echo "  - Port forward:        kubectl port-forward -n kafka-dashboard service/dashboard 3001:3001"
echo ""
echo "To cleanup: ./k8s/cleanup.sh"
echo ""

# Optionally open the dashboard
read -p "Open dashboard in browser? (y/n) " -n 1 -r
echo
if [[ $REPLY =~ ^[Yy]$ ]]; then
  minikube service dashboard -n kafka-dashboard
fi
