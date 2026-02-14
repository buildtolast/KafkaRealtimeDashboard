#!/bin/bash
# Cleanup script for Kafka Real-time Dashboard on Minikube

set -e

echo "======================================"
echo "Kafka Dashboard - Cleanup"
echo "======================================"
echo ""

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

# Confirm deletion
echo -e "${YELLOW}This will delete all resources in the kafka-dashboard namespace.${NC}"
read -p "Are you sure you want to continue? (y/n) " -n 1 -r
echo
if [[ ! $REPLY =~ ^[Yy]$ ]]; then
  echo "Cleanup cancelled."
  exit 0
fi

echo ""
echo -e "${YELLOW}Deleting kafka-dashboard namespace...${NC}"
kubectl delete namespace kafka-dashboard

echo ""
echo -e "${GREEN}✓ Cleanup complete!${NC}"
echo ""
echo "Note: PersistentVolumeClaims are deleted with the namespace."
echo "To completely reset Minikube, run: minikube delete"
echo ""
