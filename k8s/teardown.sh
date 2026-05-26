#!/bin/bash
set -euo pipefail

RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m'

export PATH="/opt/homebrew/bin:$PATH"

CLUSTER_NAME="notification-local"

echo -e "${YELLOW}Deleting k3d cluster '$CLUSTER_NAME'...${NC}"

if k3d cluster list 2>/dev/null | grep -q "$CLUSTER_NAME"; then
  k3d cluster delete "$CLUSTER_NAME"
  echo -e "${GREEN}Cluster deleted.${NC}"
else
  echo -e "${RED}Cluster '$CLUSTER_NAME' not found.${NC}"
fi

echo ""
echo -e "${YELLOW}Cleaning up Docker images...${NC}"
docker rmi notification-api:local notification-consumer:local \
  notification-scheduler:local notification-dbwriter:local 2>/dev/null || true
echo -e "${GREEN}Done.${NC}"
