#!/bin/bash
set -euo pipefail

RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m'

export PATH="/opt/homebrew/bin:$PATH"

CLUSTER_NAME="notification-local"
NAMESPACE="notification"
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
PROJECT_DIR="$(dirname "$SCRIPT_DIR")"

echo -e "${BLUE}=== Notification System — K3s + KEDA Local Setup ===${NC}"
echo ""

# --- Prerequisites ---
echo -e "${YELLOW}[1/7] Checking prerequisites...${NC}"
MISSING=""
for cmd in docker k3d kubectl helm; do
  if ! command -v "$cmd" &>/dev/null; then
    MISSING="$MISSING $cmd"
  fi
done

if [ -n "$MISSING" ]; then
  echo -e "${RED}Missing required tools:${MISSING}${NC}"
  echo ""
  echo "Install commands:"
  echo "  brew install k3d kubectl helm    # macOS"
  echo "  Docker Desktop: https://docker.com/products/docker-desktop"
  exit 1
fi
echo -e "${GREEN}All prerequisites found.${NC}"

# --- k3d Cluster ---
echo ""
echo -e "${YELLOW}[2/7] Creating k3d cluster...${NC}"
if k3d cluster list | grep -q "$CLUSTER_NAME"; then
  echo "Cluster '$CLUSTER_NAME' already exists. Deleting..."
  k3d cluster delete "$CLUSTER_NAME"
fi

k3d cluster create "$CLUSTER_NAME" \
  --port "30080:30080@server:0" \
  --agents 2 \
  --k3s-arg "--disable=traefik@server:0"

kubectl config use-context "k3d-$CLUSTER_NAME"
echo -e "${GREEN}Cluster created with 1 server + 2 agents.${NC}"

# --- Docker Images ---
echo ""
echo -e "${YELLOW}[3/7] Building Docker images...${NC}"
cd "$PROJECT_DIR"

docker build -t notification-api:local       -f notification-api/Dockerfile . &
docker build -t notification-consumer:local   -f notification-consumer/Dockerfile . &
docker build -t notification-scheduler:local  -f notification-scheduler/Dockerfile . &
docker build -t notification-dbwriter:local   -f notification-dbwriter/Dockerfile . &
wait
echo -e "${GREEN}All images built.${NC}"

echo ""
echo -e "${YELLOW}[4/7] Importing images into k3d...${NC}"
k3d image import \
  notification-api:local \
  notification-consumer:local \
  notification-scheduler:local \
  notification-dbwriter:local \
  -c "$CLUSTER_NAME"
echo -e "${GREEN}Images imported.${NC}"

# --- KEDA ---
echo ""
echo -e "${YELLOW}[5/7] Installing KEDA...${NC}"
helm repo add kedacore https://kedacore.github.io/charts 2>/dev/null || true
helm repo update kedacore
if helm list -n keda 2>/dev/null | grep -q keda; then
  echo "KEDA already installed."
else
  helm install keda kedacore/keda \
    --namespace keda \
    --create-namespace \
    --wait \
    --timeout 120s
fi
echo -e "${GREEN}KEDA ready.${NC}"

# --- Infrastructure ---
echo ""
echo -e "${YELLOW}[6/7] Deploying infrastructure...${NC}"
kubectl apply -f "$SCRIPT_DIR/infra/namespace.yaml"
kubectl apply -f "$SCRIPT_DIR/infra/"

echo "Waiting for Redis..."
kubectl -n "$NAMESPACE" wait --for=condition=ready pod -l app=redis --timeout=120s
echo "Waiting for PostgreSQL..."
kubectl -n "$NAMESPACE" wait --for=condition=ready pod -l app=postgres --timeout=120s
echo "Waiting for PgBouncer..."
kubectl -n "$NAMESPACE" wait --for=condition=ready pod -l app=pgbouncer --timeout=120s
echo -e "${GREEN}Infrastructure ready.${NC}"

# --- Applications + Autoscaling ---
echo ""
echo -e "${YELLOW}[7/7] Deploying applications + KEDA autoscaling...${NC}"
kubectl apply -f "$SCRIPT_DIR/apps/"

echo "Waiting for notification-api..."
kubectl -n "$NAMESPACE" wait --for=condition=ready pod -l app=notification-api --timeout=120s

kubectl apply -f "$SCRIPT_DIR/autoscaling/"
echo -e "${GREEN}All services deployed with KEDA autoscaling.${NC}"

# --- Summary ---
echo ""
echo -e "${BLUE}============================================${NC}"
echo -e "${GREEN}  Setup complete!${NC}"
echo -e "${BLUE}============================================${NC}"
echo ""
echo -e "${BLUE}Services:${NC}"
kubectl -n "$NAMESPACE" get pods
echo ""
echo -e "${BLUE}KEDA ScaledObjects:${NC}"
kubectl -n "$NAMESPACE" get scaledobjects
echo ""
echo -e "${BLUE}API endpoint:${NC}"
echo "  http://localhost:30080"
echo ""
echo -e "${BLUE}Useful commands:${NC}"
echo "  kubectl -n $NAMESPACE get pods -w                    # Watch pod scaling"
echo "  kubectl -n $NAMESPACE get scaledobjects              # KEDA status"
echo "  kubectl -n $NAMESPACE get hpa                        # HPA created by KEDA"
echo "  kubectl -n $NAMESPACE logs -l app=notification-consumer -f  # Consumer logs"
echo ""
echo -e "${BLUE}Run the demo:${NC}"
echo "  ./k8s/demo.sh"
echo ""
