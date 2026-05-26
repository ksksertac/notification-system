#!/bin/bash
set -euo pipefail

RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
CYAN='\033[0;36m'
NC='\033[0m'

export PATH="/opt/homebrew/bin:$PATH"

NAMESPACE="notification"
API_URL="http://localhost:30080"
API_KEY="demo-api-key-2024"
BATCH_SIZE=${1:-500}

echo -e "${BLUE}=== KEDA Autoscaling Demo ===${NC}"
echo ""

# --- Pre-check ---
echo -e "${YELLOW}[Pre-check] Current state:${NC}"
kubectl -n "$NAMESPACE" get pods -l 'app in (notification-consumer, notification-dbwriter)'
echo ""

echo -e "${YELLOW}[Pre-check] KEDA ScaledObjects:${NC}"
kubectl -n "$NAMESPACE" get scaledobjects
echo ""

# --- Health check ---
echo -e "${YELLOW}Checking API health...${NC}"
if ! curl -sf "$API_URL/health" > /dev/null 2>&1; then
  echo -e "${RED}API not reachable at $API_URL${NC}"
  echo "Make sure setup.sh has been run and port 30080 is mapped."
  exit 1
fi
echo -e "${GREEN}API is healthy.${NC}"
echo ""

# --- Send notifications ---
echo -e "${CYAN}Sending $BATCH_SIZE notifications to trigger autoscaling...${NC}"
echo -e "${CYAN}(high priority = more aggressive scaling)${NC}"
echo ""

SENT=0
FAILED=0

for i in $(seq 1 "$BATCH_SIZE"); do
  PRIORITY="high"
  if [ $((i % 3)) -eq 0 ]; then PRIORITY="normal"; fi
  if [ $((i % 5)) -eq 0 ]; then PRIORITY="low"; fi

  STATUS=$(curl -sf -o /dev/null -w "%{http_code}" \
    -X POST "$API_URL/api/v1/notifications" \
    -H "Content-Type: application/json" \
    -H "X-API-Key: $API_KEY" \
    -d "{
      \"recipient\": \"+9055512$(printf '%05d' $i)\",
      \"channel\": \"sms\",
      \"content\": \"Load test message #$i\",
      \"priority\": \"$PRIORITY\"
    }" 2>/dev/null) || STATUS="000"

  if [ "$STATUS" = "201" ]; then
    SENT=$((SENT + 1))
  else
    FAILED=$((FAILED + 1))
  fi

  if [ $((i % 50)) -eq 0 ]; then
    echo -e "  Sent: ${GREEN}$SENT${NC} | Failed: ${RED}$FAILED${NC} / $i"
  fi
done

echo ""
echo -e "${GREEN}Done: $SENT sent, $FAILED failed out of $BATCH_SIZE${NC}"
echo ""

# --- Watch scaling ---
echo -e "${BLUE}============================================${NC}"
echo -e "${BLUE}  Now watch the pods scale up!${NC}"
echo -e "${BLUE}============================================${NC}"
echo ""
echo -e "Run in another terminal:"
echo -e "  ${CYAN}kubectl -n $NAMESPACE get pods -w${NC}"
echo ""
echo -e "Or watch HPA metrics:"
echo -e "  ${CYAN}kubectl -n $NAMESPACE get hpa -w${NC}"
echo ""
echo -e "Check Redis stream lag:"
echo -e "  ${CYAN}kubectl -n $NAMESPACE exec deploy/redis -- redis-cli XINFO GROUPS notifications:high${NC}"
echo ""

echo -e "${YELLOW}Watching pod count for 60 seconds...${NC}"
echo ""
for i in $(seq 1 12); do
  CONSUMER_PODS=$(kubectl -n "$NAMESPACE" get pods -l app=notification-consumer --no-headers 2>/dev/null | wc -l | tr -d ' ')
  DBWRITER_PODS=$(kubectl -n "$NAMESPACE" get pods -l app=notification-dbwriter --no-headers 2>/dev/null | wc -l | tr -d ' ')
  echo -e "  [$(date +%H:%M:%S)] consumer: ${GREEN}${CONSUMER_PODS} pods${NC} | dbwriter: ${GREEN}${DBWRITER_PODS} pods${NC}"
  sleep 5
done

echo ""
echo -e "${GREEN}Demo complete. Pods will scale back down after cooldown period (30s).${NC}"
