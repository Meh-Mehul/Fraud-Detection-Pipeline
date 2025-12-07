#!/bin/bash
# ═══════════════════════════════════════════════════════════════════════════
# FRAUD DETECTION PIPELINE ORCHESTRATOR (FULLY CONTAINERIZED)
# ═══════════════════════════════════════════════════════════════════════════
# Usage:
#   ./pipeline.sh start    - Start with pretrain (fresh start)
#   ./pipeline.sh restart  - Restart without pretrain (use existing model)
#   ./pipeline.sh stop     - Stop all components
#   ./pipeline.sh status   - Show current status
#   ./pipeline.sh logs     - Tail all container logs
# ═══════════════════════════════════════════════════════════════════════════

set -e

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
CYAN='\033[0;36m'
NC='\033[0m' # No Color

# Configuration
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
COMPOSE_FILE="docker/docker-compose-full.yml"

# ═══════════════════════════════════════════════════════════════════════════
# HELPER FUNCTIONS
# ═══════════════════════════════════════════════════════════════════════════

print_header() {
    echo -e "${CYAN}"
    echo "═══════════════════════════════════════════════════════════════════════════"
    echo "   FRAUD DETECTION PIPELINE - $1"
    echo "═══════════════════════════════════════════════════════════════════════════"
    echo -e "${NC}"
}

print_step() {
    echo -e "${BLUE}[$(date '+%H:%M:%S')]${NC} ${GREEN}$1${NC}"
}

print_warning() {
    echo -e "${BLUE}[$(date '+%H:%M:%S')]${NC} ${YELLOW}⚠️  $1${NC}"
}

print_error() {
    echo -e "${BLUE}[$(date '+%H:%M:%S')]${NC} ${RED}❌ $1${NC}"
}

print_success() {
    echo -e "${BLUE}[$(date '+%H:%M:%S')]${NC} ${GREEN}✓ $1${NC}"
}

wait_for_container() {
    local container=$1
    local max_attempts=60
    local attempt=1
    
    echo -n "   Waiting for $container..."
    while [ $attempt -le $max_attempts ]; do
        if docker ps --filter "name=$container" --filter "status=running" | grep -q "$container"; then
            echo -e " ${GREEN}RUNNING${NC}"
            return 0
        fi
        echo -n "."
        sleep 2
        ((attempt++))
    done
    echo -e " ${RED}TIMEOUT${NC}"
    return 1
}

# ═══════════════════════════════════════════════════════════════════════════
# DOCKER COMMANDS
# ═══════════════════════════════════════════════════════════════════════════

build_images() {
    # Fast build with cache (for restart)
    print_step "Building Docker images (cached)..."
    cd "$SCRIPT_DIR"
    docker-compose -f "$COMPOSE_FILE" build
    print_success "Docker images built"
}

build_images_fresh() {
    # Full rebuild without cache (for start)
    print_step "Building Docker images (no cache - fresh)..."
    cd "$SCRIPT_DIR"
    docker-compose -f "$COMPOSE_FILE" build --no-cache
    print_success "Docker images built"
}

start_infrastructure() {
    print_step "Starting infrastructure (Redis, NATS, Prometheus, Grafana)..."
    cd "$SCRIPT_DIR"
    
    docker-compose -f "$COMPOSE_FILE" up -d redis nats prometheus grafana
    
    # Wait for health checks
    sleep 5
    wait_for_container "fraud-redis"
    wait_for_container "fraud-nats"
    wait_for_container "fraud-prometheus"
    wait_for_container "fraud-grafana"
    
    print_success "Infrastructure started"
}

run_pretrain() {
    print_step "Running pretrain (this may take a few minutes)..."
    cd "$SCRIPT_DIR"
    
    # Create empty state files if they don't exist
    touch review_stats.json frontend_queue.json negative_transactions.json 2>/dev/null || true
    
    # Run pretrain container (blocking)
    docker-compose -f "$COMPOSE_FILE" --profile pretrain run --rm pretrain
    
    if [ $? -eq 0 ]; then
        print_success "Pretrain completed"
    else
        print_error "Pretrain failed!"
        exit 1
    fi
}

load_redis_stats() {
    print_step "Loading Redis stats..."
    cd "$SCRIPT_DIR"
    
    docker-compose -f "$COMPOSE_FILE" --profile init run --rm redis-loader
    
    print_success "Redis stats loaded"
}

start_pipeline_nodes() {
    print_step "Starting pipeline nodes..."
    cd "$SCRIPT_DIR"
    
    docker-compose -f "$COMPOSE_FILE" up -d detector stats-updater feedback report publisher frontend negative-collector
    
    sleep 5
    wait_for_container "fraud-detector"
    wait_for_container "fraud-stats-updater"
    wait_for_container "fraud-feedback"
    wait_for_container "fraud-report"
    wait_for_container "fraud-publisher"
    wait_for_container "fraud-frontend"
    wait_for_container "fraud-negative-collector"
    
    print_success "Pipeline nodes started"
}

stop_all() {
    print_step "Stopping all containers..."
    cd "$SCRIPT_DIR"
    docker-compose -f "$COMPOSE_FILE" --profile pretrain --profile init down
    print_success "All containers stopped"
}

run_cleanup() {
    print_step "Cleaning up previous run data..."
    cd "$SCRIPT_DIR"
    
    # Clean checkpoints
    rm -rf ./pathway_persistence/checkpoints_* 2>/dev/null || true
    # Clean temp files
    rm -f ./publisher/temp_*.csv 2>/dev/null || true
    # Clean reports
    rm -f ./fraud_reports/*.pdf ./fraud_reports/*.json 2>/dev/null || true
    # Clean frontend state (delete and recreate as empty files for Docker mount)
    rm -rf ./review_stats.json ./frontend_queue.json ./negative_transactions.json 2>/dev/null || true
    echo '{}' > ./review_stats.json
    echo '[]' > ./frontend_queue.json
    echo '{"count": 0, "transactions": []}' > ./negative_transactions.json
    
    print_success "Cleanup complete"
}

# ═══════════════════════════════════════════════════════════════════════════
# MAIN COMMANDS
# ═══════════════════════════════════════════════════════════════════════════

cmd_start() {
    print_header "STARTING (with pretrain)"
    
    # 0. Build images (fresh, no cache)
    build_images_fresh
    
    # 1. Stop any existing containers
    stop_all 2>/dev/null || true
    
    # 2. Reset Prometheus data (optional - for fresh metrics)
    print_step "Resetting Prometheus data..."
    docker volume rm fraud-detection-pipeline_prometheus-data 2>/dev/null || true
    
    # 3. Run cleanup
    run_cleanup
    
    # 4. Start infrastructure
    start_infrastructure
    
    # 5. Run pretrain
    run_pretrain
    
    # 6. Load Redis stats
    load_redis_stats
    
    # 7. Start pipeline nodes
    start_pipeline_nodes
    
    echo ""
    print_header "PIPELINE STARTED"
    echo -e "${GREEN}All containers are running!${NC}"
    echo ""
    echo "Endpoints:"
    echo "  • Grafana Dashboard: http://localhost:3000 (admin/admin)"
    echo "  • Frontend:          http://localhost:8000"
    echo "  • Prometheus:        http://localhost:9090"
    echo ""
    echo "To view logs: ./pipeline.sh logs"
    echo "To stop:      ./pipeline.sh stop"
}

cmd_restart() {
    print_header "RESTARTING (without pretrain)"
    
    # 1. Stop ALL containers including infrastructure
    print_step "Stopping all containers..."
    cd "$SCRIPT_DIR"
    docker-compose -f "$COMPOSE_FILE" --profile pretrain --profile init down 2>/dev/null || true
    
    # 2. Reset Prometheus and Grafana data for fresh metrics
    print_step "Resetting Prometheus and Grafana data..."
    docker volume rm fraud-detection-pipeline_prometheus-data 2>/dev/null || true
    docker volume rm fraud-detection-pipeline_grafana-data 2>/dev/null || true
    
    # 3. Rebuild images to pick up code changes
    build_images
    
    # 4. Start infrastructure fresh
    start_infrastructure
    
    # 5. Run cleanup
    run_cleanup
    
    # 6. Load Redis stats
    load_redis_stats
    
    # 7. Start pipeline nodes
    start_pipeline_nodes
    
    echo ""
    print_header "PIPELINE RESTARTED"
    echo -e "${GREEN}All containers are running!${NC}"
    echo ""
    echo "Endpoints:"
    echo "  • Grafana Dashboard: http://localhost:3000 (admin/admin)"
    echo "  • Frontend:          http://localhost:8000"
    echo ""
    echo "To view logs: ./pipeline.sh logs"
    echo "To stop:      ./pipeline.sh stop"
}

cmd_stop() {
    print_header "STOPPING"
    stop_all
    
    # Clean up state files
    print_step "Cleaning up state files..."
    rm -f ./review_stats.json ./frontend_queue.json ./negative_transactions.json 2>/dev/null || true
    rm -f ./fraud_reports/*.pdf ./fraud_reports/*.json 2>/dev/null || true
    print_success "State files cleaned"
    
    print_header "PIPELINE STOPPED"
    echo -e "${GREEN}All containers have been stopped.${NC}"
}

cmd_status() {
    print_header "STATUS"
    cd "$SCRIPT_DIR"
    
    echo -e "${YELLOW}Docker Containers:${NC}"
    docker-compose -f "$COMPOSE_FILE" ps
    
    echo ""
    echo -e "${YELLOW}Service Endpoints:${NC}"
    echo "   Grafana:    http://localhost:3000 (admin/admin)"
    echo "   Frontend:   http://localhost:8000"
    echo "   Prometheus: http://localhost:9090"
    echo "   NATS:       localhost:4222"
    echo "   Redis:      localhost:6379"
}

cmd_logs() {
    cd "$SCRIPT_DIR"
    docker-compose -f "$COMPOSE_FILE" logs -f --tail=100
}

# ═══════════════════════════════════════════════════════════════════════════
# ENTRY POINT
# ═══════════════════════════════════════════════════════════════════════════

show_usage() {
    echo "Usage: $0 {start|restart|stop|status|logs}"
    echo ""
    echo "Commands:"
    echo "  start   - Full start with pretrain (builds images, runs pretrain)"
    echo "  restart - Quick restart without pretrain (uses existing model)"
    echo "  stop    - Stop all containers"
    echo "  status  - Show container status"
    echo "  logs    - Tail all container logs"
    echo ""
}

case "${1:-}" in
    start)
        cmd_start
        ;;
    restart)
        cmd_restart
        ;;
    stop)
        cmd_stop
        ;;
    status)
        cmd_status
        ;;
    logs)
        cmd_logs
        ;;
    *)
        show_usage
        exit 1
        ;;
esac
