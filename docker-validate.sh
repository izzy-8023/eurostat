#!/bin/bash
# Docker Configuration Validation Script
# Helps validate and choose the right Docker Compose setup

echo "🔍 Eurostat Docker Configuration Validator"
echo "=========================================="

# Check Docker and Docker Compose
echo "📋 Checking Docker installation..."
if ! command -v docker &> /dev/null; then
    echo "❌ Docker is not installed or not in PATH"
    exit 1
fi

if ! docker compose version &> /dev/null; then
    echo "❌ Docker Compose is not available"
    exit 1
fi

echo "✅ Docker and Docker Compose are available"

# Check existing networks
echo ""
echo "🌐 Checking existing Docker networks..."
if docker network ls | grep -q "eurostat_shared_network"; then
    echo "✅ eurostat_shared_network exists"
else
    echo "⚠️  eurostat_shared_network does not exist - will be created"
fi

# Check running containers
echo ""
echo "🐳 Checking running containers..."
RUNNING_CONTAINERS=$(docker ps --format "table {{.Names}}\t{{.Status}}" | grep -E "(eurostat|airflow)" || true)
if [ -n "$RUNNING_CONTAINERS" ]; then
    echo "⚠️  Found running Eurostat/Airflow containers:"
    echo "$RUNNING_CONTAINERS"
    echo ""
    echo "💡 Consider stopping them first: docker compose down"
else
    echo "✅ No conflicting containers running"
fi

# Validate compose files
echo ""
echo "📝 Validating Docker Compose files..."

COMPOSE_FILES=(
    "docker-compose.yml:Basic Eurostat pipeline with PostgreSQL and pgAdmin"
    "docker-compose-airflow.yaml:Full Airflow setup with CeleryExecutor (resource intensive)"
    "docker-compose-fast.yaml:Fast Airflow setup with base image (minimal build time)"
    "docker-compose-simple.yaml:Simplified Airflow with LocalExecutor (recommended for development)"
)

for file_desc in "${COMPOSE_FILES[@]}"; do
    IFS=':' read -r file desc <<< "$file_desc"
    echo ""
    echo "🔍 Validating $file..."
    if docker compose -f "$file" config > /dev/null 2>&1; then
        echo "✅ $file is valid"
        echo "   📄 $desc"
    else
        echo "❌ $file has configuration errors:"
        docker compose -f "$file" config 2>&1 | head -5
    fi
done

# Recommendations
echo ""
echo "🎯 Recommendations:"
echo "=================="
echo ""
echo "For development/testing:"
echo "  docker compose -f docker-compose-simple.yaml up -d"
echo ""
echo "For basic data processing (no Airflow):"
echo "  docker compose -f docker-compose.yml up -d"
echo ""
echo "For fast Airflow setup (no custom build):"
echo "  docker compose -f docker-compose-fast.yaml up -d"
echo ""
echo "For full production-like Airflow:"
echo "  docker compose -f docker-compose-airflow.yaml up -d"
echo ""
echo "🔧 To clean up everything:"
echo "  docker compose down --volumes --remove-orphans"
echo "  docker network prune -f"
echo ""
echo "📊 To check logs:"
echo "  docker compose logs -f [service-name]" 