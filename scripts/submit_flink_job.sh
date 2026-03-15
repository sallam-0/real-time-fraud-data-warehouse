#!/bin/bash

# Flink Submit Script for Transaction CDC Enrichment Job
# This script installs dependencies, downloads connector JARs, and submits the Flink job

set -e


echo "==========================================="
echo "Transaction CDC Enrichment — Flink + Redis"
echo "==========================================="

# Configuration (host-side paths only)
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
JARS_DIR="${SCRIPT_DIR}/jars"

# Container-side paths (hardcoded to avoid shell expansion issues with MSYS)
CONTAINER_APP_DIR="/opt/flink-apps"
CONTAINER_LIB_DIR="/opt/flink/lib"

# Create jars directory if it doesn't exist
mkdir -p "${JARS_DIR}"

# Colors
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
RED='\033[0;31m'
NC='\033[0m'

# Function to download JAR if not exists
download_jar() {
    local url=$1
    local filename=$2
    local filepath="${JARS_DIR}/${filename}"

    if [ -f "${filepath}" ]; then
        echo -e "${GREEN}✓ ${filename} already exists${NC}"
    else
        echo -e "${YELLOW}Downloading ${filename}...${NC}"
        curl -L -o "${filepath}" "${url}"
        echo -e "${GREEN}✓ Downloaded ${filename}${NC}"
    fi
}

echo ""
echo "Step 1: Downloading required Flink connector JARs..."
echo ""

# Flink Kafka SQL Connector (bundles kafka-clients)
download_jar \
    "https://repo1.maven.org/maven2/org/apache/flink/flink-sql-connector-kafka/3.1.0-1.18/flink-sql-connector-kafka-3.1.0-1.18.jar" \
    "flink-sql-connector-kafka-3.1.0-1.18.jar"

echo ""
echo -e "${GREEN}✓ All JAR files ready${NC}"
echo ""

echo "Step 2: Copying JARs into Flink containers..."
# Ensure the destination directory exists inside the container before copying
docker exec flink-jobmanager mkdir -p ${CONTAINER_LIB_DIR}
docker exec flink-taskmanager mkdir -p ${CONTAINER_LIB_DIR}

for jar in "${JARS_DIR}"/*.jar; do
    jar_name=$(basename "$jar")
    # Workaround for Docker Windows `docker cp` IO pipe bugs:
    # Stream the file via stdin directly into the container
    cat "${jar}" | docker exec -i flink-jobmanager sh -c "cat > ${CONTAINER_LIB_DIR}/${jar_name}"
    cat "${jar}" | docker exec -i flink-taskmanager sh -c "cat > ${CONTAINER_LIB_DIR}/${jar_name}"
    echo -e "${GREEN}  ✓ Copied ${jar_name}${NC}"
done

echo ""
echo "Step 3: Python deps already baked into Docker image (flink.Dockerfile)"
echo -e "${GREEN}  ✓ flink-jobmanager ready${NC}"
echo -e "${GREEN}  ✓ flink-taskmanager ready${NC}"

echo ""
echo "Step 4: Pre-loading Redis cache with dimension data..."
MSYS_NO_PATHCONV=1 docker exec flink-jobmanager python3 /opt/flink-apps/jobs/redis_cache_loader.py \
    --config /opt/flink-apps/config.ini
if [ $? -ne 0 ]; then
    echo -e "${RED}  ✗ Redis cache loading FAILED — enrichment will be empty without cache data${NC}"
    echo -e "${RED}    Make sure the Flink Docker image was rebuilt with pyodbc + ODBC Driver 17${NC}"
    echo -e "${RED}    Run: docker compose -f docker/docker-compose-flink.yml build --no-cache${NC}"
    exit 1
fi
echo -e "${GREEN}  ✓ Redis cache loaded successfully${NC}"

echo ""
echo "Step 5: Training fraud detection model (if not already trained)..."
MODEL_PATH="/opt/ml/models/fraud_model.joblib"
MSYS_NO_PATHCONV=1 docker exec flink-jobmanager bash -c "if [ ! -f ${MODEL_PATH} ]; then echo 'No existing model found — training now...'; python3 /opt/ml/train_fraud_model.py --mode unsupervised --config /opt/flink-apps/config.ini --output ${MODEL_PATH}; else echo 'Model already exists at ${MODEL_PATH} — skipping training.'; fi"
if [ $? -ne 0 ]; then
    echo -e "${RED}  ✗ Model training FAILED${NC}"
    echo -e "${RED}    Check logs above for details${NC}"
    exit 1
fi
echo -e "${GREEN}  ✓ Model ready${NC}"

echo ""
echo "Step 6: Submitting Flink job..."
echo ""

MSYS_NO_PATHCONV=1 docker exec -e PYFLINK_CLIENT_EXECUTABLE=python3 flink-jobmanager flink run \
    --python /opt/flink-apps/jobs/transaction_cdc_enrichment.py \
    --jarfile /opt/flink/lib/flink-sql-connector-kafka-3.1.0-1.18.jar \
    -d

echo ""
echo -e "${GREEN}==========================================="
echo "Flink job submitted successfully!"
echo "  Web UI:  http://localhost:8081"
echo "  Output:  enriched.Transaction (Kafka topic)"
echo -e "===========================================${NC}"
