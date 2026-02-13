#!/bin/bash

# Script to set up Debezium CDC connector for EXTERNAL MSSQL to Kafka
# This connects to your LOCAL MSSQL Server, not a Docker container

set -e

echo "=========================================="
echo "Debezium CDC Setup - External MSSQL"
echo "=========================================="

# Configuration
KAFKA_CONNECT_URL="http://localhost:8083"
CONNECTOR_CONFIG_FILE="./config/kafka/connect-debezium.json"
CONNECTOR_NAME="mssql-fraud-detection-connector"

# Colors for output
GREEN='\033[0;32m'
RED='\033[0;31m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

# Function to check if config file has been updated
check_config_file() {
    echo -e "${YELLOW}Checking connector configuration...${NC}"
    
    if [ ! -f "$CONNECTOR_CONFIG_FILE" ]; then
        echo -e "${RED}✗ Configuration file not found: $CONNECTOR_CONFIG_FILE${NC}"
        return 1
    fi
    
    # Check if password is still placeholder
    if grep -q "YOUR_MSSQL_PASSWORD_HERE" "$CONNECTOR_CONFIG_FILE"; then
        echo -e "${RED}✗ Please update the MSSQL password in: $CONNECTOR_CONFIG_FILE${NC}"
        echo "  Change 'YOUR_MSSQL_PASSWORD_HERE' to your actual MSSQL password"
        return 1
    fi
    
    # Check if hostname is correct
    if grep -q '"database.hostname": "host.docker.internal"' "$CONNECTOR_CONFIG_FILE"; then
        echo -e "${GREEN}✓ Using host.docker.internal to access local MSSQL${NC}"
    fi
    
    echo -e "${GREEN}✓ Configuration file looks valid${NC}"
    return 0
}

# Function to check if Kafka Connect is ready
check_kafka_connect() {
    echo -e "${YELLOW}Checking if Kafka Connect is ready...${NC}"
    
    max_attempts=30
    attempt=0
    
    while [ $attempt -lt $max_attempts ]; do
        if curl -s -o /dev/null -w "%{http_code}" "$KAFKA_CONNECT_URL" | grep -q "200"; then
            echo -e "${GREEN}✓ Kafka Connect is ready!${NC}"
            return 0
        fi
        
        attempt=$((attempt + 1))
        echo "Waiting for Kafka Connect... (Attempt $attempt/$max_attempts)"
        sleep 10
    done
    
    echo -e "${RED}✗ Kafka Connect is not ready after $max_attempts attempts${NC}"
    echo "  Make sure Docker services are running:"
    echo "  docker-compose -f docker-compose-mssql.yml up -d"
    return 1
}

# Function to test MSSQL connectivity from Kafka Connect container
test_mssql_connectivity() {
    echo -e "${YELLOW}Testing MSSQL connectivity from Kafka Connect container...${NC}"
    
    # Try to ping host.docker.internal
    echo "Testing host.docker.internal resolution..."
    docker exec kafka-connect ping -c 2 host.docker.internal 2>/dev/null || {
        echo -e "${RED}✗ Cannot reach host.docker.internal from container${NC}"
        echo "  On Windows/Mac: Make sure Docker Desktop is running"
        echo "  On Linux: Add --add-host=host.docker.internal:host-gateway to docker-compose"
        return 1
    }
    
    echo -e "${GREEN}✓ host.docker.internal is reachable${NC}"
    
    # Try to connect to MSSQL port
    echo "Testing MSSQL port 1433..."
    if docker exec kafka-connect nc -zv host.docker.internal 1433 2>&1 | grep -q "succeeded\|open"; then
        echo -e "${GREEN}✓ MSSQL port 1433 is accessible${NC}"
    else
        echo -e "${RED}✗ Cannot connect to MSSQL port 1433${NC}"
        echo "  Make sure:"
        echo "  1. MSSQL Server is running locally"
        echo "  2. TCP/IP is enabled in SQL Server Configuration Manager"
        echo "  3. Firewall allows port 1433"
        echo "  4. SQL Server is listening on 0.0.0.0:1433 (not just 127.0.0.1)"
        return 1
    fi
    
    return 0
}

# Function to check if connector already exists
check_connector_exists() {
    response=$(curl -s -o /dev/null -w "%{http_code}" "$KAFKA_CONNECT_URL/connectors/$CONNECTOR_NAME")
    
    if [ "$response" = "200" ]; then
        return 0
    else
        return 1
    fi
}

# Function to delete existing connector
delete_connector() {
    echo -e "${YELLOW}Deleting existing connector: $CONNECTOR_NAME${NC}"
    
    response=$(curl -s -o /dev/null -w "%{http_code}" -X DELETE "$KAFKA_CONNECT_URL/connectors/$CONNECTOR_NAME")
    
    if [ "$response" = "204" ] || [ "$response" = "200" ]; then
        echo -e "${GREEN}✓ Connector deleted successfully${NC}"
        sleep 5
    else
        echo -e "${RED}✗ Failed to delete connector (HTTP $response)${NC}"
        return 1
    fi
}

# Function to create connector
create_connector() {
    echo -e "${YELLOW}Creating Debezium connector: $CONNECTOR_NAME${NC}"
    
    # Deploy connector
    response=$(curl -s -w "\n%{http_code}" -X POST \
        -H "Content-Type: application/json" \
        --data @"$CONNECTOR_CONFIG_FILE" \
        "$KAFKA_CONNECT_URL/connectors")
    
    http_code=$(echo "$response" | tail -n1)
    body=$(echo "$response" | sed '$d')
    
    if [ "$http_code" = "201" ] || [ "$http_code" = "200" ]; then
        echo -e "${GREEN}✓ Connector created successfully!${NC}"
        echo "$body" | jq '.' 2>/dev/null || echo "$body"
        return 0
    else
        echo -e "${RED}✗ Failed to create connector (HTTP $http_code)${NC}"
        echo "$body" | jq '.' 2>/dev/null || echo "$body"
        
        # Common error hints
        if echo "$body" | grep -q "authentication"; then
            echo -e "\n${YELLOW}Hint: Check MSSQL username and password in config file${NC}"
        elif echo "$body" | grep -q "connection"; then
            echo -e "\n${YELLOW}Hint: Check MSSQL server connectivity${NC}"
        elif echo "$body" | grep -q "database"; then
            echo -e "\n${YELLOW}Hint: Check database name and if CDC is enabled${NC}"
        fi
        
        return 1
    fi
}

# Function to check connector status
check_connector_status() {
    echo -e "${YELLOW}Checking connector status...${NC}"
    
    response=$(curl -s "$KAFKA_CONNECT_URL/connectors/$CONNECTOR_NAME/status")
    
    echo "$response" | jq '.' 2>/dev/null || echo "$response"
    
    state=$(echo "$response" | jq -r '.connector.state' 2>/dev/null)
    
    if [ "$state" = "RUNNING" ]; then
        echo -e "${GREEN}✓ Connector is RUNNING${NC}"
        return 0
    else
        echo -e "${RED}✗ Connector state: $state${NC}"
        
        # Check task status
        task_state=$(echo "$response" | jq -r '.tasks[0].state' 2>/dev/null)
        if [ "$task_state" = "FAILED" ]; then
            echo -e "${RED}Task failed. Check logs: docker logs kafka-connect${NC}"
        fi
        
        return 1
    fi
}

# Function to list Kafka topics
list_kafka_topics() {
    echo -e "${YELLOW}Listing CDC Kafka topics...${NC}"
    
    docker exec kafka-broker kafka-topics --bootstrap-server localhost:9092 --list 2>/dev/null | grep -E "(cdc\.|fraud-detection-server)" || {
        echo "No CDC topics found yet. They will be created when data changes."
    }
}

# Main execution
main() {
    echo "Starting Debezium CDC setup for external MSSQL..."
    echo ""
    
    # Step 1: Check configuration file
    if ! check_config_file; then
        echo -e "${RED}Setup failed: Configuration not ready${NC}"
        exit 1
    fi
    
    echo ""
    
    # Step 2: Check Kafka Connect
    if ! check_kafka_connect; then
        echo -e "${RED}Setup failed: Kafka Connect is not available${NC}"
        exit 1
    fi
    
    echo ""
    
    # Step 3: Test MSSQL connectivity
    if ! test_mssql_connectivity; then
        echo -e "${RED}Setup failed: Cannot connect to local MSSQL${NC}"
        echo ""
        echo "Troubleshooting steps:"
        echo "1. Check MSSQL Server is running: services.msc (Windows)"
        echo "2. Enable TCP/IP: SQL Server Configuration Manager > Protocols"
        echo "3. Check firewall: Allow port 1433"
        echo "4. Test connection: sqlcmd -S localhost -U sa -P YourPassword"
        exit 1
    fi
    
    echo ""
    
    # Step 4: Check if connector exists
    if check_connector_exists; then
        echo -e "${YELLOW}Connector already exists${NC}"
        read -p "Do you want to delete and recreate it? (y/n) " -n 1 -r
        echo
        
        if [[ $REPLY =~ ^[Yy]$ ]]; then
            if ! delete_connector; then
                echo -e "${RED}Setup failed: Could not delete existing connector${NC}"
                exit 1
            fi
        else
            echo "Keeping existing connector"
            check_connector_status
            exit 0
        fi
    fi
    
    echo ""
    
    # Step 5: Create connector
    if ! create_connector; then
        echo -e "${RED}Setup failed: Could not create connector${NC}"
        echo ""
        echo "Check connector logs:"
        echo "  docker logs kafka-connect | tail -50"
        exit 1
    fi
    
    echo ""
    
    # Step 6: Wait for connector to initialize
    echo "Waiting for connector to initialize..."
    sleep 10
    
    # Step 7: Check connector status
    if ! check_connector_status; then
        echo -e "${YELLOW}Warning: Connector may not be running properly${NC}"
        echo "Check logs: docker logs kafka-connect"
    fi
    
    echo ""
    
    # Step 8: List Kafka topics
    list_kafka_topics
    
    echo ""
    echo -e "${GREEN}=========================================="
    echo "Debezium CDC Setup Complete!"
    echo -e "==========================================${NC}"
    echo ""
    echo "Your LOCAL MSSQL Server is now streaming to Kafka!"
    echo ""
    echo "Expected Kafka topics (created on first data change):"
    echo "  - cdc.Transaction"
    echo ""
    echo "Monitor connector:"
    echo "  curl http://localhost:8083/connectors/$CONNECTOR_NAME/status | jq"
    echo ""
    echo "View connector logs:"
    echo "  docker logs -f kafka-connect"
    echo ""
    echo "Test CDC consumer:"
    echo "  python scripts/testing/test_cdc_consumer.py"
    echo ""
    echo "Insert data into your local MSSQL to see CDC in action!"
}

# Run main function
main