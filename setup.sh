#!/bin/bash
# PI-1 Platform Initialization Script

set -e

echo "=========================================="
echo "ManOxCo PI-1 Platform Setup"
echo "=========================================="

# Create directories
echo "[1/5] Creating directory structure..."
mkdir -p src/{ingestion,medallion,analytics,ai,governance}
mkdir -p tests/{unit,integration}
mkdir -p docs/{architecture,governance,operations}
mkdir -p config
mkdir -p data/{raw,bronze,silver,gold}
mkdir -p notebooks/{01-exploration,02-ingestion,03-medallion,04-analytics,05-ai}

# Create Python virtual environment if not exists
echo "[2/5] Setting up Python environment..."
if [ ! -d "venv" ]; then
    python3 -m venv venv
fi
source venv/bin/activate

# Install dependencies
echo "[3/5] Installing dependencies..."
pip install --upgrade pip setuptools wheel
pip install -e ".[dev,docker,ai]"

# Create sample environment file
echo "[4/5] Creating configuration templates..."
cat > config/.env.example << 'EOF'
# Spark Configuration
SPARK_MASTER_HOST=spark-master
SPARK_MASTER_PORT=7077
SPARK_WORKER_CORES=2
SPARK_WORKER_MEMORY=2G

# Data Lake Configuration
BRONZE_PATH=/workspace/data/bronze
SILVER_PATH=/workspace/data/silver
GOLD_PATH=/workspace/data/gold

# API Configuration
API_HOST=0.0.0.0
API_PORT=8000

# AI Agent Configuration
OPENAI_API_KEY=your_key_here
LLM_MODEL=gpt-4
EOF

echo "[5/5] Initialization complete!"
echo ""
echo "Next steps:"
echo "1. Copy config/.env.example to config/.env and add your credentials"
echo "2. Run: docker-compose up -d"
echo "3. Access Jupyter at http://localhost:8888"
echo "4. Access Spark Master UI at http://localhost:8080"
echo ""
