#!/bin/bash
# Setup script for NEXUS on GCP VM
# Run this once after cloning the repo

# Set environment variables
export NEXUS_RUNTIME_DIR=/data
export NEXUS_FORCE_GCP=true

# Add to bashrc for persistence
echo 'export NEXUS_RUNTIME_DIR=/data' >> ~/.bashrc
echo 'export NEXUS_FORCE_GCP=true' >> ~/.bashrc

# Create directories
mkdir -p /data/datasets
mkdir -p /data/raw
mkdir -p /data/quarantine
mkdir -p /data/dlq
mkdir -p /data/logs
mkdir -p /data/processed

echo "NEXUS environment configured:"
echo "  NEXUS_RUNTIME_DIR=$NEXUS_RUNTIME_DIR"
echo "  NEXUS_FORCE_GCP=$NEXUS_FORCE_GCP"

# Show directory structure
echo ""
echo "Directory structure:"
ls -la /data/

# Quick test
echo ""
echo "Testing config..."
python3 -c "from common.config import DATASETS_DIR, RAW_DIR, RUNTIME_DIR; print(f'RUNTIME_DIR: {RUNTIME_DIR}'); print(f'DATASETS_DIR: {DATASETS_DIR}'); print(f'RAW_DIR: {RAW_DIR}')"
