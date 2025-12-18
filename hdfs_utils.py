import subprocess
import os
from datetime import datetime


class HDFSManager:
    def __init__(self, namenode: str = "localhost:9000"):
        self.namenode = namenode
        self.hdfs_base = f"hdfs://{namenode}"
    
    def _run_hdfs_cmd(self, cmd: list) -> tuple:
        try:
            result = subprocess.run(
                ["hdfs", "dfs"] + cmd,
                capture_output=True,
                text=True
            )
            return result.returncode == 0, result.stdout, result.stderr
        except Exception as e:
            return False, "", str(e)
    
    def create_directory(self, path: str) -> bool:
        success, _, err = self._run_hdfs_cmd(["-mkdir", "-p", path])
        if success:
            print(f"Created directory: {path}")
        else:
            print(f"Error creating {path}: {err}")
        return success
    
    def upload_file(self, local_path: str, hdfs_path: str) -> bool:
        success, _, err = self._run_hdfs_cmd(["-put", "-f", local_path, hdfs_path])
        if success:
            print(f"Uploaded: {local_path} -> {hdfs_path}")
        else:
            print(f"Error uploading: {err}")
        return success
    
    def list_directory(self, path: str) -> list:
        success, out, _ = self._run_hdfs_cmd(["-ls", path])
        if success:
            return out.strip().split("\n")
        return []
    
    def get_file_count(self, path: str) -> int:
        success, out, _ = self._run_hdfs_cmd(["-count", path])
        if success:
            parts = out.strip().split()
            return int(parts[1]) if len(parts) > 1 else 0
        return 0
    
    def setup_news_directories(self):
        directories = [
            "/user/news",
            "/user/news/training",
            "/user/news/training/fake",
            "/user/news/training/true",
            "/user/news/streaming",
            "/user/news/streaming/parquet",
            "/user/news/streaming/csv",
            "/user/news/predictions",
            "/user/news/checkpoints"
        ]
        
        for dir_path in directories:
            self.create_directory(dir_path)
        
        print("\nHDFS directory structure created!")
        return True
    
    def upload_training_data(self, fake_csv_path: str, true_csv_path: str):
        # Upload Fake.csv
        if os.path.exists(fake_csv_path):
            self.upload_file(fake_csv_path, "/user/news/training/fake/Fake.csv")
        else:
            print(f"File not found: {fake_csv_path}")
        
        # Upload True.csv
        if os.path.exists(true_csv_path):
            self.upload_file(true_csv_path, "/user/news/training/true/True.csv")
        else:
            print(f"File not found: {true_csv_path}")
    
    def check_status(self):
        print("\n" + "=" * 50)
        print("HDFS NEWS DATA STATUS")
        print("=" * 50)
        
        paths = {
            "Training - Fake": "/user/news/training/fake",
            "Training - True": "/user/news/training/true",
            "Streaming - Parquet": "/user/news/streaming/parquet",
            "Streaming - CSV": "/user/news/streaming/csv",
            "Predictions": "/user/news/predictions"
        }
        
        for name, path in paths.items():
            files = self.list_directory(path)
            count = len([f for f in files if f and not f.startswith("Found")])
            print(f"{name}: {count} files")
        
        print("=" * 50)


# =============================================================================
# Bash scripts for HDFS setup
# =============================================================================

HDFS_SETUP_SCRIPT = """#!/bin/bash
# HDFS Setup Script for News Streaming Project

echo "Setting up HDFS directories for news data..."

# Create directory structure
hdfs dfs -mkdir -p /user/news/training/fake
hdfs dfs -mkdir -p /user/news/training/true
hdfs dfs -mkdir -p /user/news/streaming/parquet
hdfs dfs -mkdir -p /user/news/streaming/csv
hdfs dfs -mkdir -p /user/news/predictions
hdfs dfs -mkdir -p /user/news/checkpoints

# Set permissions
hdfs dfs -chmod -R 755 /user/news

# Upload training data (adjust paths as needed)
if [ -f "./Fake.csv" ]; then
    hdfs dfs -put -f ./Fake.csv /user/news/training/fake/
    echo "Uploaded Fake.csv"
fi

if [ -f "./True.csv" ]; then
    hdfs dfs -put -f ./True.csv /user/news/training/true/
    echo "Uploaded True.csv"
fi

echo "HDFS setup complete!"
hdfs dfs -ls -R /user/news
"""

HDFS_MONITOR_SCRIPT = """#!/bin/bash
# Monitor HDFS news data

echo "========================================"
echo "HDFS News Data Monitor"
echo "========================================"
echo ""

echo "Training Data:"
echo "--------------"
echo "Fake news files:"
hdfs dfs -ls /user/news/training/fake 2>/dev/null || echo "  No files yet"
echo ""
echo "True news files:"
hdfs dfs -ls /user/news/training/true 2>/dev/null || echo "  No files yet"
echo ""

echo "Streaming Data:"
echo "---------------"
echo "Recent Parquet partitions:"
hdfs dfs -ls /user/news/streaming/parquet 2>/dev/null | tail -10 || echo "  No data yet"
echo ""
echo "Recent CSV files:"
hdfs dfs -ls /user/news/streaming/csv 2>/dev/null | tail -10 || echo "  No data yet"
echo ""

echo "Data Counts:"
echo "------------"
hdfs dfs -count /user/news/training 2>/dev/null || echo "Training: N/A"
hdfs dfs -count /user/news/streaming 2>/dev/null || echo "Streaming: N/A"
"""


def create_bash_scripts():
    """Create helper bash scripts."""
    
    with open("setup_hdfs.sh", "w") as f:
        f.write(HDFS_SETUP_SCRIPT)
    os.chmod("setup_hdfs.sh", 0o755)
    
    with open("monitor_hdfs.sh", "w") as f:
        f.write(HDFS_MONITOR_SCRIPT)
    os.chmod("monitor_hdfs.sh", 0o755)
    
    print("Created: setup_hdfs.sh, monitor_hdfs.sh")


if __name__ == "__main__":
    # Create bash scripts
    create_bash_scripts()
    
    # Initialize HDFS manager
    hdfs = HDFSManager()
    
    # Setup directories
    hdfs.setup_news_directories()
    
    # Check status
    hdfs.check_status()
