#!/usr/bin/env python3
"""
Backup ParcelPy Database

Simple script to create backups of the parcelpy database.
"""

import subprocess
import sys
from pathlib import Path
from datetime import datetime
import argparse

def create_backup(output_dir: Path = Path("backups"), compress: bool = True):
    """Create a backup of the parcelpy database."""
    
    # Create backup directory
    output_dir.mkdir(exist_ok=True)
    
    # Generate filename with timestamp
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    
    if compress:
        backup_file = output_dir / f"parcelpy_backup_{timestamp}.sql.gz"
        cmd = [
            "pg_dump", 
            "-d", "parcelpy",
            "-U", "parcelpy",
            "-h", "localhost",
            "--verbose",
            "--no-password"
        ]
        
        print(f"Creating compressed backup: {backup_file}")
        
        # Run pg_dump and compress
        with open(backup_file, 'wb') as f:
            dump_process = subprocess.Popen(cmd, stdout=subprocess.PIPE)
            gzip_process = subprocess.Popen(['gzip'], stdin=dump_process.stdout, stdout=f)
            dump_process.stdout.close()
            gzip_process.communicate()
            
        if gzip_process.returncode == 0:
            print(f"✅ Backup created successfully: {backup_file}")
            print(f"📁 Size: {backup_file.stat().st_size / (1024*1024):.1f} MB")
        else:
            print("❌ Backup failed")
            return False
            
    else:
        backup_file = output_dir / f"parcelpy_backup_{timestamp}.sql"
        cmd = [
            "pg_dump", 
            "-d", "parcelpy",
            "-U", "parcelpy", 
            "-h", "localhost",
            "-f", str(backup_file),
            "--verbose",
            "--no-password"
        ]
        
        print(f"Creating backup: {backup_file}")
        result = subprocess.run(cmd, capture_output=True, text=True)
        
        if result.returncode == 0:
            print(f"✅ Backup created successfully: {backup_file}")
            print(f"📁 Size: {backup_file.stat().st_size / (1024*1024):.1f} MB")
        else:
            print(f"❌ Backup failed: {result.stderr}")
            return False
    
    return True

def main():
    parser = argparse.ArgumentParser(description="Backup the parcelpy database")
    parser.add_argument("--output-dir", type=Path, default=Path("backups"),
                       help="Directory to store backups (default: backups)")
    parser.add_argument("--no-compress", action="store_true",
                       help="Don't compress the backup file")
    
    args = parser.parse_args()
    
    success = create_backup(args.output_dir, not args.no_compress)
    sys.exit(0 if success else 1)

if __name__ == "__main__":
    main() 