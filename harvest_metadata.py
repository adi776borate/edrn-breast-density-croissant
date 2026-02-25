"""
CLI Script to Harvest LabCAS Metadata
"""

import os
import sys
from pathlib import Path

# Import from local files
from labcas_client import get_jwt_token, LabCASClient
from harvester import LabCASHarvester


def main():
    # Configuration
    TARGET_COLLECTION_ID = "Automated_Quantitative_Measures_of_Breast_Density_Data"
    OUTPUT_DIR = Path(__file__).parent / "harvested_metadata"
    
    # Get credentials from environment
    username = os.getenv('LABCAS_USERNAME')
    password = os.getenv('LABCAS_PASSWORD')
    
    if not username or not password:
        print("Error: LABCAS_USERNAME and LABCAS_PASSWORD environment variables must be set")
        sys.exit(1)
    
    print(f"Using credentials for user: {username}")
    
    # Authenticate
    print("\n Authenticating with LabCAS...")
    jwt_token = get_jwt_token(username, password)
    print("✓ Authentication successful")
    
    # Create client and harvester
    client = LabCASClient(jwt_token)
    
    harvester = LabCASHarvester(client, OUTPUT_DIR)
    
    # Run harvest
    result = harvester.harvest_all(TARGET_COLLECTION_ID)
    
    print("\n Metadata harvesting complete!")
    print(f"\nNext step: Run 'python generate_croissant.py' to create Croissant metadata")


if __name__ == "__main__":
    main()
