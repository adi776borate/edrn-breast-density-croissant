"""
LabCAS API Client for EDRN
Based on the existing notebook implementation with enhanced error handling
"""

import os
import requests
import time
from requests.auth import HTTPBasicAuth
from typing import Dict, List, Optional


def get_jwt_token(username: str, password: str, base_url: str = "https://edrn-labcas.jpl.nasa.gov") -> str:
    """
    Authenticate with LabCAS and return a JWT token using POST method.
    """
    url = f"{base_url}/data-access-api/auth"
    resp = requests.post(url, auth=HTTPBasicAuth(username, password))
    resp.raise_for_status()
    return resp.text.strip()


class LabCASClient:
    """
    LabCAS API Client with automatic token refresh and POST fallback
    """
    
    def __init__(self, jwt_token: str, base_url: str = "https://edrn-labcas.jpl.nasa.gov"):
        self.base_url = base_url
        self.jwt_token = jwt_token
        self.username = os.getenv('LABCAS_USERNAME')
        self.password = os.getenv('LABCAS_PASSWORD')
        self.token_timestamp = time.time()
        self.token_max_age = 1800  # Refresh after 30 minutes
        
        self.headers = {
            "Authorization": f"Bearer {jwt_token}"
        }
    
    def refresh_token(self):
        """Refresh JWT token"""
        print("⟳ Refreshing JWT token...")
        self.jwt_token = get_jwt_token(self.username, self.password, self.base_url)
        self.headers["Authorization"] = f"Bearer {self.jwt_token}"
        self.token_timestamp = time.time()
        print("✓ Token refreshed")
    
    def _ensure_valid_token(self):
        """Check if token needs refresh based on age"""
        if time.time() - self.token_timestamp > self.token_max_age:
            self.refresh_token()
    
    def _get(self, path: str, params: dict, use_post: bool = False):
        """
        Make API request with automatic token refresh and POST fallback
        """
        self._ensure_valid_token()
        
        url = f"{self.base_url}{path}"
        
        try:
            if use_post:
                resp = requests.post(url, headers=self.headers, params=params, timeout=60)
            else:
                resp = requests.get(url, headers=self.headers, params=params, timeout=60)
            
            resp.raise_for_status()
            return resp.json()
            
        except requests.exceptions.HTTPError as e:
            if e.response.status_code == 401:
                # Token expired, refresh and retry
                print("⟳ Token expired (401), refreshing...")
                self.refresh_token()
                
                if use_post:
                    resp = requests.post(url, headers=self.headers, params=params, timeout=60)
                else:
                    resp = requests.get(url, headers=self.headers, params=params, timeout=60)
                
                resp.raise_for_status()
                return resp.json()
            else:
                # If GET fails with other error and use_post is False, try POST
                if not use_post:
                    print(f"⚠ GET failed with {e.response.status_code}, trying POST...")
                    return self._get(path, params, use_post=True)
                raise
        
        except Exception as e:
            # For other errors, try POST if not already using it
            if not use_post:
                print(f"⚠ GET failed with {type(e).__name__}, trying POST...")
                return self._get(path, params, use_post=True)
            raise
    
    # ---------- Collections ----------
    
    def list_collections(self, rows=100):
        """List all collections"""
        return self._get(
            "/data-access-api/collections/select",
            {
                "q": "*:*",
                "wt": "json",
                "rows": rows,
                "start": 0
            }
        )["response"]["docs"]
    
    # ---------- Datasets ----------
    
    def list_datasets_for_collection(self, collection_id: str, rows=10000, start=0):
        """List all datasets for a collection"""
        return self._get(
            "/data-access-api/datasets/select",
            {
                "q": f'CollectionId:"{collection_id}"',
                "wt": "json",
                "rows": rows,
                "start": start
            }
        )["response"]["docs"]
    
    # ---------- Files ----------
    
    def list_files_for_dataset(self, dataset_id: str, rows=10000, start=0):
        """List all files for a dataset with pagination support"""
        return self._get(
            "/data-access-api/files/select",
            {
                "q": f'DatasetId:"{dataset_id}"',
                "wt": "json",
                "rows": rows,
                "start": start
            }
        )["response"]["docs"]
    
    def list_all_files_for_dataset(self, dataset_id: str, batch_size=1000) -> List[Dict]:
        """
        List ALL files for a dataset with automatic pagination and incremental results
        """
        all_files = []
        start = 0
        
        while True:
            try:
                result = self._get(
                    "/data-access-api/files/select",
                    {
                        "q": f'DatasetId:"{dataset_id}"',
                        "wt": "json",
                        "rows": batch_size,
                        "start": start
                    }
                )
                
                docs = result["response"]["docs"]
                num_found = result["response"]["numFound"]
                
                if not docs:
                    break
                
                all_files.extend(docs)
                start += len(docs)
                
                print(f"  └─ Retrieved {len(all_files)}/{num_found} files...")
                
                if len(all_files) >= num_found:
                    break
                
                # Small delay to avoid rate limiting
                time.sleep(0.3)
                
            except Exception as e:
                print(f"⚠ Error retrieving files at offset {start}: {e}")
                # Refresh token and retry
                self.refresh_token()
                continue
        
        return all_files
    
    @staticmethod
    def build_download_url(file_id: str, base_url: str = "https://edrn-labcas.jpl.nasa.gov") -> str:
        """Build download URL for a file"""
        return f"{base_url}/data-access-api/download?id={file_id}"
