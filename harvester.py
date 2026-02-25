"""
Metadata Harvester for LabCAS with incremental persistence
"""

import json
import time
from pathlib import Path
from typing import Dict, List, Optional
from labcas_client import LabCASClient


class LabCASHarvester:
    """
    Harvest metadata from LabCAS with incremental on-disk persistence
    """
    
    def __init__(self, client: LabCASClient, output_dir: Path):
        self.client = client
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(parents=True, exist_ok=True)
    
    def _save_json(self, data: dict, filename: str):
        """Save data to JSON file"""
        filepath = self.output_dir / filename
        with open(filepath, 'w') as f:
            json.dump(data, f, indent=2)
        print(f"✓ Saved: {filepath}")
    
    def _load_json(self, filename: str) -> Optional[dict]:
        """Load data from JSON file if exists"""
        filepath = self.output_dir / filename
        if filepath.exists():
            with open(filepath, 'r') as f:
                return json.load(f)
        return None
    
    def harvest_collection(self, collection_id: str) -> Dict:
        """
        Harvest collection metadata
        """
        print(f"\n{'='*60}")
        print(f"STEP 1: Harvesting Collection Metadata")
        print(f"{'='*60}")
        
        # Check if already harvested
        existing = self._load_json("collection.json")
        if existing and existing.get("id") == collection_id:
            print(f"✓ Collection metadata already harvested")
            return existing
        
        # Get all collections and find target
        collections = self.client.list_collections(rows=100)
        collection_doc = next(
            (c for c in collections if c.get("id") == collection_id),
            None
        )
        
        if not collection_doc:
            raise ValueError(f"Collection '{collection_id}' not found")
        
        print(f"✓ Found collection: {collection_doc.get('CollectionName')}")
        
        # Save immediately
        self._save_json(collection_doc, "collection.json")
        
        return collection_doc
    
    def harvest_datasets(self, collection_id: str) -> List[Dict]:
        """
        Harvest all datasets for a collection
        """
        print(f"\n{'='*60}")
        print(f"STEP 2: Harvesting Dataset Metadata")
        print(f"{'='*60}")
        
        # Check if already harvested
        existing = self._load_json("datasets.json")
        if existing:
            print(f"✓ Datasets already harvested ({len(existing)} datasets)")
            return existing
        
        datasets = self.client.list_datasets_for_collection(collection_id, rows=10000)
        print(f"✓ Retrieved {len(datasets)} datasets")
        
        # Save immediately
        self._save_json(datasets, "datasets.json")
        
        return datasets
    
    def analyze_datasets(self, datasets: List[Dict]) -> Dict:
        """
        Analyze dataset hierarchy and identify leaf datasets
        """
        print(f"\n{'='*60}")
        print(f"STEP 3: Analyzing Dataset Hierarchy")
        print(f"{'='*60}")
        
        # Check if already analyzed
        existing = self._load_json("leaf_datasets.json")
        if existing:
            print(f"✓ Leaf datasets already identified ({len(existing)} leaf datasets)")
            return existing
        
        def get_dataset_id(d):
            if isinstance(d.get("id"), str) and d["id"].strip():
                return d["id"].strip()
            dv = d.get("DatasetId")
            if isinstance(dv, (list, tuple)) and dv:
                return str(dv[0]).strip()
            return None
        
        def get_parent_id(d):
            for key in ("ParentDatasetId", "DatasetParentId", "DatasetParent"):
                val = d.get(key)
                if isinstance(val, (list, tuple)) and val:
                    return str(val[0]).strip()
                if isinstance(val, str) and val.strip():
                    return val.strip()
            return None
        
        def extract_names(val):
            if val is None:
                return []
            if isinstance(val, (list, tuple, set)):
                return [str(x).strip() for x in val if str(x).strip()]
            s = str(val).strip()
            return [s] if s else []
        
        # Build parent -> children mapping
        children = {}
        for d in datasets:
            parent = get_parent_id(d)
            if not parent:
                continue
            children.setdefault(parent, []).append(get_dataset_id(d))
        
        # Identify leaf datasets (no children)
        leaf_datasets = [
            d for d in datasets
            if get_dataset_id(d) not in children
        ]
        
        # Classify leaf datasets
        targets = {"RAW": 0, "PROC": 0, "MASK": 0, "Documentation": 0}
        
        for d in leaf_datasets:
            name_vals = d.get("DatasetName") or d.get("name") or d.get("labcasName")
            for nm in extract_names(name_vals):
                if nm in targets:
                    targets[nm] += 1
        
        print(f"Total datasets: {len(datasets)}")
        print(f"Leaf datasets: {len(leaf_datasets)}")
        print(f"Non-leaf datasets: {len(datasets) - len(leaf_datasets)}")
        print(f"  RAW: {targets['RAW']}")
        print(f"  PROC: {targets['PROC']}")
        print(f"  MASK: {targets['MASK']}")
        print(f"  Documentation: {targets['Documentation']}")
        
        # Save immediately
        self._save_json(leaf_datasets, "leaf_datasets.json")
        
        return leaf_datasets
    
    def harvest_files(self, leaf_datasets: List[Dict]) -> Dict:
        """
        Harvest file metadata for all leaf datasets with incremental persistence
        """
        print(f"\n{'='*60}")
        print(f"STEP 4: Harvesting File Metadata")
        print(f"{'='*60}")
        
        # Load existing progress
        resources_file = self.output_dir / "resources_by_dataset.json"
        if resources_file.exists():
            with open(resources_file, 'r') as f:
                resources_by_dataset = json.load(f)
            print(f"✓ Resuming from existing harvest ({len(resources_by_dataset)} datasets completed)")
        else:
            resources_by_dataset = {}
        
        def get_dataset_id(d):
            if isinstance(d.get("id"), str) and d["id"].strip():
                return d["id"].strip()
            dv = d.get("DatasetId")
            if isinstance(dv, (list, tuple)) and dv:
                return str(dv[0]).strip()
            return None
        
        total = len(leaf_datasets)
        completed = len(resources_by_dataset)
        
        for idx, d in enumerate(leaf_datasets, 1):
            did = get_dataset_id(d)
            if not did:
                continue
            
            # Skip if already harvested
            if did in resources_by_dataset:
                continue
            
            print(f"\n[{idx}/{total}] Harvesting files for dataset: {did}")
            
            try:
                # Get all files for this dataset
                files = self.client.list_all_files_for_dataset(did, batch_size=1000)
                
                file_entries = []
                for f in files:
                    fid = f.get("id")
                    if not fid:
                        continue
                    
                    file_entries.append({
                        "file_id": fid,
                        "name": f.get("name"),
                        "file_type": f.get("FileType"),
                        "file_size": f.get("FileSize"),
                        "dataset_id": did,
                        "download_url": self.client.build_download_url(fid),
                        "metadata": f  # Store full metadata
                    })
                
                resources_by_dataset[did] = {
                    "dataset_metadata": d,
                    "files": file_entries,
                    "file_count": len(file_entries)
                }
                
                print(f"  ✓ Found {len(file_entries)} files")
                
                # **INCREMENTAL SAVE AFTER EACH DATASET**
                with open(resources_file, 'w') as f:
                    json.dump(resources_by_dataset, f, indent=2)
                
                completed += 1
                print(f"  ✓ Progress saved ({completed}/{total} datasets)")
                
                # Small delay to avoid rate limiting
                # time.sleep(0.5)
                
            except Exception as e:
                print(f"  ⚠ Error harvesting dataset {did}: {e}")
                print(f"  Continuing with next dataset...")
                continue
        
        print(f"\n✓ File harvesting complete: {len(resources_by_dataset)} datasets")
        
        return resources_by_dataset
    
    def harvest_all(self, collection_id: str) -> Dict:
        """
        Run full harvest pipeline
        """
        print(f"\n{'#'*60}")
        print(f"# LabCAS Metadata Harvesting")
        print(f"# Collection: {collection_id}")
        print(f"{'#'*60}")
        
        # Step 1: Collection
        collection_doc = self.harvest_collection(collection_id)
        
        # Step 2: Datasets
        datasets = self.harvest_datasets(collection_id)
        
        # Step 3: Analyze hierarchy
        leaf_datasets = self.analyze_datasets(datasets)
        
        # Step 4: Files (with incremental persistence)
        resources_by_dataset = self.harvest_files(leaf_datasets)
        
        print(f"\n{'='*60}")
        print(f"✓ HARVEST COMPLETE")
        print(f"{'='*60}")
        print(f"Collection: {collection_doc.get('CollectionName')}")
        print(f"Total datasets: {len(datasets)}")
        print(f"Leaf datasets: {len(leaf_datasets)}")
        print(f"Datasets with files: {len(resources_by_dataset)}")
        print(f"\nAll metadata saved to: {self.output_dir}")
        
        return {
            "collection": collection_doc,
            "datasets": datasets,
            "leaf_datasets": leaf_datasets,
            "resources_by_dataset": resources_by_dataset
        }
