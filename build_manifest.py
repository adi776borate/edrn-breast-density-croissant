#!/usr/bin/env python3
"""
Process harvested metadata (resources_by_dataset.json) to produce a STRICT CSV manifest.
Logic:
  1. Only allow views: LCC, LMLO, RCC, RMLO.
  2. STRICTLY REJECT any file with a numeric suffix (e.g. _2.dcm).
     Only "clean" filenames (e.g. ..._LCC.dcm) are allowed.

Output:
  - manifest.csv  (columns: group, patient_id, view, proc_url, mask_url, proc_name, mask_name)
  - manifest_diagnostics.json
"""

import csv
import json
import re
import argparse
from pathlib import Path
from collections import defaultdict

# Regex: capture patient (C or N + 3-4 digits), view made of letters (no digits),
# optional numeric suffix like _2, _3, etc, before ".dcm"
FILENAME_RE = re.compile(
    r"(?P<patient>[CN]\d{3,4}).*?_(?P<view>[A-Z]+)(?:_(?P<suffix>\d+))?\.dcm$",
    re.IGNORECASE,
)

ALLOWED_VIEWS = {"LCC", "LMLO", "RCC", "RMLO"}

INPUT_FILE = Path("harvested_metadata/resources_by_dataset.json")
DEFAULT_OUTPUT = Path("manifest.csv")
DIAG_OUTPUT = Path("manifest_diagnostics.json")

BASE_URL = "https://edrn-labcas.jpl.nasa.gov/data-access-api/download?id="


def parse_args():
    p = argparse.ArgumentParser(description="Clean manifest selecting preferred files per patient+view")
    p.add_argument("--input", "-i", type=Path, default=INPUT_FILE, help="Input harvested metadata JSON")
    p.add_argument("--output", "-o", type=Path, default=DEFAULT_OUTPUT, help="Output CSV manifest")
    p.add_argument("--diag", type=Path, default=DIAG_OUTPUT, help="Diagnostics JSON file")
    return p.parse_args()


def extract_from_path(path_str):
    m = FILENAME_RE.search(path_str.split("/")[-1])
    if not m:
        return None
    patient = m.group("patient")
    view = m.group("view").upper()
    suffix = m.group("suffix")
    suffix_int = int(suffix) if suffix is not None else None
    return patient, view, suffix_int


def get_dataset_type(meta, dataset_id):
    """Determine if dataset is PROC or MASK based on metadata or ID"""
    val = meta.get("DatasetName", "")
    if isinstance(val, list):
        name = val[0] if val else ""
    else:
        name = str(val)
    
    if name in ["PROC", "MASK"]:
        return name
        
    # Fallback to ID check
    if "/PROC" in dataset_id:
        return "PROC"
    if "/MASK" in dataset_id:
        return "MASK"
    return None


def select_best_candidate(candidates):
    """
    candidates: list of dict entries (file metadata)
    selection policy:
      - Only pick candidate with suffix None (no numeric suffix like _2).
      - If multiple exist (unlikely given naming convention), pick first or sort by file_id.
      - If none exist, return None.
    """
    clean_candidates = []
    
    for c in candidates:
        proc = c.get("file_id") or ""
        parsed = extract_from_path(proc)
        if parsed is None:
            continue
            
        patient, view, suffix = parsed
        
        # STRICT: Reject if suffix exists
        if suffix is not None:
            continue
            
        clean_candidates.append({
            "row": c, 
            "file_id": proc
        })

    if not clean_candidates:
        return None, {"reason": "no_clean_file_found"}

    # Sort to ensure deterministic output
    clean_candidates.sort(key=lambda x: x["file_id"])
    
    # Pick the best one (first one)
    chosen = clean_candidates[0]
    return chosen["row"], {"reason": "clean_file_selected"}


def _get_name(file_entry):
    """Extract the file name string from a file metadata entry."""
    raw = file_entry.get("name", "")
    if isinstance(raw, list):
        return raw[0] if raw else ""
    return str(raw)


def main():
    args = parse_args()
    if not args.input.exists():
        raise SystemExit(f"Input file not found: {args.input}")

    print(f"Loading {args.input}...")
    data = json.loads(args.input.read_text())
    
    # Groups: (Patient, View) -> {'proc': [], 'mask': []}
    groups = defaultdict(lambda: {'proc': [], 'mask': []})
    
    total_files = 0
    skipped_files = 0
    skipped_views = 0

    print(f"Processing {len(data)} datasets...")
    
    for dataset_id, payload in data.items():
        meta = payload.get("dataset_metadata", {})
        ds_type = get_dataset_type(meta, dataset_id)
        
        if not ds_type:
            continue
            
        files = payload.get("files", [])
        for f in files:
            total_files += 1
            file_id = f.get("file_id")
            if not file_id:
                continue
                
            parsed = extract_from_path(file_id)
            if not parsed:
                skipped_files += 1
                continue
                
            patient, view, suffix = parsed
            
            # STRICT VIEW FILTER
            if view not in ALLOWED_VIEWS:
                skipped_views += 1
                continue
            
            key = (patient.upper(), view.upper())
            
            if ds_type == "PROC":
                groups[key]['proc'].append(f)
            elif ds_type == "MASK":
                groups[key]['mask'].append(f)

    print(f"Total files scanned: {total_files}")
    print(f"Skipped (Unparseable name): {skipped_files}")
    print(f"Skipped (Disallowed view): {skipped_views}")
    print(f"Unique Patient/View groups found: {len(groups)}")

    rows = []
    diagnostics = {
        "total_groups": len(groups),
        "decisions": {},
        "rejected_groups": [],
        "half_pairs": []
    }

    for (patient, view), candidates in sorted(groups.items()):
        key_str = f"{patient}_{view}"
        
        proc_list = candidates['proc']
        best_proc, proc_info = (None, None)
        if proc_list:
            best_proc, proc_info = select_best_candidate(proc_list)

        mask_list = candidates['mask']
        best_mask, mask_info = (None, None)
        if mask_list:
            best_mask, mask_info = select_best_candidate(mask_list)
            
        # Must have both to form a pair
        if best_proc and best_mask:
            group = "case" if patient[0].upper() == "C" else "control"
            rows.append({
                "group": group,
                "patient_id": patient,
                "view": view,
                "proc_url": BASE_URL + best_proc["file_id"],
                "mask_url": BASE_URL + best_mask["file_id"],
                "proc_name": _get_name(best_proc),
                "mask_name": _get_name(best_mask),
            })
            diagnostics["decisions"][key_str] = {
                "proc": proc_info,
                "mask": mask_info,
                "proc_candidates_count": len(proc_list),
                "mask_candidates_count": len(mask_list)
            }
        else:
            diagnostics["half_pairs"].append({
                "group": key_str,
                "has_proc": bool(best_proc),
                "has_mask": bool(best_mask),
                "proc_candidates": len(proc_list),
                "mask_candidates": len(mask_list)
            })

    # Write CSV manifest
    fieldnames = ["group", "patient_id", "view", "proc_url", "mask_url", "proc_name", "mask_name"]
    with open(args.output, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)

    # Write diagnostics
    args.diag.write_text(json.dumps(diagnostics, indent=2))

    print(f"Pairs created: {len(rows)}")
    print(f"CSV manifest written to: {args.output}")
    print(f"Diagnostics written to: {args.diag}")


if __name__ == "__main__":
    main()
