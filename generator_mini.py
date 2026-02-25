#!/usr/bin/env python3
"""
Generate Croissant 1.0 metadata for the EDRN Breast Density dataset (MINI version).

Reads manifest_mini.csv (top-5 PROC/MASK pairs) and writes
outputs/croissant_mini.json.

Usage:
    python generator_mini.py
"""

import hashlib
import json
from pathlib import Path
import mlcroissant as mlc


MANIFEST_PATH = Path("manifest_mini.csv")
OUTPUT_PATH = Path("outputs/croissant_mini.json")


def sha256_of_file(path: Path) -> str:
    h = hashlib.sha256()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(8192), b""):
            h.update(chunk)
    return h.hexdigest()


def main():
    if not MANIFEST_PATH.exists():
        raise SystemExit(
            "manifest_mini.csv not found. Run this script from the project root."
        )

    sha = sha256_of_file(MANIFEST_PATH)
    print(f"manifest_mini.csv SHA-256: {sha}")

    metadata = mlc.Metadata(
        name="EDRN_Breast_Density_Collection_2_Mini",
        description=(
            "Mini subset (5 pairs) of the Automated Quantitative Measures of "
            "Breast Density Data - processed mammograms and segmentation masks "
            "streamed from LabCAS via authenticated URLs. "
            "Used for rapid prototyping and pipeline testing."
        ),
        conforms_to="http://mlcommons.org/croissant/1.0",
        cite_as="EDRN LabCAS Breast Density Collection",
        date_published="2025-01-22",
        license="https://creativecommons.org/licenses/by/4.0/",
        url="https://edrn-labcas.jpl.nasa.gov/collections/Automated_Quantitative_Measures_of_Breast_Density_Data",
        version="1.0.0-mini",
        distribution=[
            mlc.FileObject(
                id="manifest_mini.csv",
                name="manifest_mini.csv",
                description="Mini CSV manifest: top-5 matched PROC/MASK mammogram pairs.",
                content_url="../manifest_mini.csv",
                encoding_formats=["text/csv"],
                sha256=sha,
            ),
        ],
        record_sets=[
            mlc.RecordSet(
                id="mammograms",
                name="mammograms",
                description="Each record is a matched PROC/MASK pair for one patient-view combination.",
                fields=[
                    mlc.Field(
                        id="mammograms/group",
                        name="mammograms/group",
                        description="Case/control status: 'case' or 'control'.",
                        data_types=[mlc.DataType.TEXT],
                        source=mlc.Source(
                            file_object="manifest_mini.csv",
                            extract=mlc.Extract(column="group"),
                        ),
                    ),
                    mlc.Field(
                        id="mammograms/patient_id",
                        name="mammograms/patient_id",
                        description="Patient identifier (e.g. C0250, C0251).",
                        data_types=[mlc.DataType.TEXT],
                        source=mlc.Source(
                            file_object="manifest_mini.csv",
                            extract=mlc.Extract(column="patient_id"),
                        ),
                    ),
                    mlc.Field(
                        id="mammograms/view",
                        name="mammograms/view",
                        description="Mammographic view: LCC, LMLO, RCC, or RMLO.",
                        data_types=[mlc.DataType.TEXT],
                        source=mlc.Source(
                            file_object="manifest_mini.csv",
                            extract=mlc.Extract(column="view"),
                        ),
                    ),
                    mlc.Field(
                        id="mammograms/proc_url",
                        name="mammograms/proc_url",
                        description="Download URL for the processed mammogram DICOM.",
                        data_types=[mlc.DataType.URL],
                        source=mlc.Source(
                            file_object="manifest_mini.csv",
                            extract=mlc.Extract(column="proc_url"),
                        ),
                    ),
                    mlc.Field(
                        id="mammograms/mask_url",
                        name="mammograms/mask_url",
                        description="Download URL for the segmentation mask DICOM.",
                        data_types=[mlc.DataType.URL],
                        source=mlc.Source(
                            file_object="manifest_mini.csv",
                            extract=mlc.Extract(column="mask_url"),
                        ),
                    ),
                    mlc.Field(
                        id="mammograms/proc_name",
                        name="mammograms/proc_name",
                        description="Filename of the processed mammogram DICOM.",
                        data_types=[mlc.DataType.TEXT],
                        source=mlc.Source(
                            file_object="manifest_mini.csv",
                            extract=mlc.Extract(column="proc_name"),
                        ),
                    ),
                    mlc.Field(
                        id="mammograms/mask_name",
                        name="mammograms/mask_name",
                        description="Filename of the segmentation mask DICOM.",
                        data_types=[mlc.DataType.TEXT],
                        source=mlc.Source(
                            file_object="manifest_mini.csv",
                            extract=mlc.Extract(column="mask_name"),
                        ),
                    ),
                ],
            ),
        ],
    )

    OUTPUT_PATH.parent.mkdir(parents=True, exist_ok=True)
    jsonld = metadata.to_json()
    OUTPUT_PATH.write_text(json.dumps(jsonld, indent=2, ensure_ascii=True), encoding='utf-8')
    print(f"Croissant mini metadata written to {OUTPUT_PATH}")


if __name__ == "__main__":
    main()
