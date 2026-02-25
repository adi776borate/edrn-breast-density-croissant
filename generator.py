#!/usr/bin/env python3
"""
Generate Croissant 1.0 metadata for the EDRN Breast Density dataset
using the mlcroissant Python library.

Reads manifest.csv (produced by build_manifest.py) and writes
biomed_croissant.json.
"""

import hashlib
import json
from pathlib import Path
import mlcroissant as mlc


MANIFEST_PATH = Path("manifest.csv")
OUTPUT_PATH = Path("output/croissant.json")


def sha256_of_file(path: Path) -> str:
    h = hashlib.sha256()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(8192), b""):
            h.update(chunk)
    return h.hexdigest()


def main():
    if not MANIFEST_PATH.exists():
        raise SystemExit(
            "manifest.csv not found. Run build_manifest.py first."
        )

    sha = sha256_of_file(MANIFEST_PATH)
    print(f"manifest.csv SHA-256: {sha}")

    metadata = mlc.Metadata(
        name="EDRN_Breast_Density_Collection_2",
        description=(
            "Automated Quantitative Measures of Breast Density Data - "
            "processed mammograms and segmentation masks streamed from "
            "LabCAS via authenticated URLs."
        ),
        conforms_to="http://mlcommons.org/croissant/1.0",
        cite_as="EDRN LabCAS Breast Density Collection",
        date_published="2025-01-22",
        license="https://creativecommons.org/licenses/by/4.0/",
        url="https://edrn-labcas.jpl.nasa.gov/collections/Automated_Quantitative_Measures_of_Breast_Density_Data",
        version="1.0.0",
        distribution=[
            mlc.FileObject(
                id="manifest.csv",
                name="manifest.csv",
                description="CSV manifest of matched PROC/MASK mammogram pairs with download URLs.",
                content_url="manifest.csv",
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
                            file_object="manifest.csv",
                            extract=mlc.Extract(column="group"),
                        ),
                    ),
                    mlc.Field(
                        id="mammograms/patient_id",
                        name="mammograms/patient_id",
                        description="Patient identifier (e.g. C0250, N0500).",
                        data_types=[mlc.DataType.TEXT],
                        source=mlc.Source(
                            file_object="manifest.csv",
                            extract=mlc.Extract(column="patient_id"),
                        ),
                    ),
                    mlc.Field(
                        id="mammograms/view",
                        name="mammograms/view",
                        description="Mammographic view: LCC, LMLO, RCC, or RMLO.",
                        data_types=[mlc.DataType.TEXT],
                        source=mlc.Source(
                            file_object="manifest.csv",
                            extract=mlc.Extract(column="view"),
                        ),
                    ),
                    mlc.Field(
                        id="mammograms/proc_url",
                        name="mammograms/proc_url",
                        description="Download URL for the processed mammogram DICOM.",
                        data_types=[mlc.DataType.URL],
                        source=mlc.Source(
                            file_object="manifest.csv",
                            extract=mlc.Extract(column="proc_url"),
                        ),
                    ),
                    mlc.Field(
                        id="mammograms/mask_url",
                        name="mammograms/mask_url",
                        description="Download URL for the segmentation mask DICOM.",
                        data_types=[mlc.DataType.URL],
                        source=mlc.Source(
                            file_object="manifest.csv",
                            extract=mlc.Extract(column="mask_url"),
                        ),
                    ),
                    mlc.Field(
                        id="mammograms/proc_name",
                        name="mammograms/proc_name",
                        description="Filename of the processed mammogram.",
                        data_types=[mlc.DataType.TEXT],
                        source=mlc.Source(
                            file_object="manifest.csv",
                            extract=mlc.Extract(column="proc_name"),
                        ),
                    ),
                    mlc.Field(
                        id="mammograms/mask_name",
                        name="mammograms/mask_name",
                        description="Filename of the segmentation mask.",
                        data_types=[mlc.DataType.TEXT],
                        source=mlc.Source(
                            file_object="manifest.csv",
                            extract=mlc.Extract(column="mask_name"),
                        ),
                    ),
                ],
            ),
        ],
    )

    jsonld = metadata.to_json()
    OUTPUT_PATH.write_text(json.dumps(jsonld, indent=2, ensure_ascii=False))
    print(f"Croissant metadata written to {OUTPUT_PATH}")


if __name__ == "__main__":
    main()