# EDRN Breast Density – Croissant Dataset

> Croissant 1.0 metadata, manifest tooling, and an example of using Croissant to load and train ML models on the [EDRN Automated Quantitative Measures of Breast Density](https://edrn-labcas.jpl.nasa.gov/labcas-ui/c/index.html?collection_id=Automated_Quantitative_Measures_of_Breast_Density_Data) dataset.

---

## Overview

This repository packages the EDRN Breast Density dataset as a **[Croissant](https://mlcommons.org/croissant/)** dataset, enabling reproducible, standardised ML data access. The full dataset contains **2437 matched PROC/MASK DICOM pairs** across case and control patients; a 5-pair mini subset is included for rapid pipeline testing.

---

## Quickstart

### 1. Install dependencies

```bash
pip install mlcroissant pydicom torch torchvision matplotlib pandas scikit-learn tqdm requests
```

### 2. Set credentials

Data is hosted on the [EDRN LabCAS server](https://edrn-labcas.jpl.nasa.gov/labcas-ui/index.html) and requires authentication:

```bash
export LABCAS_USERNAME=your_username
export LABCAS_PASSWORD=your_password
```

### 3. Harvest metadata from LabCAS
(The metadata is already harvested in the `harvested_metadata` directory)

```bash
python harvest_metadata.py
```

### 4. Build manifest, generate Croissant metadata, and validate

```bash
python build_manifest.py
python generator.py
mlcroissant validate --jsonld outputs/croissant.json
```

*Note: If you want to load a mini subset for testing, run:*

```bash
python build_manifest_mini.py
python generator_mini.py
mlcroissant validate --jsonld outputs/croissant_mini.json
```

### 5. Run the training notebook

Open `train_unet.ipynb` in Jupyter. The notebook covers loading Croissant metadata, building a DataFrame and EDA plots, authenticating with LabCAS and defining a DICOM downloader, downloading and visualising all PROC/MASK pairs, setting up the `MammogramDataset` and train/val/test splits, defining a lightweight U-Net with Dice+BCE loss, and running the training loop with metrics plots and test evaluation.


---

## Repository structure

```
edrn-breast-density-croissant/
│
├── harvested_metadata/               ← raw metadata harvested from LabCAS API
│   ├── collection.json               ← top-level collection metadata
│   ├── datasets.json                 ← all datasets in the collection
│   ├── leaf_datasets.json            ← leaf (file-containing) datasets only
│   └── resources_by_dataset.json     ← file metadata per dataset
│
├── outputs/                          ← generated Croissant metadata files
│   ├── croissant.json                ← full Croissant 1.0 metadata (2437 pairs)
│   ├── croissant_mini.json           ← mini Croissant metadata (5 pairs)
│   ├── croissant_individual_fileobjects.json  ← alternative schema variant
│
├── build_manifest.py                 ← build manifest.csv from harvested metadata
├── generator.py                      ← generate outputs/croissant.json
├── generator_mini.py                 ← generate outputs/croissant_mini.json
├── harvest_metadata.py               ← entry point: run full LabCAS harvest
├── harvester.py                      ← LabCAS metadata harvester class
├── labcas_client.py                  ← authenticated LabCAS REST client
├── loader.py                         ← minimal mlcroissant usage example
├── manifest.csv                      ← full dataset index (2437 PROC/MASK pairs)
├── manifest_mini.csv                 ← 5-pair mini subset for testing
├── train_unet.ipynb                  ← simple U-Net training notebook
├── __init__.py
├── .gitattributes                    ← Git LFS tracking rules
├── .gitignore
└── README.md
```
