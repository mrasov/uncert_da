# How to Stop Worrying the Experimental Uncertainty of Heterogeneous Bioactivity Data

## About
This repository contains scripts used for data processing and analysis related to the scientific article titled **"How to Stop Worrying the Experimental Uncertainty of Heterogeneous Bioactivity Data"**.

## Project Structure
- `data/`: Contains raw and cleaned datasets in `.prqt` format.
- `plots/`: Visualization results such as `.png` and `.svg` plots.
- `scripts/`: Python scripts for various data processing and analysis steps.

## Scripts Execution Sequence
The following is the order in which the scripts should be run for successful data processing:

1. `sql_queries.py`: Extract relevant data using SQL queries.
2. `generate_pairs.py`: Generate pairs of molecule-target systems for further analysis.
3. `prot_info_sparql.py`: Fetch UniProt information using SPARQL queries.
4. `crossref_info.py`: Retrieve additional information about documents.
5. `assay_regex.py`: Extract features from assay descriptions using regular expressions.
6. `main_steps.py`: Perform the main data processing steps.

Ensure to execute the scripts in the above sequence for proper results.