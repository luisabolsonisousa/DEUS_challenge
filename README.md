# DEUS Code Challenge – Data Engineering with PySpark

## Overview

This repository contains the solution to a data engineering coding challenge focused on processing and transforming retail data using PySpark. The goal is to demonstrate the ability to work with structured data, apply validation and transformation logic, and deliver insights in a clean and reproducible pipeline.

## Objective

The task simulates a real-world scenario involving transactional sales data. The solution includes:
- Loading CSV data into Spark DataFrames
- Validating and cleaning the datasets
- Performing aggregations and generating business insights
- Enriching data by combining multiple sources
- Exporting results in the required formats
- Writing automated tests for critical logic
- Setting up a CI workflow using GitHub Actions

## Datasets

The input data consists of three CSV files:
- **Sales transactions** (`sales_uuid.csv`)
- **Products** (`products_uuid.csv`)
- **Stores** (`stores_uuid.csv`)

Each file includes relevant identifiers and attributes, such as product categories, pricing, quantities sold, and store locations.

## Implementation Highlights

- Explicit schema definitions using `StructType` to ensure schema consistency
- Null and duplicate checks implemented before transformation
- Revenue aggregations by store and category
- Monthly sales quantity aggregated by category
- Unified dataset with enriched attributes, including store and product information
- Custom UDF to categorize products by price range

## Outputs

The following outputs are generated:
- **Parquet**: Enriched dataset, partitioned by `category` and `transaction_date`
- **CSV**: Revenue insights aggregated by `store_id` and `category`

## How to Run

```bash
# Create and activate virtual environment
python3 -m venv venv
source venv/bin/activate

# Install dependencies
pip install -r requirements.txt

# Execute the pipeline
python main.py
```

## Testing

Unit tests are implemented using `pytest` and `chispa`.

```bash
pytest tests/
```

## CI/CD

A GitHub Actions workflow (`.github/workflows/build.yml`) automatically runs the test suite on every push and pull request to the `main` branch.

## Assumptions & Notes

- Input data is expected to follow the defined schema. Basic validation is performed but not exhaustive.
- UDFs are used to classify products into predefined pricing categories (Low, Medium, High).
- File paths and formats are configurable via a YAML file under `src/configs/config.yaml`.

## Project Structure

```
.
├── src/
│   ├── configs/
│   ├── data/
│   └── utils/
├── tests/
├── export/
├── main.py
└── config.yaml
```

## Author

Developed by Mariana as part of a technical code challenge.

