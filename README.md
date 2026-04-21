# E-Commerce Data Lakehouse

An end-to-end data lakehouse built on AWS processing 100K real Brazilian e-commerce orders through a 3-layer S3 architecture queried with Amazon Athena.

[![Python](https://img.shields.io/badge/Python-3.11-3776AB?style=flat-square&logo=python&logoColor=white)](https://python.org)
[![AWS S3](https://img.shields.io/badge/AWS_S3-569A31?style=flat-square&logo=amazons3&logoColor=white)](https://aws.amazon.com/s3)
[![Athena](https://img.shields.io/badge/Amazon_Athena-232F3E?style=flat-square&logo=amazonaws&logoColor=white)](https://aws.amazon.com/athena)

---

## Architecture

Kaggle Dataset → S3 Raw Layer → S3 Processed Layer → S3 Curated Layer → Amazon Athena

- Raw Layer: Original Kaggle data uploaded as Parquet files with Hive-style date partitioning
- Processed Layer: Cleaned, validated and standardized data
- Curated Layer: Business-ready aggregations (revenue, products, customers)
- Athena: Serverless SQL queries directly on S3 Parquet files

---

## AWS Console Screenshots

### S3 Bucket - 3 Layer Architecture
![S3 Bucket](Lakehouse_ss/Screenshot_2026-04-21_182527.png)

### S3 Raw Layer - 7 Datasets
![S3 Raw](Lakehouse_ss/Screenshot_2026-04-21_183043.png)

### Athena Query Results
![Athena](Lakehouse_ss/Screenshot_2026-04-21_183802.png)

---

## S3 Structure

    s3://saikrishna-ecommerce-lakehouse/
    ├── raw/
    │   ├── orders/year=2026/month=04/day=21/data.parquet
    │   ├── customers/year=2026/month=04/day=21/data.parquet
    │   ├── products/year=2026/month=04/day=21/data.parquet
    │   ├── order_items/year=2026/month=04/day=21/data.parquet
    │   ├── payments/year=2026/month=04/day=21/data.parquet
    │   ├── reviews/year=2026/month=04/day=21/data.parquet
    │   └── sellers/year=2026/month=04/day=21/data.parquet
    ├── processed/
    ├── curated/
    │   ├── revenue_summary/
    │   ├── product_performance/
    │   └── customer_summary/
    └── athena-results/

---

## Athena Query Results

Orders by Status:

    delivered    96,478
    shipped       1,107
    canceled        625
    invoiced        314
    processing      301

Top 5 Categories by Revenue:

    beleza_saude (Beauty)         $1,258,681
    relogios_presentes (Watches)  $1,205,005
    cama_mesa_banho (Bedding)     $1,036,988
    esporte_lazer (Sports)          $988,048
    informatica_acessorios (IT)     $911,954

---

## Pipeline Results

- Total orders: 99,441
- Delivered: 96,478 (97%)
- Pipeline runtime: 19.3 seconds
- File format: Parquet (columnar, compressed)
- Partitioning: Hive-style by year/month/day

---

## Data Quality Checks

- No null primary keys
- No duplicate IDs
- Positive price values
- Valid order statuses
- Valid payment values

---

## Project Structure

    ecommerce-lakehouse/
    ├── src/
    │   ├── extract/uploader.py       - Upload CSV to S3 as Parquet
    │   ├── transform/processor.py    - Clean and process data
    │   ├── load/athena_loader.py     - Create Athena tables
    │   └── quality/validator.py      - Data quality checks
    ├── config/config.yaml            - AWS settings
    ├── Lakehouse_ss/                 - AWS console screenshots
    ├── run_pipeline.py               - Main pipeline
    └── requirements.txt

---

## Setup

1. Install dependencies: pip install -r requirements.txt
2. Configure AWS: aws configure
3. Copy Kaggle CSV files to data/kaggle/
4. Run: python run_pipeline.py

---

## Dataset

Brazilian E-Commerce dataset from Kaggle (Olist) - 100,000 real orders from 2016-2018

---

## Connect

Saikrishna Suryavamsham - Senior Data Engineer - Tampa FL

LinkedIn: linkedin.com/in/sai207
Email: krishnasv207@gmail.com