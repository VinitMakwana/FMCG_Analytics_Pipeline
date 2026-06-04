# 🏭 Unified Analytics Pipeline — Atlikon × Sports Bar
### End-to-End Data Engineering Project on Databricks (FMCG Domain)

## 📖 Project Overview

This project delivers a **production-grade, end-to-end data engineering pipeline** on **Databricks (Free Edition)** to unify the data infrastructure of **Atlikon** (parent company, sports equipment manufacturer) and its newly acquired subsidiary **Sports Bar** (energy bar startup FMCG). 

The pipeline ingests raw, fragmented data from an **AWS S3 data lake**, processes it through a **Medallion Architecture** (Bronze → Silver → Gold), and surfaces unified, BI-ready analytics via **Databricks Dashboards** and the **Genie AI assistant** — enabling leadership to make cross-entity decisions without waiting for a multi-year migration.

| Attribute | Detail |
|---|---|
| **Domain** | FMCG (Fast-Moving Consumer Goods) |
| **Companies** | Atlikon (Parent) + Sports Bar (Subsidiary) |
| **Goal** | Unified, reliable analytics dashboard across merged entities |
| **Platform** | Databricks Community / Free Edition |
| **Storage** | AWS S3 (Data Lake) |
| **Pipeline Pattern** | Medallion Architecture (Bronze / Silver / Gold) |

---

## ❗ Problem Statement

Atlikon, a leading sports equipment manufacturer, recently acquired **Sports Bar**, a fast-growing energy bar startup. While Atlikon operates mature ERP systems with structured data pipelines, Sports Bar's data is highly fragmented:

- 📊 Spreadsheets scattered across team drives
- 💬 WhatsApp conversation exports
- ☁️ Inconsistent cloud storage
- 🔌 Unstandardised APIs with no schema enforcement

This **"data chaos"** created a critical bottleneck:

Leadership could not unify supply chain forecasting or inventory planning because data formats were mismatched, reporting cycles were misaligned, and metrics were inconsistent across both companies.

**The requirement:** A **reliable, scalable, low-learning-curve** solution to provide streamlined cross-company insights — without waiting for a multi-year ERP migration project.

---

## 🏗️ Architecture

![alt text](resources/FMCG_Architecture.png)

### Layer Descriptions

#### 🥉 Bronze Layer — Raw Data Ingestion
- Reads raw CSV files from the AWS S3 landing zone
- Adds ingestion metadata: `ingestion_timestamp`, `source_file_name`, `source_system`
- No transformations applied; data preserved in original form
- Supports full historical backfill and daily incremental loads

#### 🥈 Silver Layer — Data Cleaning & Transformation
- **Deduplication:** Removes duplicate records using `ROW_NUMBER()` window functions
- **Null Handling:** Imputes or flags missing values based on business rules
- **String Normalization:** Regex-based fixes for city names, product names, and category typos
- **Date Standardization:** Converts all date formats to `YYYY-MM-DD` ISO standard
- **Currency & Unit Conversion:** Normalises values to INR and standard units (kg, litre)
- **SHA-256 Surrogate Keys:** Generates deterministic keys for dimensional entities

#### 🥇 Gold Layer — BI-Ready Aggregated Data
- **Star Schema:** Fact table (`fact_orders`) + dimension tables (`dim_customers`, `dim_products`, `dim_date`, `dim_gross_price`)
- **SCD Type-1 Upserts:** Merge/upsert logic using Databricks `MERGE INTO` for slowly changing dimensions
- **Cross-entity Join:** Sports Bar data merged into Atlikon's Gold layer tables
- **Pre-aggregated KPIs:** Revenue by product, sales by quarter, channel performance

---

## 📁 Project Structure
```text
Atlikon-sportsbar-pipeline/
│
├── 0_data/
│   ├── 1_parent_company/
│   │   ├── full_load/
│   │   └── incremental_load/
│   │
│   └── 2_child_company/
│       ├── full_load/
│       └── incremental_load/
│
├── 1_codes/
│   │
│   ├── 1_setup/
│   │   ├── dim_date_table_creation.ipynb
│   │   ├── setup_catalog.ipynb
│   │   └── utilities.ipynb
│   │
│   ├── 2_dimension_data_processing/
│   │   ├── 1_customers_data_processing.ipynb
│   │   ├── 2_products_data_processing.ipynb
│   │   ├── 3_pricing_data_processing.ipynb
│   │   └── .ipynb_checkpoints/
│   │
│   ├── 3_fact_data_processing/
│   │   ├── 1_full_load_fact.ipynb
│   │   └── 2_incremental_load_fact.ipynb
│   │
│   └── 2_dashboarding/
│       ├── denormalise_table_query_fmcg.txt
│       └── fmcg_dashboard.pdf
│
├── resources/
│   ├── databricks_project.excalidraw
│   └── project_architecture.png
│
├── README.md
└── requirements.txt
```

---

# Folder Description

| Folder | Purpose |
|----------|----------|
| **0_data** | Stores raw source data received from Parent and Child organizations. |
| **1_parent_company** | Parent organization datasets for Full Load and Incremental Load processing. |
| **2_child_company** | Child organization datasets for Full Load and Incremental Load processing. |
| **1_setup** | Environment setup, catalog creation, utility functions, and date dimension generation. |
| **2_dimension_data_processing** | ETL pipelines for processing dimension tables (Customer, Product, Pricing). |
| **3_fact_data_processing** | ETL pipelines for building Fact tables through Full Load and Incremental Load strategies. |
| **2_dashboarding** | Gold-layer reporting queries and dashboard artifacts. |
| **resources** | Architecture diagrams, design documents, and project reference materials. |

---

# Medallion Architecture Mapping

```text
Raw Files (S3)
      │
      ▼
Bronze Layer
      │
      ▼
Silver Layer
      │
      ▼
Gold Layer
      │
      ├── Parent Company Analytics
      ├── Databricks Dashboards
      └── Genie AI/BI
```

---

# Processing Flow

1. Load raw files from Parent and Child companies.
2. Store source data in Bronze Layer.
3. Apply cleansing, validation, and transformations in Silver Layer.
4. Generate business-ready Gold tables.
5. Merge Child Company Gold data with Parent Company Gold layer.
6. Expose curated datasets to:
   - Databricks Dashboards
   - Genie AI
   - Reporting & Analytics Consumers


---

## 🛠️ Technologies & Tools

| Category | Technology | Purpose |
|---|---|---|
| **Platform** | Databricks Free Edition | Unified compute + notebook environment |
| **Storage** | AWS S3 | Data lake (landing + processed zones) |
| **Language** | Python (PySpark) | Distributed data processing |
| **Language** | SQL (Spark SQL) | Data modelling, aggregations, upserts |
| **Architecture** | Medallion (Bronze/Silver/Gold) | Layered data quality framework |
| **Schema** | Star Schema | Dimensional modelling for BI |
| **Orchestration** | Databricks Jobs | Task chaining & scheduling |
| **BI** | Databricks Dashboard | Self-serve analytics interface |
| **AI** | Databricks Genie | Natural language query assistant |
| **Secrets** | Databricks Secret Scopes | Secure credential management |
| **Format** | Delta Lake | ACID transactions + time travel |
| **Version Control** | Git + GitHub | Source control |

---

## ⚙️ Setup & Installation

### Prerequisites

Ensure you have the following before starting:

- Databricks Free Edition account → [Sign up here](https://community.cloud.databricks.com/)
- AWS account with an S3 bucket created
- Git installed locally
- Python 3.9+ (for running local tests)

---

### Step 1: Clone the Repository

```bash
git clone https://github.com/<your-org>/Atlikon-sportsbar-pipeline.git
cd Atlikon-sportsbar-pipeline
```

### Step 2: Install Python Dependencies (Local Testing Only)

```bash
pip install -r requirements.txt
```

### Step 3: Configure AWS S3

1. Create an S3 bucket (e.g., `s3://Atlikon-datalake/`)
2. Create the following folder structure inside the bucket:

```
Atlikon-datalake/
├── landing/
│   
└── processed/
```

3. Upload your raw CSV source files to the `landing/` folders.


### Step 6: Import Notebooks into Databricks

1. In Databricks workspace, go to **Workspace → Import**
2. Import all notebooks from the `/notebooks/` directory, maintaining the folder structure
3. Attach each notebook to a running cluster (DBR 13.x LTS or above recommended)

---

## ▶️ Running the Pipeline

### Option A: Run Notebooks Manually (Development)

Execute notebooks in the following order inside Databricks Workspace:

```text
1_codes/
│
├── 1_setup/
│   ├── setup_catalog.ipynb
│   ├── dim_date_table_creation.ipynb
│   └── utilities.ipynb
│
├── 2_dimension_data_processing/
│   ├── 1_customers_data_processing.ipynb
│   ├── 2_products_data_processing.ipynb
│   └── 3_pricing_data_processing.ipynb
│
└── 3_fact_data_processing/
    ├── 1_full_load_fact.ipynb
    └── 2_incremental_load_fact.ipynb
```

### Execution Flow

#### Initial Setup (Run Once)

```text
1_setup/setup_catalog.ipynb
      ↓
1_setup/dim_date_table_creation.ipynb
      ↓
1_setup/utilities.ipynb
```

Creates:

- Unity Catalog Structure
- Schemas
- Storage Locations
- Utility Functions
- Date Dimension Table

---

#### Full Load Processing

```text
Parent Company Full Load Files
          +
Child Company Full Load Files
          ↓
1_customers_data_processing.ipynb
          ↓
2_products_data_processing.ipynb
          ↓
3_pricing_data_processing.ipynb
          ↓
1_full_load_fact.ipynb
```

Output:

- Bronze Tables
- Silver Tables
- Gold Dimension Tables
- Gold Fact Table

---

#### Incremental Load Processing

```text
Parent Incremental Files
          +
Child Incremental Files
          ↓
1_customers_data_processing.ipynb
          ↓
2_products_data_processing.ipynb
          ↓
3_pricing_data_processing.ipynb
          ↓
2_incremental_load_fact.ipynb
```

Output:

- Incremental Bronze Updates
- Incremental Silver Updates
- SCD/Merge Processing
- Gold Fact Refresh

---

### Dashboard Layer

After Gold tables are refreshed:

```text
2_dashboarding/
│
├── denormalise_table_query_fmcg.txt
└── fmcg_dashboard.pdf
```

Steps:

1. Execute denormalization query.
2. Create reporting table/view.
3. Connect Databricks Dashboard.
4. Configure Genie AI semantic layer.
5. Publish business KPIs.

---

## Option B: Run via Databricks Workflows (Production)

### Full Load Job

Create a Databricks Job with the following task sequence:

```text
Task 1 → setup_catalog
      ↓
Task 2 → dim_date_table_creation
      ↓
Task 3 → customers_data_processing
      ↓
Task 4 → products_data_processing
      ↓
Task 5 → pricing_data_processing
      ↓
Task 6 → full_load_fact
```

Run:

```text
Trigger Type: Manual
Purpose: Historical Backfill / Initial Load
```

---

### Incremental Load Job

Create a Databricks Job with the following task sequence:

```text
Task 1 → customers_data_processing
      ↓
Task 2 → products_data_processing
      ↓
Task 3 → pricing_data_processing
      ↓
Task 4 → incremental_load_fact
```

Run:

```text
Trigger Type: Scheduled
Frequency: Daily
Recommended Time: 11:00 PM
```

---

## End-to-End Data Flow

```text
Raw Files
(Parent Company + Child Company)
            │
            ▼
      Lakeflow Jobs
            │
            ▼
      Bronze Layer
            │
            ▼
      Silver Layer
            │
            ▼
      Gold Layer
            │
            ├── Child Company Gold Tables
            │
            └── Parent Company Gold Tables
                      │
                      ▼
            Consolidated Gold Analytics
                      │
                      ▼
      ┌───────────────┴───────────────┐
      │                               │
      ▼                               ▼
Databricks Dashboard            Genie AI
```

### Recommended Execution Frequency

| Layer | Frequency |
|---------|-----------|
| Bronze | Daily |
| Silver | Daily |
| Dimensions | Daily |
| Fact Tables | Daily |
| Dashboard Refresh | Daily |
| Genie Semantic Refresh | Daily |


## 📊 Business Insights & BI Dashboard

The Gold Layer powers an interactive Databricks Dashboard that provides a consolidated view of sales performance across Parent and Child Companies.

### Dashboard Filters

The dashboard supports dynamic filtering across:

| Filter | Description |
|----------|-------------|
| Year | Analyze performance by year |
| Quarter | Quarterly analysis |
| Month | Monthly analysis |
| Channel | Retail, Direct, Acquisition, etc. |
| Category | Product category-wise insights |

---

## Executive KPIs

| KPI | Description |
|------|------------|
| **Total Revenue** | Total business revenue generated across all channels |
| **Total Quantity Sold** | Total units sold |
| **Unique Customers** | Distinct customer count |
| **Average Selling Price (ASP)** | Average product selling price |

---

## Revenue & Sales Analytics

| Visualization | Business Value |
|--------------|----------------|
| **Top Products by Revenue** | Identifies highest revenue-generating products |
| **Revenue Share by Channel** | Shows contribution of each sales channel |
| **Monthly Revenue Trend** | Tracks seasonality and growth patterns over time |
| **Top Variants by Revenue** | Highlights best-performing product variants |

---

## Customer Analytics

| Visualization | Business Value |
|--------------|----------------|
| **Customer Revenue Leaderboard** | Top customers ranked by revenue contribution |
| **Customer Quantity Analysis** | Evaluates customer purchase volumes |
| **Customer Segmentation Support** | Helps identify high-value customers |

---

## Product Analytics

| Visualization | Business Value |
|--------------|----------------|
| **Product Price vs Quantity Analysis** | Evaluates pricing impact on sales volume |
| **Category-wise Product Performance** | Measures category contribution to revenue |
| **Variant-wise Revenue Analysis** | Tracks performance of different product variants |

---

## Dashboard 
![alt text](2_dashboard/dashboard.png)


## Business Questions Answered

### Sales Performance

- What is the total revenue generated?
- Which products generate the highest revenue?
- How does revenue trend across months?
- Which product variants contribute the most sales?

### Channel Performance

- Which sales channel contributes the most revenue?
- What percentage of revenue comes from each channel?

### Customer Insights

- Who are the highest revenue-generating customers?
- Which customers purchase the highest quantities?
- How concentrated is revenue among top customers?

### Product Insights

- Does product pricing affect quantity sold?
- Which categories perform best?
- Which variants are most profitable?

---


### Genie AI Assistant

Databricks Genie is configured to answer natural language questions over the Gold layer tables. Example queries stakeholders can ask:

> *"What were the top 5 products by revenue last quarter across both companies?"*
>
> *"Which city had the highest Sports Bar sales in FY2024?"*
>
> *"Compare atlikon and Sports Bar revenue month-over-month."*

---

## ⚙️ Workflow & Orchestration

### Incremental Loading Strategy

```
Daily at 11:00 PM
       │
       ▼
  Check S3 landing/
  for new files
       │
       ├─── New files found ──▶ Bronze Ingest ──▶ Silver Clean ──▶ Gold Merge (Upsert)
       │
       └─── No new files ────▶ Log "No new data" ──▶ Exit
```

---

## 📋 Requirements

```
# requirements.txt
pyspark==3.5.0
delta-spark==3.0.0
boto3==1.34.0
pytest==7.4.0
pandas==2.1.0
```

---

