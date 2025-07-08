🧾 **Customer Loss Data Pipeline (End-to-End DE Project)**

This is a real-world data engineering project built using **Spark SQL on Databricks**. The goal was to design and implement a full data pipeline that simulates how companies manage customer loss, financial risk, and demographic analysis using production-grade data layers.

I built it using the **Medallion architecture**: Bronze → Silver → Silver Validated → Gold.

**💡 What's This Project About?**

Imagine you're working at a utilities or finance company. You have messy customer account data with financial losses, risk flags, and demographic info. You need to clean it, validate it, and deliver solid, trustworthy data to your business teams.

That's what this project does, it transforms raw customer loss data into fully usable BI and risk dashboards, through layered transformations.

**🧱 Architecture Overview (Medallion Style)**

- **Bronze Layer**  
  This is where I land the raw data as-is. No changes, no filters — it's like the backup copy.

- **Silver Layer**  
  I cleaned up the data, enforced types, added derived fields (like `IncomeBand`, `AccountAgeDays`), and created detailed validation flags (null checks, date sanity, offset rules).

- **Silver Validated Layer**  
  This is the strict gate. Only rows that pass *all* validation rules are allowed through. I added confidence scores, rejection reasons, row-level hashes, and even DQ metadata. Think of it like: "these rows are safe for business."

- **Gold Layer**  
  This is where the real business value comes out. I created summary tables that show loss trends, segment risk, and income-based behavior that can power dashboards or ML models.

**🧰 Tools & Stack**

- **Databricks**
- **Apache Spark SQL**
- **Unity Catalog (Schema & Governance)**
- **Medallion Architecture**
- **Delta Lake**
- **Data Quality Validation Rules**
- **Dimensional Modeling**
- **Versioning and Metadata Strategy**
- **Git + GitHub**

**📁 ETL Architecture**
## 🔄 ETL Flow Overview

This project follows a layered ETL approach using the Medallion architecture:

                            ┌──────────────────────────┐
                            │   sample_data (.csv)     │
                            └────────────┬─────────────┘
                                         │
                                         ▼
                          ┌────────────────────────────┐
                          │   🟫 Bronze Layer           │
                          │   Raw data: no changes     │
                          │   Table: customer_loss_raw │
                          └────────────┬───────────────┘
                                         │
                                         ▼
                          ┌────────────────────────────┐
                          │   🥈 Silver Layer           │
                          │   Typed, cleaned, enriched │
                          │   Table: customer_loss_clean│
                          └────────────┬───────────────┘
                                         │
                                         ▼
               ┌─────────────────────────────────────────────────┐
               │     🧪 Silver Validated Layer                   │
               │     Only high-confidence, valid rows            │
               │     + Flags, rejection reasons, row_hash, etc.  │
               │     Table: customer_loss_strict                 │
               └────────────┬────────────────────────────────────┘
                            │
                            ▼
    ┌────────────────────────────────────────────────────────────────────┐
    │                         🟡 Gold Layer                              │
    │   Aggregated business insights for dashboards and reporting:       │
    │   ├── loss_summary_by_customer_type                                │
    │   ├── loss_summary_by_income_band                                  │
    │   ├── monthly_loss_trends                                          │
    │   └── loss_summary_by_mosaic_group                                 │
    └────────────────────────────────────────────────────────────────────┘


**📊 What the Gold Tables Show**

Each Gold table gives a different lens on customer risk and financial loss:

1. **loss_summary_by_customer_type**  
   Shows how much loss is happening by customer type (Person vs Business), including loss rate and risk score.

2. **loss_summary_by_income_band**  
   Breaks down financial loss by income levels. Helps answer: are lower-income customers driving more losses?

3. **monthly_loss_trends**  
   Tracks loss trends over time. Super useful for dashboards or spotting seasonality.

4. **loss_summary_by_mosaic_group**  
   This one's based on demographics (MosaicGroup). It shows which lifestyle segments are riskier financially.

Each one has clear, business-relevant KPIs and a custom `loss_risk_score`.

**🧪 Data Quality Rules Used**

In the Silver and Silver Validated layers, I used these validation checks:

- **Date sequence check**  
  LossDate can't be before AccountStartDate

- **Offset sanity check**  
  Offset values must be above a threshold

- **Null critical field check**  
  Can't have missing values in fields like `CustomerType`, `OffsetValue`, etc.

- **Demographic null check**  
  Missing Mosaic or income info is flagged separately

These flags were all retained and used to gate data into the strict silver_validated layer.

I also added:
- `validation_status` (Valid / Invalid)
- `rejection_reason` and `rejection_code`
- `row_confidence_score` (a simple quality score per row)
- `row_hash` (for tracking)
- `processed_date`, `schema_version`, `table_version`


**📈 Business Use Cases This Supports**

- Financial Risk Dashboards
- BI reporting by segment and income
- Monthly loss trend analysis
- Early risk detection and customer targeting
- Future: ML models for loss prediction

**✍️ Author**

A data engineer who doesn’t just write SQL — I **engineer trust into data**.
If you're into clean architecture, business-aware pipelines, and solving real problems with explainable data... we should talk.
Feel free to reach out if you'd like a walkthrough, feedback, or want to build something exceptional together.



