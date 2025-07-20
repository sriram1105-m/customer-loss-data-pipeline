![image](https://github.com/user-attachments/assets/93447953-29fa-4582-bd69-498b5fa5496e)




<h1>🧾 Customer Loss Data Pipeline (End-to-End DE Project) </h1>

A real world data engineering project using **Databricks**, **Spark SQL**, and **Delta Lake**

<h1>💼 Business Problem and Project Goal </h1>

In critical industries like **finance** and  **utilities**, even a small increase in **customer churn** or **credit risk** can result in **millions in financial loss**. These companies collect vast amounts of customer data, but much of it is **incomplete, inconsistent, or unreliable**, making it difficult to derive actionable insights.

The core issue is that **poor data quality** undermines everything that follows:
- Reports are misleading
- **Risk flags are missed**
- Business users **lose trust in dashboards**
- Analysts waste hours on **manual data cleanup** instead of delivering insights

This project addresses that challenge by designing a **production-grade data pipeline** that takes **raw customer loss data** and transforms it into **trusted, validated, and analysis-ready insights.**
The pipeline enforces strict **data quality rules**, applies **multi-stage validation**, tracks **row-level metadata**, and organizes data using the **Medallion architecture**. The end goal is to create a **high-confidence data layer** that supports **business reporting, risk dashboards,** and **predictive analytics** with full **auditability** and **traceability**.
This is not just a technical build, it reflects how **modern data engineering** helps companies reduce risk, increase operational efficiency, and make **data-driven decisions** with confidence.

**🧠 Project Summary**

This project demonstrates a fully operational **ETL pipeline** built on **Databricks**, using **Spark SQL**, **Delta Lake**, and **Unity Catalog**. It simulates a real-world scenario where messy, high-volume customer data must be turned into **trustworthy, business-ready insights.**

Data flows through a structured **Medallion Architecture:**
- **Bronze Layer:** Raw ingest with no transformation
- **Silver Layer:** Cleaned, typed, and enriched with derived fields
- **Silver Validated Layer:** Strict validation with rejection tracking and scoring
- **Gold Layer:** Aggregated **business KPIs** for use in **BI Dashboards, risk modelling,** and **churn analysis.**

The result is a pipeline that supports everything from **operational reporting** to **machine learning**, all while maintaining high standards for **data quality, scalability, and governance.**

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
                          │   🟫 Bronze Layer          │
                          │   Raw data: no changes     │
                          │   Table: customer_loss_raw │
                          └────────────┬───────────────┘
                                         │
                                         ▼
                          ┌────────────────────────────┐
                          │   🥈 Silver Layer          │
                          │   Typed, cleaned, enriched │
                          │  Table: customer_loss_clean│
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

**⚙️ Pipeline Capabilities**

This project reflects real-world ETL responsibilities as expected in modern data engineering roles:

**✅ End-to-End ETL Pipeline**
- Covers full ETL scope: ingest raw data, clean and validate, deliver business-ready outputs.
- Built using modular SQL scripts, fully compatible with modern Lakehouse architecture.

**✅ Batch Data Processing**
- Data is processed in batch mode (daily/weekly) using Delta-like layer separation (Bronze → Gold).
- Supports business use cases like financial reporting, risk monitoring, and churn analysis.

**⚠️ Real-Time Compatible Design**
- While this pipeline runs in batch mode, it’s structured to support real-time ingestion in production:
  - Bronze layer could easily be fed by Kafka or Auto Loader
  - Validation logic and transformations are stateless and stream-friendly
- Design is ready for future integration with tools like **Delta Live Tables**, **Structured Streaming**, or **Apache Kafka**.


**✍️ Author**

A data engineer who doesn’t just write SQL. I **engineer trust into data**.
If you're into clean architecture, business-aware pipelines, and solving real problems with explainable data... we should talk.
Feel free to reach out if you'd like a walkthrough, feedback, or want to build something exceptional together.



