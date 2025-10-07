# 🩺 Health Data Pipeline — Airflow DAG Overview

This project showcases a robust, production-ready data pipeline orchestrated with Apache Airflow. It ingests, validates, and transforms healthcare staffing data from  Amazon S3 into Amazon Redshift, progressing through bronze, silver, and gold layers—aligned with modern data lakehouse principles.

## 👤 Developer/Engineer

**Bagus Adiyanto**  
Data Engineer | Software Engineer 

## 🎯 Project Goals

- Reliable ingestion of healthcare data from S3 into Redshift
- Automated validation of critical dimensions and fact tables
- Layered transformation into silver and gold datasets for analytics
- Audit-ready orchestration with clear task dependencies and gates

## 🧠 Why It Matters

This DAG reflects real-world production standards:
- Validation gates enforce data integrity before transformations proceed
- Stored procedures encapsulate business logic for maintainability
- Parallel validation and modular task design support scalability

## 🧩 DAG Highlights

- **Ingestion**: Python-based ingestion of new S3 files into Redshift
- **Validation**: Three parallel checks on provider info, staffing dates, and fact table integrity
- **Transformation**:
  - Silver Layer: Provider, staffing type, workdate, and fact table dimensions
  - Gold Layer: Aggregated utilization metrics for reporting
- **Control Gates**: Ensure downstream tasks only run on validated data

## 🛠️ Technologies Used

- Apache Airflow (DAG orchestration)
- Amazon Redshift (Data warehouse)
- Amazon S3 (Source data storage)
- Python & SQL (Validation and transformation logic)

## ✅ Production Considerations

- No DAG changes required for new file ingestion—supports decoupled triggers
- IAM and connection management handled via `redshift_default` Airflow connection
- Stored procedures ensure business logic is version-controlled and centralized

## 📁 Notes

- Stored procedures (e.g., `sp_generate_silver_provider_dim`) must exist in Redshift
- Optional upstream task `gdrive_to_s3` is currently commented out but can be re-enabled if needed