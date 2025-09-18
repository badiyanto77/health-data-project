# Data Engineering Pipeline: Google Drive → S3 → Redshift → Power BI  

## 📌 Project Overview  
This project implements a **data engineering pipeline** that automates the flow of data from **Google Drive** into **Amazon Redshift** for reporting in **Power BI**.  

The pipeline stages raw files in **Amazon S3**, loads them into Redshift (Bronze layer), and applies transformations into **Silver** and **Gold** tables for analytics. The entire workflow is orchestrated with **Apache Airflow**, first tested locally and then deployed on **Amazon MWAA** (Managed Workflows for Apache Airflow).  

The end goal is a **scalable, automated, and production-ready pipeline** that ensures reliable data ingestion, transformation, and reporting.  

---

## 🛠️ Tech Stack  
- **Cloud & Storage**  
  - Google Drive (source files)  
  - Amazon S3 (raw data storage)  

- **Data Warehouse**  
  - Amazon Redshift (Bronze → Silver → Gold layers)  

- **Orchestration**  
  - Apache Airflow (local testing)  
  - Amazon MWAA (production deployment)  

- **Visualization & Reporting**  
  - Power BI  

- **Programming & Tools**  
  - Python (data ingestion, transformation scripts)  
  - PyDrive2 (Google Drive integration)  
  - Boto3 (AWS SDK for Python)  
  - Pandas, PyArrow (data processing)  
  - GitHub Actions (CI/CD, testing & deployment)  
