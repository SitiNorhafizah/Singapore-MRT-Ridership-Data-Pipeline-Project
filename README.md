# 🚇 Singapore MRT Data Pipeline Project

This project demonstrates a complete end-to-end data engineering pipeline for Singapore MRT data.  
It showcases how data can be ingested, transformed, and visualized using modern open-source tools.

---

## 📘 Project Summary

The project simulates real-time MRT data ingestion from JSON files, processes and transforms the data using Hive, and visualizes key insights through Superset dashboards.  
Prefect is included as a demonstration of ETL orchestration and automation.

---

## 🧰 Tools & Technologies

- **Kafka** – Data ingestion and streaming  
- **MinIO** – Object storage for raw data  
- **Hive** – Data transformation and warehousing  
- **Superset** – Data visualization and analytics  
- **Prefect** – Workflow orchestration (demo)  
- **Docker Compose** – Service containerization and orchestration  

---

## 🧩 Project Workflow

1. Ingest MRT JSON data into Kafka.  
2. Store raw data in MinIO (Bronze layer).  
3. Process data in Hive (Silver and Gold layers).  
4. Visualize insights in Superset dashboards.  
5. Automate ETL runs using Prefect (demo purpose).

---
## 📂 Repository Structure

mrt_project/
├── producers/ # Kafka producers for MRT JSON data
├── hive/ # Hive SQL transformation scripts
├── prefect/ # Prefect demo ETL workflow
├── superset_exports/ # Superset dashboard exports
├── docker-compose.yml # Containerized service setup
└── README.md


---

## 👩‍💻 Author

**Fiza Rooslan**  
Aspiring Data Engineer | Singapore  
Building scalable ETL pipelines and real-time analytics solutions.

