# Airflow Dataproc PySpark Orders Backfilling

This Project demonstrates how to orchestrate **date-based backfilling of orders data** using **Apache Airflow (2.x)** and **PySpark on an existing Google Cloud Dataproc cluster**.

The pipeline is designed for **manual and ad-hoc execution**, enabling safe reprocessing of historical data without creating or deleting Dataproc clusters.

---

## 🚀 Key Features

- Runs PySpark jobs on an existing Dataproc cluster
- Supports date-partitioned backfilling
- Manual / on-demand execution
- Uses Airflow params for runtime configuration
- Clean and modern Airflow 2.x DAG syntax
- Production-ready orchestration pattern

---

## 🛠 Tech Stack

- Apache Airflow 2.x
- Google Cloud Dataproc
- PySpark
- Google Cloud Storage (GCS)

---

## 📁 Project Structure

.
├── dags/
│   └── orders_backfilling_dag.py
├── spark/
│   └── orders_data_process.py
├── configs/
│   └── airflow_variables_example.json
└── README.md

---

## 📦 Test Data

This project uses date-partitioned CSV files as test input data.

Sample test files:

orders_20251229.csv  
orders_20251230.csv  
orders_20251231.csv  

Each file contains order records for a single business day.

---

## 📌 File Naming Convention

orders_<YYYYMMDD>.csv

Example:

orders_20251230.csv → Orders for 30-Dec-2025

---

## 📍 Expected GCS Input Structure

gs://airflow-projects-de/airflow-project-02/input/orders/

orders_20251229.csv  
orders_20251230.csv  
orders_20251231.csv  

---

## ▶️ Execution Date Handling

The Airflow DAG accepts an optional parameter:

{
  "execution_date": "20251230"
}

Resolution logic:

1. If execution_date is provided, it is used directly
2. If not provided, the DAG falls back to Airflow execution date (ds_nodash)

---

## 🔁 Pipeline Execution Flow

Airflow DAG  
→ Resolve execution_date  
→ Submit PySpark job to Dataproc  
→ Read orders_<execution_date>.csv from GCS  
→ Apply transformations  
→ Write processed output  

---

<img width="1726" height="1043" alt="image" src="https://github.com/user-attachments/assets/1edee623-145f-49e2-9e77-489eb7f88f86" />


<img width="2930" height="1756" alt="image" src="https://github.com/user-attachments/assets/d330798c-8b33-4886-ba80-2895a27dfcae" />


## 🧠 Spark Job Input Logic (Conceptual)

execution_date = args.date  
input_file = f"orders_{execution_date}.csv"  
input_path = gs://.../input/orders/input_file  


<img width="3440" height="1922" alt="image" src="https://github.com/user-attachments/assets/d6f59061-9232-4a67-8b79-0b19e692ff1d" />


---

## 📤 Output Structure (Example)

gs://airflow-projects-de/airflow-project-02/output/orders/date=20251230/

<img width="3440" height="1788" alt="image" src="https://github.com/user-attachments/assets/0ced00a0-2b7b-41f1-9c13-52d20f43eb0c" />

---

## ▶️ Running the DAG

Trigger the DAG manually from the Airflow UI.

Example DAG run configuration:

{
  "execution_date": "20251230"
}

If the parameter is omitted, the DAG automatically processes data for the DAG execution date.

---

## ⚙️ Airflow Variable Configuration

Create an Airflow Variable named cluster_details:

{
  "CLUSTER_NAME": "your-dataproc-cluster-name",
  "PROJECT_ID": "your-gcp-project-id",
  "REGION": "us-central1"
}

---

## 🧪 Sample CSV Format

order_id,customer_id,order_date,amount,status  
1001,C123,2025-12-30,150.75,COMPLETED  
1002,C456,2025-12-30,89.99,PENDING  

---

## ⚠️ Assumptions & Notes

- Dataproc cluster is already running
- Cluster lifecycle is intentionally excluded
- CSV schema is consistent across all dates
- Files strictly follow the naming convention
- Airflow has permission to access GCS and Dataproc

---

## 📈 Use Cases

- Historical orders backfilling
- Reprocessing failed dates
- Data corrections
- Month-end reconciliation

---

## 📄 License

MIT License
