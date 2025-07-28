
# 🇵🇦 Panama Government Payroll Data Ingestion and Analysis

This project automates the ingestion, transformation, and exploration of payroll data published by the **Contraloría General de la República de Panamá**. It includes:

- A Python script (`ingestion.py`) that downloads the latest Excel reports from the government portal.
- A Jupyter notebook (`Contraloria.ipynb`) for exploratory data analysis and enrichment.

---

## 🧩 Project Structure

```
├── ingestion.py              # Automation script to ingest and store new government payroll reports
├── Contraloria.ipynb         # Notebook for exploratory analysis and enrichment
├── /staging                     # Folder where newly ingested Parquet files are saved
├── last_updated_date.txt     # Stores last successful update timestamp
```

---

## ⚙️ ingestion.py - Data Ingestion Pipeline

This script performs the following steps:

### ✅ Key Features

- **Detects Updates Automatically**  
  Uses a regular expression to detect the "Last update" timestamp published on the official website. Skips execution if no new data is available.

- **Scrapes Institution List**  
  Retrieves the dropdown list of government institutions available on the portal using BeautifulSoup.

- **Downloads Excel Reports**  
  Iterates through each institution, sends a request to download their payroll report, and parses the resulting Excel file.

- **Cleans and Standardizes Data**  
  Cleans up rows and columns, renames fields, infers schema, and casts data types. Converts columns like `fecha_actualizacion`, `fecha_consulta`, and `fecha_de_inicio` to datetime.

- **Exports as Parquet**  
  Saves cleaned data to `/staging/` folder in Parquet format using timestamped filenames for traceability.

- **Error Handling**  
  Collects any failed downloads and retries them.

### 🧪 How to Run

```bash
python ingestion.py
```

> ⚠️ Requires an active internet connection and access to the public site: https://www.contraloria.gob.pa/CGR.PLANILLAGOB.UI/Formas/Index

---

## 📓 Contraloria.ipynb - Exploratory Analysis

This notebook is used to:

- Load and consolidate all Parquet files from the `/new/` folder.
- Parse and display employee payroll information over time.
- Track salary changes, employee movements, and institutional changes.
- Prepare data for additional transformations (e.g., SCD Type 2 modeling).

### Sample Activities in the Notebook

- Load all Parquet files into a single DataFrame.
- Plot salary distributions and employee counts per institution.
- Detect duplicates, missing data, and structural changes.
- Aggregate payroll expenditures over time.

---

## 🧰 Requirements

Make sure to install the following dependencies (used in script and notebook):

```bash
pip install pandas openpyxl beautifulsoup4 requests
```

Optional for the notebook:

```bash
pip install matplotlib seaborn plotly
```

---

## 🔐 Notes

- The script disables SSL verification (`verify=False`) due to the configuration of the official website. Use with caution.
- Stored data includes employee names, salary, ID numbers (cedula), and work history — ensure privacy and compliance when sharing or publishing results.

---

## 📅 Update Tracking

The script creates/updates a local file `last_updated_date.txt` to store the timestamp of the last successful ingestion. It prevents redundant downloads and ensures only new data is ingested.

---

## 👨‍💻 Author

Developed by [@jquesada92](https://github.com/jquesada92)  
This project is a public data initiative for transparency and analysis.
