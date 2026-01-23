# Live Pokémon Data Pipeline ⚡🐉

A robust end-to-end data engineering project designed to ingest, store, and transform live Pokémon data.

**Project Status:** ✅ Complete  (although i am fixing bugs that pop up here and there)
**Primary Focus:** Advanced SQL Server Development & Azure Cloud Integration

## 📖 About The Project

I built this project to move beyond basic SQL queries and master the intricacies of **Data Warehousing** and **Modern Data Pipelines**. 

While many data projects rely heavily on Python for transformation, I intentionally pushed the logic down into the database layer to deepen my understanding of **SQL Server**. The goal was to take raw, semi-structured JSON data from an API and architect a clean, relational schema using advanced parsing techniques and Medallion Architecture principles.

## 🏗️ Architecture & Workflow

The pipeline follows a standard extract-load-transform (ELT) pattern:

1.  **Ingestion:** An Azure Function (Python) triggers the extraction of live data from the Pokémon API (daily at 2AM).
2.  **Data Lake:** Raw JSON files are stored immediately in **Azure Blob Storage** (JSON).
3.  **Database Connection:** My Azure SQL Database connects directly to Blob Storage to read the raw files (python calls SQL Server immediately after all cards are complete).
4.  **Transformation (The Core):**
    * **Bronze Layer:** Dynamic loading of raw JSON (use sp_executesql and REPLACE w/ GETDATE in order to get data from today's json (which was just uploaded to blob).
    * **Silver/Gold Layers:** Parsing JSON into relational tables, applying surrogate keys, and enforcing data types.

Here is the Data Architecture plan i started with:
(![DATA LAYERS](https://github.com/user-attachments/assets/ac6e8218-8802-4fa5-ab86-fc167b22f5a4)

*(See `images/` folder  to see what my complete data flow (simple and complex) ended up looking like 😂)*

## 🧠 Key Learnings & Highlights

### 1. Advanced SQL & JSON Parsing
This project relies heavily on `CROSS APPLY` and `OPENJSON` to flatten complex, nested data structures.
* **Deep Dive into `CROSS APPLY`:** I used this extensively to join parent rows with their child JSON arrays, allowing for the "exploding" of nested data into distinct rows.
* **`OPENJSON` Mastery:** I learned to formulate complete SQL tables directly from JSON files. This involved:
    * Extracting `[Key]` and `[Value]` pairs.
    * Using JSON path expressions like `$.[name]` to map specific JSON attributes to my own naming nomenclature.
* **Dynamic Bronze Layer:** I implemented logic to dynamically load data into the Bronze layer, ensuring the pipeline is resilient to changes in the raw data stream.

### 2. Azure Integration
* **Blob Storage Connectivity:** I learned the basics of securing and connecting an Azure SQL Database to Azure Blob Storage, allowing T-SQL to query files directly from the data lake as if they were tables.

### 3. Data Modeling
* **Surrogate Keys:** I deepened my understanding of *when* and *why* to use surrogate keys versus natural keys, implementing them to ensure stable relationships between the data layers.

## 🤖 Attribution & AI Usage

Transparency regarding the development process:

* **Python Function App:** The Python script used for the Azure Function (API extraction) was **100% written by AI**. My focus was on the data engineering and database side, so I automated the ingestion script to focus on the SQL challenges.
* **SQL Development:** The SQL codebase (90% of the project) was **hand-written by me**, the other 10% was stuff that i already knew how to do and was repetitive so i fed it into AI.
    * *Note:* I used AI as a "sounding board" to brainstorm solutions for complex problems, such as handling dynamic JSON loading and understanding the nuances of `CROSS APPLY`.

## 📂 Project Structure

```text
├── .github/              # Workflows and actions
├── scripts/
│   ├── python/     # Python code for Azure Functions (AI Generated)
│   └── sql/              # T-SQL Scripts (Stored Procs, DDL, DML)
├── docs/
│   └── diagrams/         # Draw.io exports of Data Lineage & ERD
└── README.md
