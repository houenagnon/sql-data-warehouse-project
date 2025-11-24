# 📦 Modern Data Warehouse – PostgreSQL Edition

## 🧭 Overview

Ce projet met en œuvre un **Data Warehouse Moderne** sur PostgreSQL en utilisant une architecture multi-couches typique : **Bronze → Silver → Gold**.

L’objectif est de démontrer comment construire un pipeline ETL complet incluant :

* ingestion de données brutes
* transformations
* normalisation
* modélisation dimensionnelle
* génération de vues analytiques
* bonnes pratiques qualité

Ce projet reproduit le pipeline classique MS SQL Server → mais adapté proprement au monde PostgreSQL.

---

## 🏛 Architecture

```
+--------------------+
|   Source Files     |  CSV
+---------+----------+
          |
          v
+--------------------+
|     BRONZE Layer   |
|  (raw structured)  |
+---------+----------+
          |
          v
+--------------------+
|     SILVER Layer   |
|   (cleaned data)   |
+---------+----------+
          |
          v
+--------------------+
|      GOLD Layer    |
|  (dimensional BI)  |
+--------------------+
```

---

## 🛠️ Tech Stack

|    Component | Tool                       |
| -----------: | :------------------------- |
|     Database | **PostgreSQL**             |
| Orchestrator | PL/pgSQL Stored Procedures |
|    Analytics | pgAdmin / SQL              |
|  Data Format | CSV                        |
|      Schemas | bronze, silver, gold       |

---

## 📂 Repository Structure

```
/data
    /source_crm
    /source_erp
/sql
    bronze_tables.sql
    bronze_loader.sql
    silver_tables.sql
    silver_loader.sql
    gold_views.sql
README.md
```

---

## 📥 Data Sources

Input located in:

```
data/source_crm/
data/source_erp/
```

Format:

* `.csv`
* headers included
* comma separated

---

## 🗄 Database Setup

### 1. Create database

```sql
DROP DATABASE IF EXISTS "DataWarehouse";
CREATE DATABASE "DataWarehouse";
```

### 2. Create schemas

```sql
CREATE SCHEMA bronze;
CREATE SCHEMA silver;
CREATE SCHEMA gold;
```

---

## 🧱 Layer – Bronze

### Tables

Located in:

```
sql/bronze_tables.sql
```

### Data loading procedure

Loads CSV → Bronze

```sql
CALL bronze.load_bronze();
```

(function is implemented in PL/pgSQL)

---

## 🧼 Layer – Silver

Silver cleans + standardizes:

* sexes
* dates
* strings
* categories
* costs
* data consistency
* removes duplicates

### Create tables

```
sql/silver_tables.sql
```

### Transformation procedure

```
CALL silver.load_silver();
```

---

## 🟨 Layer – Gold (Dimensional)

Designed following star-schema concepts:

### Views

```
gold.dim_customers
gold.dim_products
gold.fact_sales
```

Created by:

```sql
sql/gold_views.sql
```

---

## 🔍 Data Quality Checks

Validated aspects:

* duplicates
* nullness
* unwanted spaces
* invalid dates
* referential join quality
* mapping consistency

Queries located in:

```
sql/silver_quality_checks.sql
```

---

## 📊 Example Queries

```sql
SELECT * FROM gold.fact_sales LIMIT 100;
SELECT country, COUNT(*) FROM gold.dim_customers GROUP BY 1;
SELECT product_line, SUM(sales_amount) FROM gold.fact_sales GROUP BY 1;
```

---

## 🧪 Expected Outputs

* bronze → equal to source files
* silver → conform, standardized
* gold → dimensional semantic layer ready for BI

---

## 🏁 Final Result

You get:

✔ A functioning Data Warehouse
✔ A clean ELT flow
✔ Clean normalized customer / product data
✔ Dimensional-ready analytics layer
✔ High-quality SQL transformation logic

---

## 🚀 Improvements (future)

* dbt migration
* Airflow orchestration
* Dockerized PostgreSQL
* dashboard (Metabase / PowerBI)
* CDC / incremental load
* slowly changing dimensions
* quality scoring indicators

---

