# 🚀 ETL Pipeline using Databricks, Spark, Python & SQL

## 📌 Overview
This project demonstrates how to build an **ETL (Extract, Transform, Load) pipeline** using **Databricks, PySpark, and SQL**.  
The pipeline processes raw transaction data, performs data cleaning and transformation, and finally loads structured outputs for analysis.

---

## ⚙️ Technologies Used
- Databricks
- Apache Spark (PySpark)
- Python
- SQL

---

## 🔄 ETL Process

### 1️⃣ Extract
- Loaded transaction data into Databricks.
- Handled multiple columns such as **Transaction_ID, Date, Customer_Name, Product, Total_Cost**, etc.

### 2️⃣ Transform
- Performed **data cleaning**:
  - Dropped duplicate rows.  
  - Filtered rows based on conditions (e.g., transactions with `Total_Cost > 97`).  
- Applied **aggregations**:
  - Total items sold per product, city, and year.  
  - Identified most sold products.  

### 3️⃣ Load
- Final transformed data stored into structured DataFrames/tables for querying.
- Executed SQL queries on top of the cleaned dataset for insights.

---

## 📂 Project Files
- `ETL_Pipeline.py` → Main Databricks notebook exported as Python file.  
- (Optional) `ETL_Pipeline_clean.py` → Cleaned version without Databricks system commands (if added).  

---

## 📊 Example Queries
- Top N most expensive products per city.  
- Yearly sales aggregation by product.  
- Customer-wise purchase summaries.  

---

## 🚀 How to Run
1. Clone this repo:  
   ```bash
   git clone https://github.com/arslanaslam-work/ETL-Pipeline.git
