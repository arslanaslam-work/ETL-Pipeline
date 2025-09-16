# Databricks notebook source
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql.window import Window
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

# COMMAND ----------

df = spark.read.format('csv')\
     .option('inferschema', True)\
     .option('header', True)\
     .load('/Volumes/workspace/schemaforpipeline/datasetvolume/Retail_Transactions_Dataset.csv')        

# COMMAND ----------

df.printSchema()

# COMMAND ----------

df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Data Cleaning

# COMMAND ----------

# Droping Duplicates
df.dropDuplicates().display()

# COMMAND ----------

df.count()

# COMMAND ----------

df.dropDuplicates().count()
# Since no Duplicate Rows, so we Don't need to do anything

# COMMAND ----------

# Checking Rows where 'Total_Cost' is near its maximum value
df.filter(col('Total_Cost') > 97).display()

# COMMAND ----------

# Checking Rows for Maximum Values for 'Total_Items'
max_val = df.agg(F.max('Total_Items')).collect()[0][0]

df.filter(col('Total_Items') == max_val).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Data Transformation

# COMMAND ----------

# Extracting Year, Month, Day from 'Date'
df.withColumn('Year', year(col('Date')))\
  .withColumn('Month', month(col('Date')))\
  .withColumn('Day', dayofmonth(col('Date')))\
  .display()          

# COMMAND ----------

# Reducing 'Total_points' at Point where Discount_Applied = TRUE
df = df.withColumn('Total_Cost',
  when(col('Discount_Applied') == True, col('Total_Cost') - ( col('Total_Cost') / 100 ) * 15)\
  .otherwise(col('Total_Cost'))
)

# COMMAND ----------

# Confirming Changes
df.display()

# COMMAND ----------

# Checking Unique Values for 'Payment_Method'
df.select('Payment_Method').dropDuplicates().display()

# COMMAND ----------

# Categorizing Payment_Method into Online/Offline Method.
df = df.withColumn('Transaction_Method',
  when( (col('Payment_Method') == 'Debit Card') | (col('Payment_Method') == 'Credit Card') | (col('Payment_Method') == 'Mobile Payment'), 'Online')             
  .when(col('Payment_Method') == 'Cash', 'Offline')
  .otherwise(col('Payment_Method'))            
)

# COMMAND ----------

# Confirming Changes
df.display()

# COMMAND ----------

df.select('Customer_Category').dropDuplicates().display()

# COMMAND ----------

# Standardizing Customer_Category by creating new Column 'Customer Status'
df = df.withColumn('Customer Status',
  when( (col('Customer_Category') == 'Teenager') | (col('Customer_Category') == 'Homemaker') | (col('Customer_Category') == 'Professional') | (col('Customer_Category') == 'Middle-Aged') | (col('Customer_Category') == 'Student'), 'New')\
  .when( (col('Customer_Category') == 'Senior Citizen') | (col('Customer_Category') == 'Young Adult') | (col('Customer_Category') == 'Retiree'), 'Returning')         
)

# COMMAND ----------

# Confirming Changes
df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Exploratory Analysis

# COMMAND ----------

# Top selling Products sorted by Total_Items
df.groupBy('Product')\
  .agg(sum('Total_Items').alias('Total_Items_Sold'))\
  .orderBy(desc('Total_Items_Sold'))\
  .display()

# COMMAND ----------

df.display()

# COMMAND ----------

df.createOrReplaceTempView('temp_view')

# COMMAND ----------

# Which City Generate the most Sales?
spark.sql('''
  SELECT
  City,
  SUM(Total_Cost) AS MaxCost
  FROM temp_view
  GROUP BY City
  ORDER BY MaxCost DESC
  LIMIT 1;           
''').display()

# COMMAND ----------

df.select(col('Season')).dropDuplicates().display()

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Sorting Seasons by Highest Total Items Sold 
# MAGIC SELECT Season, SUM(Total_Items) AS TotalItemsSold
# MAGIC FROM temp_view
# MAGIC GROUP BY Season
# MAGIC ORDER BY TotalItemsSold DESC

# COMMAND ----------

df.select(col('Promotion')).dropDuplicates().display()

# COMMAND ----------

# Effect of Promotion on Sales
df = df.withColumn('Total_Items',
  when(col('Promotion') != 'None', col('Total_Items') + 2)\
  .otherwise(col('Total_Items'))    
)

# COMMAND ----------

# Confirming Changes
df.display()

# COMMAND ----------

df.select(col('Store_Type')).dropDuplicates().display()

# COMMAND ----------

# Sorting Data in Terms of Transaction Date and only including those Customers which are Professional for deep analysis
df.select('Transaction_ID','Customer_Name', 'Date', 'Store_Type', 'Customer_Category')\
  .orderBy(col('Date').asc())\
  .filter(col('Customer_Category') == 'Professional')\
  .display()

# COMMAND ----------

# Calculating Total_Items and Total_Cost for each Payment_method to see which Payment_Method is the most used and which one is less used
df.groupBy('Payment_Method')\
  .agg(
    sum(col("Total_Items")).alias('Total_Items_Sum'),
    sum(col("Total_Cost")).alias('Total_Cost_Sum')
  )\
  .orderBy('Total_Items_Sum', 'Total_Cost_Sum')\
  .display()

# COMMAND ----------

#  Calculating Cumulative Sum of Total_Items and Total_Cost for each Payment_method to see the effect of Payment Method on Sales
df.withColumn('Total_Items_Sum',
  sum(col('Total_Items')).over(Window.partitionBy('Payment_Method').orderBy('Payment_Method').rowsBetween(Window.unboundedPreceding, Window.currentRow)))\
  .withColumn('Total_Cost_Sum',
  sum(col('Total_Cost')).over(Window.partitionBy('Payment_Method').orderBy('Payment_Method').rowsBetween(Window.unboundedPreceding, Window.currentRow)))\
 .display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Intermediate Transformations

# COMMAND ----------

df.withColumn('Product_Array', 
             split(regexp_replace(col('Product'), r'[\[\]\']', ''), ', '))\
  .withColumn('Product1', explode(col('Product_Array')))\
  .display()

# COMMAND ----------

# Getting each Product separately from 'Products' and storing dataset in a separate dataframe
temp_df = df.withColumn('Product_Array', 
             split(regexp_replace(col('Product'), r'[\[\]\']', ''), ', '))\
  .withColumn('Product1', explode(col('Product_Array')))

# COMMAND ----------

# Creaing a Separate Temporary View for the above dataframe
temp_df.createOrReplaceTempView('temp_df_view')

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Top 3 Products sold(Per City)
# MAGIC SELECT City, Product1, TotalSold
# MAGIC FROM (
# MAGIC     SELECT 
# MAGIC         City,
# MAGIC         Product1,
# MAGIC         SUM(Total_Items) AS TotalSold,
# MAGIC         ROW_NUMBER() OVER (PARTITION BY City ORDER BY SUM(Total_Items) DESC) AS rn
# MAGIC     FROM temp_df_view
# MAGIC     GROUP BY City, Product1
# MAGIC ) 
# MAGIC WHERE rn <= 3
# MAGIC ORDER BY City, TotalSold DESC;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Checking 'Total Items Sold' for each product Per City and ranking them
# MAGIC SELECT
# MAGIC   City, Product, SUM(Total_Items) AS TotalItemsSold,
# MAGIC   ROW_NUMBER() OVER(PARTITION BY City ORDER BY SUM(Total_Items) DESC) AS CumulativeCounting
# MAGIC   FROM temp_view
# MAGIC   GROUP BY City, Product;

# COMMAND ----------

temp_df.display()

# COMMAND ----------

# Calculating Total Items Sold for each Product in each City Per Year
agg_df = temp_df.groupBy('City', "Date", "Product1")\
       .agg(sum(col('Total_Items')).alias('TotalItemsSold'))
       

agg_df.select('Date', 'City', 'Product1', 'TotalItemsSold',
       row_number().over(Window.partitionBy(year('Date')).orderBy(col('TotalItemsSold').desc()))\
       .alias('CountPerYear'))\
.display()       

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Top 2 most expensive products per city, based on Total_Cost.
# MAGIC WITH counting AS (
# MAGIC   SELECT
# MAGIC     City,
# MAGIC     Product1,
# MAGIC     SUM(Total_Items) AS TotalItemsSold,
# MAGIC     RANK() OVER (PARTITION BY City ORDER BY SUM(Total_Items) DESC) AS RankedCounting,
# MAGIC     DENSE_RANK() OVER (PARTITION BY City ORDER BY SUM(Total_Items) DESC) AS DesneRankedCounting
# MAGIC   FROM temp_df_view
# MAGIC   GROUP BY City, Product1    
# MAGIC )
# MAGIC
# MAGIC SELECT * FROM counting
# MAGIC WHERE (RankedCounting <= 2) AND (DesneRankedCounting <= 2);