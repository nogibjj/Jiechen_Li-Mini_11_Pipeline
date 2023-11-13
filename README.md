[![CI](https://github.com/nogibjj/Jiechen_Li-Mini_11_Pipeline/actions/workflows/ci.yml/badge.svg)](https://github.com/nogibjj/Jiechen_Li-Mini_11_Pipeline/actions/workflows/ci.yml)

## Jiechen_Li_Mini_11_Pipeline

### Purpose

* Create a data pipeline using Databricks.
* Include at least one data source and one data sink.

### Dataset

The dataset is sellcted from [DATA.GOV](https://catalog.data.gov/dataset/school-attendance-by-student-group-and-district-2021-2022/resource/d923f39c-c84c-4fa9-a252-c1f6b465bd55) in the United States.
The dataset appears to represent attendance data for various student groups across different districts in Connecticut for the academic years 2021-2022, 2020-2021, and 2019-2020.

### Databricks Notebook Performs ETL

1. **Extract: Read the CSV file from Azure Blob storage.**

```python
# Import necessary libraries
from pyspark.sql import SparkSession
from pyspark.sql.functions import *

# Create Spark session
spark = SparkSession.builder.appName("School Attendance ETL").getOrCreate()

# Set up the Blob storage account access key
spark.conf.set("fs.azure.account.key.<individual3>.blob.core.windows.net", "<RvtG/R+z25TAAm6LdgMMfxV9zJT/4A20TYb2O30T+EeliwJFKoONpPZvQt25WHFP9fTVBcbmInd5+AStlw2TzA==>")

# Read the CSV file from Blob storage
storage_account_name = "individual3"
storage_account_access_key = "RvtG/R+z25TAAm6LdgMMfxV9zJT/4A20TYb2O30T+EeliwJFKoONpPZvQt25WHFP9fTVBcbmInd5+AStlw2TzA=="

spark.conf.set(
  f"fs.azure.account.key.{storage_account_name}.blob.core.windows.net",
  storage_account_access_key)

csv_file_path = "wasbs://schoolattendance@individual3.blob.core.windows.net/School_Attendance_by_Student_Group_and_District__2021-2022.csv"
df = spark.read.csv(csv_file_path, header=True, inferSchema=True)
df.show()

```

2. **Transform: Perform the necessary transformations on the data.**

```python
# Transform Data Using SQL
# Register the DataFrame as a temp view
df.createOrReplaceTempView("School_Attendance_Table")

# Example Transformation: Create new table with specific columns
transformed_df = spark.sql("""
    SELECT 
        "District code" AS District_code, 
        "District name" AS District_name, 
        Category, 
        "Student group" AS Student_group, 
        "2021-2022_student_count_-_year_to_date" AS Student_count, 
        "2021-2022_attendance_rate_-_year_to_date" AS Attendance_rate 
    FROM School_Attendance_Table
""")
transformed_df.show()

```

3. **Load: Save the transformed data back to Azure Blob storage, potentially as a Delta Lake table for efficient querying and storage.**

```python
# Load Transformed Data into Delta Lake
# Define the path to store the Delta table
delta_table_path = "/mnt/delta/School_Attendance_Transformed"

# Write the DataFrame as a Delta table
transformed_df.write.format("delta").mode("overwrite").save(delta_table_path)

# Read from Delta Lake
delta_df = spark.read.format("delta").load(delta_table_path)
delta_df.show()
```

Please check ``ETL_pipeline_within_repo.py`` for detailed output.

### Results

1. **Connect GitHub Repo with Databricks Repo**
<img decoding="async" src="connect_github_databricks_repo.png" width="85%"><br>  

2. **Transformed data in Delta Lake to Interact with Spark SQL**
<img decoding="async" src="delta_lake_with_Spark.png" width="85%"><br/>

### Reference

Please click <a href="https://github.com/nogibjj/Jiechen_Li_Individual_3.git" target="_blank">here</a> to see the template of this repo.
