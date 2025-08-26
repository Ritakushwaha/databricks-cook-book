# Databricks notebook source
# MAGIC %md
# MAGIC # Databricks Delta Table Hands-on Demo
# MAGIC
# MAGIC This notebook demonstrates how to:
# MAGIC - Create a table in Databricks using the Hive Metastore.
# MAGIC - Insert and update records.
# MAGIC - Explore table metadata.
# MAGIC - Review table history and Delta log files.
# MAGIC
# MAGIC We’ll use the `employees` table.

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Set the catalog to use Hive Metastore
# MAGIC USE CATALOG hive_metastore;

# COMMAND ----------

# MAGIC %md
# MAGIC We set the catalog to `hive_metastore` so that the table we create is stored in the default Hive-managed catalog.

# COMMAND ----------

# MAGIC %sql
# MAGIC --cleanup
# MAGIC DROP TABLE IF EXISTS employees;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Create a Delta table named 'employees'
# MAGIC CREATE TABLE employees
# MAGIC (id INT, name STRING, salary DOUBLE);

# COMMAND ----------

# MAGIC %md
# MAGIC We create the `employees` table with 3 columns:
# MAGIC - **id**: integer unique identifier
# MAGIC - **name**: employee name
# MAGIC - **salary**: employee salary (double precision)

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Insert initial employee records
# MAGIC INSERT INTO employees
# MAGIC VALUES (1, 'John', 100000),
# MAGIC        (2, 'Jane', 120000),
# MAGIC        (3, 'Bob', 80000),
# MAGIC        (4, 'Alice', 90000),
# MAGIC        (5, 'Mary', 110000);
# MAGIC
# MAGIC -- Insert duplicate employees for testing
# MAGIC INSERT INTO employees VALUES (6, 'John', 100000);
# MAGIC INSERT INTO employees VALUES (7, 'Jane', 120000);

# COMMAND ----------

# MAGIC %md
# MAGIC We insert 5 employees first, then add 2 duplicate records intentionally:
# MAGIC - Another **John** with salary 100000.
# MAGIC - Another **Jane** with salary 120000.  
# MAGIC
# MAGIC This helps demonstrate how Delta Lake maintains history even when duplicates are inserted.

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Query all data
# MAGIC SELECT * FROM employees;
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC We query all rows to verify that data was inserted successfully.

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Describe detailed metadata of the Delta table
# MAGIC DESCRIBE DETAIL employees;
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC `DESCRIBE DETAIL` provides table-level metadata, including:
# MAGIC - Location on DBFS.
# MAGIC - Format (`delta`).
# MAGIC - Creation and modification time.
# MAGIC - Number of files and size on disk.

# COMMAND ----------

# List physical files of the Delta table in DBFS
dbutils.fs.ls('dbfs:/user/hive/warehouse/employees')

# COMMAND ----------

# MAGIC %fs ls 'dbfs:/user/hive/warehouse/employees' 

# COMMAND ----------

# MAGIC %md
# MAGIC The Delta table is stored in DBFS at:
# MAGIC ```dbfs:/user/hive/warehouse/employees```
# MAGIC This directory contains data files and a `_delta_log` folder for transaction history.
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Apply an update (increase salary of 'John' by 10%)
# MAGIC UPDATE employees
# MAGIC SET salary = salary * 1.1
# MAGIC WHERE name = 'John'

# COMMAND ----------

# MAGIC %md
# MAGIC We use an `UPDATE` statement to increase the salary of all employees named **John** by 1.1.  
# MAGIC Delta Lake supports ACID transactions, so this update is versioned and logged.

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from employees

# COMMAND ----------

# MAGIC %md
# MAGIC After the update, John's salary should increase from **100000 → 110000.00000000001**.  
# MAGIC Both rows for John will reflect the updated salary.

# COMMAND ----------

# MAGIC %fs ls 'dbfs:/user/hive/warehouse/employees' 

# COMMAND ----------

# MAGIC %md
# MAGIC Notice that new Parquet files are written when updates occur, instead of modifying files in place.  
# MAGIC This is **copy-on-write** behavior of Delta Lake.

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Get updated table metadata
# MAGIC DESCRIBE DETAIL employees

# COMMAND ----------

# MAGIC %md
# MAGIC We can see table details again. The `lastModified` timestamp will change after the update.

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Query the Delta Lake transaction history
# MAGIC DESCRIBE HISTORY employees;

# COMMAND ----------

# MAGIC %md
# MAGIC The `DESCRIBE HISTORY` command shows:
# MAGIC - Each transaction committed.
# MAGIC - User performing the operation.
# MAGIC - Operation type (CREATE, INSERT, UPDATE).
# MAGIC - Version number of the table.

# COMMAND ----------

# MAGIC %fs ls 'dbfs:/user/hive/warehouse/employees/_delta_log'

# COMMAND ----------

# MAGIC %md
# MAGIC The `_delta_log` folder contains JSON and checkpoint files that track transaction history.  
# MAGIC Each JSON file corresponds to a transaction, numbered sequentially (e.g., `00000000000000000005.json`).

# COMMAND ----------

# MAGIC %fs head 'dbfs:/user/hive/warehouse/employees/_delta_log/00000000000000000004.json'

# COMMAND ----------

# MAGIC %md
# MAGIC This JSON file shows low-level Delta Lake transaction details:
# MAGIC - Schema updates.
# MAGIC - File additions and removals.
# MAGIC - Commit metadata (e.g., user, operation type, cluster ID).
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC # Summary
# MAGIC
# MAGIC In this notebook, we:
# MAGIC 1. Created a Delta table using Hive Metastore.
# MAGIC 2. Inserted records (including duplicates).
# MAGIC 3. Explored data and metadata (`DESCRIBE DETAIL`, DBFS listing).
# MAGIC 4. Performed an **UPDATE** operation on specific rows.
# MAGIC 5. Queried the updated results.
# MAGIC 6. Viewed **transaction history** with `DESCRIBE HISTORY`.
# MAGIC 7. Inspected the **_delta_log** folder to understand Delta Lake’s transaction logging.
# MAGIC
# MAGIC This demonstrates how Delta Lake ensures **ACID compliance, schema enforcement, and time travel capabilities** for tables in Databricks.
