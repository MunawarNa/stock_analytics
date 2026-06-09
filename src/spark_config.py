import os
import sys
from pathlib import Path

# Get the path to your current virtual environment dynamically
venv_path = str(Path(sys.executable).parent.parent)

# Force Spark to use the virtual environment's Python executable
os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable

# 2. CRITICAL FIX: Force Spark to use the virtual environment's PySpark Java files
os.environ["SPARK_HOME"] = os.path.join(venv_path, "Lib", "site-packages", "pyspark")

# Hadoop Home Patch for Windows
os.environ["HADOOP_HOME"] = "C:/Users/Nour/hadoop"
# Append the bin directory to the execution path so Spark can find hadoop.dll
os.environ["PATH"] += os.pathsep + "C:\\Users\\Nour\\hadoop\\bin"

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, MapType

def get_spark_session():

    spark = SparkSession.builder \
        .appName("Stock Data Analytics") \
        .config("spark.jars.packages", "io.delta:delta-spark_2.12:3.2.0") \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
        .config("spark.databricks.delta.retentionDurationCheck.enabled", "false") \
        .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")

    return spark