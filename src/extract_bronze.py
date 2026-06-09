import os
import requests
from dotenv import load_dotenv
load_dotenv("D:\\projects\\stock_analytics\\.env")
import json
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, MapType
from spark_config import get_spark_session

def fetch_stock_data():

    api_key = os.getenv("API_KEY")

    url = f"https://www.alphavantage.co/query?function=TIME_SERIES_MONTHLY&symbol=IBM&apikey={api_key}"


    try:
        response = requests.get(url=url)
        response.raise_for_status()
        data = response.json()
        return data
    except Exception as e:
        print(f"An error occurred: {e}")
        return None

def extract_bronze():

    spark = get_spark_session()

    data = fetch_stock_data()

    if data is None:
        print("❌ Error: API request failed completely or returned invalid data.")
        return
    else:
        print("✅ API request successful. Processing data...")
    
    symbol = data.get("Meta Data", {}).get("2. Symbol", "N/A")
    time_series = data.get("Monthly Time Series", {})

    flattened_data = []
    for date, metrics in time_series.items():
        record = (
            symbol,
            date,
            float(metrics.get("1. open", 0)),
            float(metrics.get("2. high", 0)),
            float(metrics.get("3. low", 0)),
            float(metrics.get("4. close", 0)),
            int(metrics.get("5. volume", 0))
        ) # Create a tuple for each record so that it can be easily converted to a DataFrame
        flattened_data.append(record)
    
    schema = StructType([
        StructField("symbol", StringType(), True),
        StructField("date", StringType(), True),
        StructField("open", DoubleType(), True),
        StructField("high", DoubleType(), True),
        StructField("low", DoubleType(), True),
        StructField("close", DoubleType(), True),
        StructField("volume", IntegerType(), True)
    ])
    
    df = spark.createDataFrame(flattened_data, schema=schema)

    delta_table_path = "D:\\projects\\stock_analytics\\data\\bronze_tables"
    df.write \
        .format("delta") \
        .mode("overwrite") \
        .save(delta_table_path)
    
    print(f"Delta table successfully created at: {delta_table_path}")



if __name__ == "__main__":
    extract_bronze()