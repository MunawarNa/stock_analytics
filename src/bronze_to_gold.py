from spark_config import get_spark_session


def yearly_avrage(df_bronze, spark):
    

    
    df_bronze.createOrReplaceTempView("stock_data")
    
    query = ("""
                SELECT 
                    symbol,
                    EXTRACT(year FROM date) AS year,
                    ROUND(AVG(open), 2) AS avg_open,
                    ROUND(AVG(high), 2) AS avg_high,
                    ROUND(AVG(low), 2) AS avg_low,
                    ROUND(AVG(close), 2) AS avg_close,
                    SUM(volume) AS sum_volume
                FROM stock_data
                GROUP BY symbol, EXTRACT(year FROM date)
                ORDER BY year DESC
              """)
    df_yearly_avg = spark.sql(query)

    return df_yearly_avg

def yearly_close_extremes(df_bronze, spark):
    

    df_bronze.createOrReplaceTempView("stock_data")

    query = ("""
                SELECT
                    symbol,
                    EXTRACT(year FROM date) AS year,
                    MAX(close) AS max_close,
                    MIN(close) AS min_close
                FROM stock_data
                GROUP BY symbol, EXTRACT(year FROM date)
                ORDER BY year DESC
            """)

    df_yearly_close_extremes = spark.sql(query)

    return df_yearly_close_extremes

def main():
    spark = get_spark_session()

    print("Reading raw Bronze data once...")
    df_bronze = spark.read \
        .format("delta") \
        .load("D:\\projects\\stock_analytics\\data\\bronze_tables")

    print("Executing SQL analytics transformations...")
    df_yearly_avg = yearly_avrage(df_bronze, spark)
    df_yearly_close_extremes = yearly_close_extremes(df_bronze, spark)

    print("Writing outputs to separate Gold destinations...")
    df_yearly_avg.write \
        .format("delta") \
        .mode("overwrite") \
        .save("D:\\projects\\stock_analytics\\data\\gold_tables\\yearly_avg")

    df_yearly_close_extremes.write \
        .format("delta") \
        .mode("overwrite") \
        .save("D:\\projects\\stock_analytics\\data\\gold_tables\\yearly_close_extremes")

    print("✅ Gold layers built successfully!")

if __name__ == "__main__":
    main()
