import dlt
from pyspark.sql.functions import col
source_path = "/Volumes/smart_claims_dev/00_landing/sql_server"

def read_csv(file_name):
    return (
        spark.readStream
        .format("cloudFiles")                      # Auto Loader
        .option("cloudFiles.format", "csv")
        .option("header", "true")
        .option("pathGlobFilter", file_name)       # pick one file from the folder
        .load(source_path)
    )


@dlt.table(name="customer", comment="Customers loaded from CSV", table_properties={"quality": "bronze"})
def customer():
    return read_csv("customers.csv")


@dlt.table(name="policy", comment="Policies loaded from CSV", table_properties={"quality": "bronze"})
def policy():
    df = read_csv("policies.csv")
    # CSV headers are uppercase; match the SQL Server table's lowercase names
    return df.select([col(c).alias(c.lower()) for c in df.columns])


@dlt.table(name="claim", comment="Claims loaded from CSV", table_properties={"quality": "bronze"})
def claim():
    # Rename CSV headers to the SQL Server column names the silver code expects
    return (
        read_csv("claims.csv")
        .withColumnRenamed("age", "driver_age")
        .withColumnRenamed("date", "incident_date")
        .withColumnRenamed("hour", "incident_hour")
        .withColumnRenamed("type", "incident_type")
        .withColumnRenamed("severity", "incident_severity")
    )