# Databricks notebook source
landing_catalog = "smart_claims_dev"
landing_schema = "00_landing"

base_path = f"/Volumes/{landing_catalog}/{landing_schema}/claims"
source_path = f"{base_path}/images"
archive_path = f"{base_path}/archive"
metadata_path = f"{base_path}/autoloader_metadata"

archive_configs = {
    "cloudFiles.cleanSource": "MOVE",                        # or DELETE
    "cloudFiles.cleanSource.retentionDuration": "1 minute",  # MOVE after 1 min (DELETE needs min. 7 days)
    "cloudFiles.cleanSource.moveDestination": archive_path
}

claim_images_df = (
    spark.readStream
    .format("cloudFiles")
    .option("cloudFiles.format", "binaryFile")
    .option("cloudFiles.schemaLocation", f"{metadata_path}/_schema")
    .options(**archive_configs)
    .load(source_path)
)

(
    claim_images_df.writeStream
    .option("checkpointLocation", f"{metadata_path}/_checkpoint")
    .trigger(availableNow=True)
    .toTable("smart_claims_dev.01_bronze.claim_images")
)

# COMMAND ----------

display(spark.table("smart_claims_dev.01_bronze.claim_images").select("path", "length"))

# COMMAND ----------

display(dbutils.fs.ls("/Volumes/smart_claims_dev/00_landing/claims/archive"))

# COMMAND ----------

src = "/Volumes/smart_claims_dev/00_landing/claims/images"
dst = "/Volumes/smart_claims_dev/00_landing/claims/archive"

for f in dbutils.fs.ls(src):
    dbutils.fs.mv(f.path, f"{dst}/{f.name}")

display(dbutils.fs.ls(dst))   # should now show 15 files