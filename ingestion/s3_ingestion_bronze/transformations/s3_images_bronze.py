import dlt

# Both volumes are external volumes backed by s3://smart-claims-landing-daniel
landing_path = "/Volumes/smart_claims_dev/00_landing"


# Course file: ingest_training_imgs.py
@dlt.table(
    name="smart_claims_dev.01_bronze.training_images",
    comment="Raw accident training images ingested from S3",
    table_properties={"quality": "bronze"}
)
def training_images():
    return (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "BINARYFILE")   # each image = one row (path, length, content)
        .load(f"{landing_path}/training_imgs")
    )


# Course file: metadata_imgs_schema.py
@dlt.table(
    name="smart_claims_dev.01_bronze.claim_images_meta",
    comment="Raw accident claim images metadata ingested from S3",
    table_properties={"quality": "bronze"}
)
def claim_images_meta():
    return (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "csv")
        .option("header", "true")   # added: the course file omits this, which turns the header into a data row
        .load(f"{landing_path}/claims/metadata")
    )