# import dlt
# from pyspark.sql.functions import from_json, col, struct
# from pyspark.sql.types import MapType, StringType

# kinesis_config = {
#     "streamName": "telematics-stream-tmh",
#     "region": "us-east-1",
#     "serviceCredential": "kinesis-service-credential", #unity catalog service credentials
#     "initialPosition": "earliest"
# }

# @dlt.table(
#     name = "telematics_test",
#     comment = "Parsed Kinesis data holding telematics data",
#     table_properties = {"quality": "bronze"}
# )

# def bronze_table():
#     raw_stream = (
#         spark.readStream
#         .format("kinesis")
#         .options(**kinesis_config)
#         .load()
#     )
#     return raw_stream