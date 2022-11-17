import sys

from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext

args = getResolvedOptions(sys.argv, ["JOB_NAME", "file_key"])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
logger = glueContext.get_logger()
job = Job(glueContext)
job.init(args["JOB_NAME"], args)
logger.info(args['file_key'].replace('+', ' '))
file_name = args['file_key'].replace('+', ' ').rstrip()

# Script generated for node S3 bucket
S3bucket_node1 = glueContext.create_dynamic_frame.from_options(
    format_options={
        "quoteChar": '"',
        "withHeader": True,
        "separator": ",",
        "optimizePerformance": False,
    },
    connection_type="s3",
    format="csv",
    connection_options={"paths": ["s3://raw-uber-data/" + file_name]},
    transformation_ctx="S3bucket_node1",
)

# Script generated for node ApplyMapping
ApplyMapping_node2 = ApplyMapping.apply(
    frame=S3bucket_node1,
    mappings=[
        ("Origin Movement ID", "string", "origin_id", "long"),
        ("Origin Display Name", "string", "origin_name", "string"),
        ("Origin Geometry", "string", "origin_geometry", "string"),
        ("Destination Movement ID", "string", "destination_id", "long"),
        ("Destination Display Name", "string", "destination_name", "string"),
        ("Destination Geometry", "string", "destination_geometry", "string"),
        ("Date Range", "string", "date_range", "string"),
        ("`Mean Travel Time (Seconds)`", "string", "mean_travel_time", "long"),
        (
            "`Range - Lower Bound Travel Time (Seconds)`",
            "string",
            "lower_travel_time",
            "long",
        ),
        (
            "`Range - Upper Bound Travel Time (Seconds)`",
            "string",
            "upper_travel_time",
            "long",
        ),
    ],
    transformation_ctx="ApplyMapping_node2",
)

# Script generated for node S3 bucket
S3bucket_node3 = glueContext.write_dynamic_frame.from_options(
    frame=ApplyMapping_node2,
    connection_type="s3",
    format="glueparquet",
    connection_options={
        "path": "s3://processed-uber-data/city=" + file_name.split(' - ')[-1].replace('.csv', '') + '/',
        "partitionKeys": [],
    },
    format_options={"compression": "snappy"},
    transformation_ctx="S3bucket_node3",
)

job.commit()
