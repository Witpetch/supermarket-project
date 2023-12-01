import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrame
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql.functions import col, to_date, to_timestamp, date_format

## @params: [JOB_NAME]
args = getResolvedOptions(sys.argv, ['JOB_NAME'])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# create dynamic frame from table supermarket_branch_data
datasource0 = glueContext.create_dynamic_frame.from_catalog(database='de-proj-glue-etl-db', table_name='supermarket_branch_data_csv')
# create dynamic frame from table supermarket_sales_data
datasource1 = glueContext.create_dynamic_frame.from_catalog(database='de-proj-glue-etl-db', table_name='supermarket_sales_data_csv')
# Perform the join
joined_dynamic_frame = Join.apply(
    frame1=datasource0,
    frame2=datasource1,
    keys1=["invoice id"],
    keys2=["invoice id"],
    transformation_ctx="joined_data"
)

df_spark = joined_dynamic_frame.toDF()


def process_data(df):
    #Filter and drop duplicates from invoice id / Drop null on invoice id
    df = df.dropDuplicates(["invoice id"])
    df = df.dropna(subset=["invoice id"])

    #Rename columns to lowercase
    lowercase_columns = [col.lower() for col in df.columns]
    df = df.toDF(*lowercase_columns)

    #Rename columns from spacebar to _
    underscored_columns = [col.replace(" ", "_") for col in df.columns]
    df = df.toDF(*underscored_columns)

    # Convert column Date from string to date type
    df = df.withColumn("Date", date_format(to_date(col("Date"), "M/d/yyyy"), "MM-dd-yyyy"))

    # Convert column time from string to timesatmp type
    df = df.withColumn("time", date_format(to_timestamp(col("time"), "HH:mm"), "HH:mm"))
    
    return df

df_final = process_data(df_spark)

# From Spark dataframe to glue dynamic frame
glue_dynamic_frame_final = DynamicFrame.fromDF(df_final, glueContext, "df_final_output")

# Write the data in the DynamicFrame to a location in Amazon S3 and a table for it in the AWS Glue Data Catalog
s3output = glueContext.getSink(
  path="s3://aws-glue-de-proj-etl-clean/supermarket_reports_parquet",
  connection_type="s3",
  updateBehavior="UPDATE_IN_DATABASE",
  partitionKeys=[],
  compression="snappy",
  enableUpdateCatalog=True,
  transformation_ctx="s3output",
)

s3output.setCatalogInfo(
  catalogDatabase="de-proj-glue-etl-db", catalogTableName="supermarket_reports_parquet"
)

s3output.setFormat("glueparquet")
s3output.writeFrame(glue_dynamic_frame_final)

job.commit()