import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql.functions import *
from pyspark.sql import *
from pyspark import *
import boto3


reload(sys)
sys.setdefaultencoding('UTF8')
## @params: [JOB_NAME]
args = getResolvedOptions(sys.argv, ['JOB_NAME'])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)
relus360_reports = SparkSession.builder.appName('relus360_reports').getOrCreate()
pm_dashboard_query_file = sc.textFile('s3://relus360.datalake/query-file/resource.txt')
pm_dashboard_query_text = ''.join(pm_dashboard_query_file.collect())


pm_dashboard_query= relus360_reports.read.format('jdbc').options(
url='jdbc:awsathena://AwsRegion=us-east-1;UID=;PWD=;Schema=relus360_netsuite_ods;S3OutputLocation=s3://relus360.datalake/temp/',
          driver='',
          dbtable='('+pm_dashboard_query_text+')').load()
pm_dashboard_query.write.mode('overwrite').format("parquet").save("s3://relus360.datalake/relus360_reports/resource/")
glue_client = boto3.client('glue', region_name='us-east-1')
# glue_client.start_crawler(Name='relus360_reports_resource')
job.commit()
