import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Script generated for node Trending Video
TrendingVideo_node1729361372636 = glueContext.create_dynamic_frame.from_catalog(database="youtube-trending-kieitmo-database", table_name="trending_videos", transformation_ctx="TrendingVideo_node1729361372636")

# Script generated for node Category
Category_node1729361375299 = glueContext.create_dynamic_frame.from_catalog(database="youtube-trending-kieitmo-database", table_name="video_categories", transformation_ctx="Category_node1729361375299")

# Script generated for node Change column names for join
Changecolumnnamesforjoin_node1729355546165 = ApplyMapping.apply(frame=TrendingVideo_node1729361372636, mappings=[("id", "string", "left_video_id", "string"), ("`snippet.publishedat`", "string", "left_video_publishedat", "date"), ("`snippet.channelid`", "string", "left_video_channelid", "string"), ("`snippet.title`", "string", "left_video_title", "string"), ("`snippet.description`", "string", "left_video_description", "string"), ("`snippet.categoryid`", "string", "left_video_categoryid", "string"), ("`snippet.defaultlanguage`", "string", "left_video_defaultlanguage", "string"), ("`statistics.viewcount`", "string", "left_video_viewcount", "long"), ("`statistics.likecount`", "string", "left_video_likecount", "long"), ("`statistics.commentcount`", "string", "left_video_commentcount", "long"), ("region", "string", "left_video_region", "string"), ("year", "string", "left_video_year", "string"), ("month", "string", "left_video_month", "string"), ("day", "string", "left_video_day", "string")], transformation_ctx="Changecolumnnamesforjoin_node1729355546165")

# Script generated for node Change column names for join
Changecolumnnamesforjoin_node1729358436066 = ApplyMapping.apply(frame=Category_node1729361375299, mappings=[("kind", "string", "right_category_kind", "string"), ("etag", "string", "right_category_etag", "string"), ("id", "string", "right_category_id", "string"), ("`snippet.title`", "string", "right_category_title", "string"), ("`snippet.assignable`", "boolean", "right_category_assignable", "boolean"), ("`snippet.channelid`", "string", "right_category_channelid", "string"), ("region", "string", "right_category_region", "string"), ("year", "string", "right_category_year", "string"), ("month", "string", "right_category_month", "string"), ("day", "string", "right_category_day", "string")], transformation_ctx="Changecolumnnamesforjoin_node1729358436066")

# Script generated for node Join
Join_node1729355535196 = Join.apply(frame1=Changecolumnnamesforjoin_node1729355546165, frame2=Changecolumnnamesforjoin_node1729358436066, keys1=["left_video_categoryid", "left_video_region", "left_video_month", "left_video_day", "left_video_year"], keys2=["right_category_id", "right_category_region", "right_category_month", "right_category_day", "right_category_year"], transformation_ctx="Join_node1729355535196")

# Script generated for node Change Schema
ChangeSchema_node1729361983023 = ApplyMapping.apply(frame=Join_node1729355535196, mappings=[("left_video_id", "string", "id", "string"), ("left_video_publishedat", "date", "publishedat", "string"), ("left_video_channelid", "string", "channelid", "string"), ("left_video_title", "string", "title", "string"), ("left_video_description", "string", "description", "string"), ("left_video_defaultlanguage", "string", "defaultlanguage", "string"), ("left_video_viewcount", "long", "viewcount", "long"), ("left_video_likecount", "long", "likecount", "long"), ("left_video_commentcount", "long", "commentcount", "long"), ("left_video_region", "string", "region", "string"), ("left_video_year", "string", "year", "string"), ("left_video_month", "string", "month", "string"), ("left_video_day", "string", "day", "string"), ("right_category_id", "string", "category_id", "string"), ("right_category_title", "string", "category_title", "string")], transformation_ctx="ChangeSchema_node1729361983023")

# Script generated for node Amazon S3
AmazonS3_node1729355675358 = glueContext.getSink(path="s3://kietitmo-youtube-data-for-analysis", connection_type="s3", updateBehavior="LOG", partitionKeys=["region", "year", "month", "day"], enableUpdateCatalog=True, transformation_ctx="AmazonS3_node1729355675358")
AmazonS3_node1729355675358.setCatalogInfo(catalogDatabase="youtube-trending-kieitmo-database",catalogTableName="youtube-trending-videos-for-analysis")
AmazonS3_node1729355675358.setFormat("glueparquet", compression="snappy")
AmazonS3_node1729355675358.writeFrame(ChangeSchema_node1729361983023)
job.commit()