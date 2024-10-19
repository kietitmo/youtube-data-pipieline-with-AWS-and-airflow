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
Changecolumnnamesforjoin_node1729355546165 = ApplyMapping.apply(frame=TrendingVideo_node1729361372636, mappings=[("id", "string", "left_video_id", "string"), ("`snippet.publishedat`", "string", "left_video_publishedat", "string"), ("`snippet.channelid`", "string", "left_video_channelid", "string"), ("`snippet.title`", "string", "left_video_title", "string"), ("`snippet.description`", "string", "left_video_description", "string"), ("`snippet.categoryid`", "string", "left_video_categoryid", "string"), ("`snippet.defaultlanguage`", "string", "left_video_defaultlanguage", "string"), ("`statistics.viewcount`", "string", "left_video_viewcount", "long"), ("`statistics.likecount`", "string", "left_video_likecount", "long"), ("`statistics.commentcount`", "string", "left_video_commentcount", "long"), ("region", "string", "region", "string"), ("year", "string", "year", "string"), ("month", "string", "month", "string"), ("day", "string", "day", "string")], transformation_ctx="Changecolumnnamesforjoin_node1729355546165")

# Script generated for node Change column names for join
Changecolumnnamesforjoin_node1729358436066 = ApplyMapping.apply(frame=Category_node1729361375299, mappings=[("id", "string", "right_category_id", "string"), ("`snippet.title`", "string", "right_category_title", "string"), ("region", "string", "region", "string"), ("year", "string", "year", "string"), ("month", "string", "month", "string"), ("day", "string", "day", "string")], transformation_ctx="Changecolumnnamesforjoin_node1729358436066")

# Script generated for node Renamed keys for Join
RenamedkeysforJoin_node1729361889932 = ApplyMapping.apply(frame=Changecolumnnamesforjoin_node1729358436066, mappings=[("right_category_id", "string", "right_right_category_id", "string"), ("right_category_title", "string", "right_right_category_title", "string"), ("region", "string", "right_region", "string"), ("year", "string", "right_year", "string"), ("month", "string", "right_month", "string"), ("day", "string", "right_day", "string")], transformation_ctx="RenamedkeysforJoin_node1729361889932")

# Script generated for node Join
Join_node1729355535196 = Join.apply(frame1=Changecolumnnamesforjoin_node1729355546165, frame2=RenamedkeysforJoin_node1729361889932, keys1=["left_video_categoryid", "region", "year", "month", "day"], keys2=["right_right_category_id", "right_region", "right_year", "right_month", "right_day"], transformation_ctx="Join_node1729355535196")

# Script generated for node Change Schema
ChangeSchema_node1729361983023 = ApplyMapping.apply(frame=Join_node1729355535196, mappings=[("left_video_id", "string", "id", "string"), ("left_video_publishedat", "string", "publishedat", "string"), ("left_video_channelid", "string", "channelid", "string"), ("left_video_title", "string", "title", "string"), ("left_video_description", "string", "description", "string"), ("left_video_defaultlanguage", "string", "defaultlanguage", "string"), ("left_video_viewcount", "long", "viewcount", "long"), ("left_video_likecount", "long", "likecount", "long"), ("left_video_commentcount", "long", "commentcount", "long"), ("region", "string", "region", "string"), ("year", "string", "year", "string"), ("month", "string", "month", "string"), ("day", "string", "day", "string"), ("right_right_category_id", "string", "category_id", "string"), ("right_right_category_title", "string", "category_title", "string")], transformation_ctx="ChangeSchema_node1729361983023")

# Script generated for node Amazon S3
AmazonS3_node1729355675358 = glueContext.getSink(path="s3://kietitmo-youtube-data-for-analysis", connection_type="s3", updateBehavior="UPDATE_IN_DATABASE", partitionKeys=["region", "year", "month", "day"], enableUpdateCatalog=True, transformation_ctx="AmazonS3_node1729355675358")
AmazonS3_node1729355675358.setCatalogInfo(catalogDatabase="youtube-trending-kieitmo-database",catalogTableName="youtube-trending-videos-for-analysis")
AmazonS3_node1729355675358.setFormat("glueparquet", compression="snappy")
AmazonS3_node1729355675358.writeFrame(ChangeSchema_node1729361983023)
job.commit()