import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrameCollection
from awsgluedq.transforms import EvaluateDataQuality
from awsglue import DynamicFrame
import concurrent.futures
import re


class GroupFilter:
    def __init__(self, name, filters):
        self.name = name
        self.filters = filters


def apply_group_filter(source_DyF, group):
    return Filter.apply(frame=source_DyF, f=group.filters)


def threadedRoute(glue_ctx, source_DyF, group_filters) -> DynamicFrameCollection:
    dynamic_frames = {}
    with concurrent.futures.ThreadPoolExecutor(max_workers=5) as executor:
        future_to_filter = {
            executor.submit(apply_group_filter, source_DyF, gf): gf
            for gf in group_filters
        }
        for future in concurrent.futures.as_completed(future_to_filter):
            gf = future_to_filter[future]
            if future.exception() is not None:
                print("%r generated an exception: %s" % (gf, future.exception()))
            else:
                dynamic_frames[gf.name] = future.result()
    return DynamicFrameCollection(dynamic_frames, glue_ctx)


args = getResolvedOptions(sys.argv, ["JOB_NAME"])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

# Source S3 bucket and catalog table
S3bucket_node1 = glueContext.create_dynamic_frame.from_catalog(
    database="movies-dataset-metadata",
    table_name="imdb_movies_rating_csv",
    transformation_ctx="S3bucket_node1",
)

# Data Quality Ruleset
EvaluateDataQuality_node_ruleset = """
Rules = [
    RowCount > 500,
    IsComplete "poster_link",
    Uniqueness "poster_link" > 0.95,
    ColumnLength "series_title" between 1 and 100,
    IsComplete "released_year",
    ColumnValues "released_year" in ["2000", "2001", "2010", "2020"] with threshold >= 0.9,
    IsComplete "imdb_rating",
    ColumnValues "imdb_rating" between 1.0 and 10.0,
    Completeness "meta_score" >= 0.8,
    IsComplete "genre"
]
"""

EvaluateDataQuality_node = EvaluateDataQuality().process_rows(
    frame=S3bucket_node1,
    ruleset=EvaluateDataQuality_node_ruleset,
    publishing_options={
        "dataQualityEvaluationContext": "EvaluateDataQuality",
        "enableDataQualityCloudWatchMetrics": True,
        "enableDataQualityResultsPublishing": True,
    },
    additional_options={"performanceTuning.caching": "CACHE_NOTHING"},
)

# Route data based on quality evaluation
ConditionalRouter = threadedRoute(
    glueContext,
    source_DyF=EvaluateDataQuality_node,
    group_filters=[
        GroupFilter(
            name="output_group_1",
            filters=lambda row: (
                bool(re.match("Failed", row["DataQualityEvaluationResult"]))
            ),
        ),
        GroupFilter(
            name="default_group",
            filters=lambda row: (
                not (bool(re.match("Failed", row["DataQualityEvaluationResult"])))
            ),
        ),
    ],
)

# Process valid data
ChangeSchema_node = ApplyMapping.apply(
    frame=ConditionalRouter.getFrame("default_group"),
    mappings=[
        ("poster_link", "string", "poster_link", "varchar"),
        ("series_title", "string", "series_title", "varchar"),
        ("released_year", "string", "released_year", "varchar"),
        ("certificate", "string", "certificate", "varchar"),
        ("runtime", "string", "runtime", "varchar"),
        ("genre", "string", "genre", "varchar"),
        ("imdb_rating", "double", "imdb_rating", "decimal"),
        ("overview", "string", "overview", "varchar"),
        ("meta_score", "long", "meta_score", "int"),
        ("director", "string", "director", "varchar"),
        ("star1", "string", "star1", "varchar"),
        ("star2", "string", "star2", "varchar"),
        ("star3", "string", "star3", "varchar"),
        ("star4", "string", "star4", "varchar"),
        ("no_of_votes", "long", "no_of_votes", "int"),
        ("gross", "string", "gross", "varchar"),
    ],
    transformation_ctx="ChangeSchema_node",
)

# Write valid data to Redshift
glueContext.write_dynamic_frame.from_options(
    frame=ChangeSchema_node,
    connection_type="redshift",
    connection_options={
        "redshiftTmpDir": "s3://movie-data-analysis/temp/",
        "dbtable": "movies.imdb_movies_rating",
        "connectionName": "redshift-connection",
        "preactions": "CREATE TABLE IF NOT EXISTS movies.imdb_movies_rating (...)",
    },
)

# Write invalid data to S3
glueContext.write_dynamic_frame.from_options(
    frame=ConditionalRouter.getFrame("output_group_1"),
    connection_type="s3",
    format="json",
    connection_options={"path": "s3://movie-data-analysis/invalid-data/"},
)

job.commit()
