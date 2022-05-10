from pyspark import SparkConf, SparkContext
from pyspark.sql import SQLContext
from pyspark.sql.types import *
from pyspark import SparkConf, SparkContext
from shapely.geometry import Point
from shapely.geometry.polygon import Polygon
import shapely.wkt
import pyspark.sql.functions as F


def in_polygon(point, polygon_shape):
    p = shapely.wkt.loads(point)
    return polygon_shape.contains(p)


def proccesing_points(credentials: dict, destination: dict, polygon_str):

    polygon = polygon_str
    polygon_shape = shapely.wkt.loads(polygon)

    sc = SparkContext("local", "jobsity_spark")
    spark = SQLContext(sc)
    spark_conf = SparkConf().setMaster("local").setAppName("jobsity_spark")

    # Set options below
    sfOptions = {
        "sfURL": f"{credentials['account']}.snowflakecomputing.com",
        "sfUser": credentials["user"],
        "sfPassword": credentials["password"],
        "sfDatabase": destination["database"],
        "sfSchema": destination["schema"],
        "sfWarehouse": destination["warehouse"],
    }

    SNOWFLAKE_SOURCE_NAME = "net.snowflake.spark.snowflake"

    df = (
        spark.read.format(SNOWFLAKE_SOURCE_NAME)
        .options(**sfOptions)
        .option("query", "select * from TRIPS_STAGING_TABLE")
        .load()
    )

    check_point = F.udf(lambda coord: in_polygon(coord, polygon_shape), BooleanType())

    df_in_polygon = df.filter(check_point(F.col("ORIGIN_COORD")))
    
    ##Here we basically calculate a week_of_year column and group by this column to finally get the average trips for the region from the input
    df_in_polygon.withColumn("week_of_year", F.weekofyear(F.col("DATETIME"))).groupBy(
        "week_of_year"
    ).count().select(F.avg("count")).show()
