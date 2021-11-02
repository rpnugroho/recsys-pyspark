import gzip
import json
from pyspark.sql import SparkSession
from pyspark.sql.types import StructField, FloatType, StructType, StringType
from pyspark.ml.feature import StringIndexer, IndexToString
from pyspark.ml import Pipeline
from pyspark.ml.recommendation import ALS
from pyspark.sql.functions import struct, col, array, lit


FILENAME = "Gift_Cards_5.json.gz"
RAW_DATA_BUCKET = f"reviews/{FILENAME}"

N_RECS = 100  # number of recommendation

spark = SparkSession.builder.master("local").appName("amazon-recsys").getOrCreate()

# UNCOMMENT IF USING GCS
# # Setup hadoop fs configuration for schema gs://
# conf = spark.sparkContext._jsc.hadoopConfiguration()
# conf.set("fs.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem")
# conf.set("fs.AbstractFileSystem.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFS")

# UTILITY FUNCTION
def get_rating(path):
    """Create dataframe data"""

    data_schema = [
        StructField("asin", StringType(), True),
        StructField("reviewerID", StringType(), True),
        StructField("overall", FloatType(), True),
    ]
    final_schema = StructType(fields=data_schema)

    df = spark.read.json(path, schema=final_schema)
    return df

# LOAD DATA
reviews_data = get_rating(RAW_DATA_BUCKET)

# TRANSFORM STRING TO INDEX
indexer = [
    StringIndexer(inputCol="asin", outputCol="asin_index"),
    StringIndexer(inputCol="reviewerID", outputCol="reviewerID_index"),
]
pipeline = Pipeline(stages=indexer)
transformed = pipeline.fit(reviews_data).transform(reviews_data)

# RECOMMENDATION
# creating an item vector using ALS
als = ALS(
    maxIter=5,
    regParam=0.01,
    userCol="reviewerID_index",
    itemCol="asin_index",
    ratingCol="overall",
    coldStartStrategy="drop",
    nonnegative=True,
)
# fit model
model = als.fit(transformed)
# create a recommendation
user_recs = model.recommendForAllUsers(N_RECS)
user_recs.printSchema()

# REVERSE INDEX TO STRING
# get index label
index_labels = {
    c.name: c.metadata["ml_attr"]["vals"]
    for c in transformed.schema.fields
    if c.name.endswith("_index")
}
# reverse reviewer index
reviewerID_labels = index_labels["reviewerID_index"]
reviewerID_to_label = IndexToString(
    inputCol="reviewerID_index", outputCol="reviewerID", labels=reviewerID_labels
)
user_recs = reviewerID_to_label.transform(user_recs)
# reverse asin index
asin_labels = index_labels["asin_index"]
asin_labels_ = array(*[lit(x) for x in asin_labels])
recommendations = array(
    *[
        struct(
            asin_labels_[col("recommendations")[i]["asin_index"]].alias("asin"),
            col("recommendations")[i]["rating"].alias("rating"),
        )
        for i in range(N_RECS)
    ]
)
user_recs = user_recs.withColumn("recommendations", recommendations)

# DROP UNUSED COLUMNS
columns_to_drop = ["asin_index", "reviewerID_index"]
user_recs = user_recs.drop(*columns_to_drop)

# REORDER COLUMNS
user_recommendation = user_recs.select("reviewerID", "recommendations")
user_recommendation.show()

# CAST recommendations TO STRING
user_recommendation = user_recommendation.withColumn('recommendations', col('recommendations').cast('string'))

# WRITE TO POSTGRES
user_recommendation.write.format("jdbc").options(
    url="jdbc:postgresql://0.0.0.0",
    driver="org.postgresql.Driver",
    dbtable="metadata",
    user="postgres",
    password="postgres",
).mode("overwrite").save()
