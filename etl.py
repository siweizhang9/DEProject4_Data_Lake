from configparser import ConfigParser
from datetime import datetime
import boto3
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf
from pyspark.sql.types import StringType
from pyspark.sql.types import IntegerType
from pyspark.sql.functions import desc
from pyspark.sql.functions import asc
from pyspark.sql.functions import col
from pyspark.sql.functions import sum as Fsum
from pyspark.sql.functions import from_unixtime, hour, dayofmonth, weekofyear, month, year, date_format, dayofweek
from pyspark.sql.functions import monotonically_increasing_id
import os

# Replace "my-bucket-name" with the name of your S3 bucket
bucket_name = "udacity-dend"
config = ConfigParser()
config.read('dl.cfg')

aws_access_key_id = config.get('credentials', 'AWS_ACCESS_KEY_ID')
aws_secret_access_key = config.get('credentials', 'AWS_SECRET_ACCESS_KEY')

# Create an S3 client using the session
s3 = boto3.client('s3', aws_access_key_id=aws_access_key_id,
                  aws_secret_access_key=aws_secret_access_key)

def get_s3_filepaths(bucket_name, prefix):
    file_paths = []
    response = s3.list_objects_v2(Bucket=bucket_name, Prefix=prefix, Delimiter='/')

    for content in response.get('Contents', []):
        if content['Key'].endswith('.json'):
            file_paths.append("s3a://" + bucket_name + "/" +content['Key'])

    for subdir in response.get('CommonPrefixes', []):
        file_paths.extend(get_s3_filepaths(bucket_name, subdir['Prefix']))
    return file_paths


def create_spark_session():
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    spark.conf.set("spark.hadoop.fs.s3a.access.key", aws_access_key_id)
    spark.conf.set("spark.hadoop.fs.s3a.secret.key", aws_secret_access_key)
    return spark


def process_song_data(spark, input_data, output_data):

    all_files = get_s3_filepaths(bucket_name, input_data)

   # read song data file
    schema = spark.read.json(all_files[1]).schema

    combined_df = spark.createDataFrame([], schema)
    for filename in all_files:
        if filename.endswith(".json"):
            df = spark.read.json(filename)
            combined_df = combined_df.union(df)
    
    # extract columns to create songs table
    songs_table = combined_df.select("song_id", "title", "artist_id", "year", "duration")
    
    # write songs table to parquet files partitioned by year and artist
    songs_table.write.mode("overwrite").parquet(output_data + "songs_table.parquet")

    # extract columns to create artists table
    artists_table = combined_df.select("artist_id", "artist_name", "artist_location", "artist_latitude", "artist_longitude")
    artists_table = artists_table.dropDuplicates()
    
    # write artists table to parquet files
    artists_table.write.mode("overwrite").parquet(output_data + "artists_table.parquet")
    return songs_table, artists_table


def process_log_data(spark, input_data, output_data):

    all_files = get_s3_filepaths(bucket_name, input_data)
    
    # read log data file
    log_schema = spark.read.json(all_files[1]).schema
    combined_log_df = spark.createDataFrame([], log_schema)
    for filename in all_files:
        if filename.endswith(".json"):
            df = spark.read.json(filename)
            combined_log_df = combined_log_df.union(df)
    
    # filter by actions for song plays
    log_df = combined_log_df.filter(combined_log_df.page == "NextSong")

    # extract columns for users table    
    users_table = log_df.select("userId", "firstName", "lastName", "gender", "level")
    users_table = users_table.dropDuplicates()
    
    # write users table to parquet files
    users_table.write.mode("overwrite").parquet(output_data + "users_table.parquet")

    # create timestamp column from original timestamp column
    time_table = log_df.withColumn("timestamp", (col("ts")/1000).cast("timestamp"))
    time_table = time_table.select(
        "timestamp",
        hour("timestamp").alias("hour"),
        dayofmonth("timestamp").alias("day"),
        weekofyear("timestamp").alias("week"),
        month("timestamp").alias("month"),
        year("timestamp").alias("year"),
        date_format("timestamp", "E").alias("weekday")
    )   
 
    # write time table to parquet files partitioned by year and month
    time_table.write.mode("overwrite").parquet(output_data + "time_table.parquet")
    return log_df

def create_songplays_table(spark, output_data, songs_table, artists_table, log_df):

    # extract columns from joined song and log datasets to create songplays table 
    songplays_table = log_df.join(songs_table, log_df.song == songs_table.title, 'left') \
    .selectExpr('ts as start_time', 'userId as user_id', 'level', 'sessionId as session_id', 
                'location', 'userAgent as user_agent', 'song', 'title', 'song_id', 'artist')
    
    songplays_table = songplays_table.join(artists_table, songplays_table.artist == artists_table.artist_name, 'left') \
    .selectExpr('start_time', 'user_id', 'level', 'session_id', 'location', 'user_agent', 
                'song_id', 'artist_id')

    songplays_table = songplays_table.withColumn("songplay_id", monotonically_increasing_id() * 1)
    


    # write songplays table to parquet files partitioned by year and month
    songplays_table.write.mode("overwrite").parquet(output_data + "songplays_table.parquet")


def main():
    spark = create_spark_session()
    # Naming the folder names
    log_input_data = "log_data/"
    song_input_data = "song_data/"
    # Naming the output folder for the results
    # s3.put_object(Bucket = bucket_name, Key = "szsparkifyoutput/")
    output_data = "s3://sparkify-spark-sz/szsparkifyoutput/"


    songs_table, artists_table = process_song_data(spark, song_input_data, output_data)    
    log_df = process_log_data(spark, log_input_data, output_data)
    create_songplays_table(spark, output_data, songs_table, artists_table, log_df)
    result = get_s3_filepaths(bucket_name, output_data)
    print(result)


if __name__ == "__main__":
    main()
