import configparser
from datetime import datetime
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col, monotonically_increasing_id
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, date_format
from pyspark.sql.types import TimestampType, DateType


config = configparser.ConfigParser()
config.read('dl.cfg')

os.environ['AWS_ACCESS_KEY_ID']=config['AWS']['AWS_ACCESS_KEY_ID']
os.environ['AWS_SECRET_ACCESS_KEY']=config['AWS']['AWS_SECRET_ACCESS_KEY']


def create_spark_session():
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    return spark


def process_song_data(spark, input_data, output_data):
    """
    This function reads in json song data from a specified input data location.
    It extracts relevant columns to create songs and artists tables in the specified output location.
    """
    print('processing song data....')
    # get filepath to song data file
    song_data = input_data + 'song_data/*/*/*/*.json'
    
    # read song data file
    df = spark.read.json(song_data)
    print('finished reading song data!')

    # extract columns to create songs table
    songs_table = df.select('song_id', 'title', 'artist_id', 'year', 'duration')
    
    # write songs table to parquet files partitioned by year and artist
    songs_table.write.mode('overwrite').partitionBy('year', 'artist_id').parquet(output_data+'songs')

    # extract columns to create artists table
    artists_table = df.select('artist_id', 'artist_name', 'artist_location', 'artist_latitude', 'artist_longitude')
    
    # write artists table to parquet files
    artists_table.write.mode('overwrite').parquet(output_data + 'artists')
    print('finished processing song data!')


def process_log_data(spark, input_data, output_data):
    """
    This function reads in json song and user log data from a specified input data location.
    It extracts relevant columns to create users, time and songplay tables in the specified output location.
    """
    print('processing log data....')
    # get filepath to log data file
    log_data = input_data + 'log_data/*/*/*.json'

    # read log data file
    df = spark.read.json(log_data)
    print('finished reading log data!')

    # filter by actions for song plays
    df = df.where(df.page=='NextSong')

    # extract columns for users table    
    users_table = df.select('userId', 'firstName', 'lastName', 'gender', 'level')

    # write users table to parquet files
    users_table.write.mode('overwrite').parquet(output_data + 'users')

    # create timestamp column from original timestamp column
    get_timestamp = udf(lambda x: datetime.fromtimestamp(x/1000), TimestampType())
    df = df.withColumn('start_time', get_timestamp('ts'))

    # create datetime column from original timestamp column
    get_datetime = udf(lambda x: datetime.fromtimestamp(x/1000), DateType())
    df = df.withColumn('date', get_datetime('ts'))

    # extract columns to create time table
    time_table = df.select('start_time', hour('start_time').alias('hour'), dayofmonth('start_time').alias('day'),\
                           weekofyear('start_time').alias('week'), month('start_time').alias('month'),\
                           year('start_time').alias('year'), date_format('start_time', 'u').alias('weekday'))

    # write time table to parquet files partitioned by year and month
    time_table.write.mode('overwrite').partitionBy('year', 'month').parquet(output_data + 'time')
    
    # get filepath to song data file
    song_data = input_data + 'song_data/*/*/*/*.json'
    
    # read in song data to use for songplays table
    song_df = spark.read.json(song_data)

    # extract columns from joined song and log datasets to create songplays table 
    cond = [df['song']==song_df['title'], df['artist']==song_df['artist_name'], df['length']==song_df['duration']]
    songplays_table = df.join(song_df, cond, 'inner')\
    .withColumn('songplay_id', monotonically_increasing_id())\
    .select(year('start_time').alias('year'), month('start_time').alias('month'),
            'songplay_id', 'start_time', 'userId', 'level',
            'song_id', 'artist_id', 'sessionId', 'location', 'userAgent')

    # write songplays table to parquet files partitioned by year and month
    songplays_table.write.mode('overwrite').partitionBy('year', 'month').parquet(output_data + 'songplays')
    
    print('finished processing log data!')

def main():
    spark = create_spark_session()
    input_data = "s3a://udacity-dend/"
    output_data = "s3://aws-emr-resources-462215576595-us-east-1/notebooks/"
    
    process_song_data(spark, input_data, output_data)    
    process_log_data(spark, input_data, output_data)


if __name__ == "__main__":
    main()
