import configparser
from datetime import datetime
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, date_format


config = configparser.ConfigParser()
config.read('dl.cfg')

os.environ['AWS_ACCESS_KEY_ID']=config['AWS_ACCESS_KEY_ID']
os.environ['AWS_SECRET_ACCESS_KEY']=config['AWS_SECRET_ACCESS_KEY']


def create_spark_session():

    """
    Create a Spark Session to process 'S3' data.
    """

    spark = SparkSession.builder.config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0").getOrCreate()
    return spark


def process_song_data(spark, input_data, output_data):

    """
    Load song_data from S3 and process it by extracting the songs and artist tables and then again load back to S3.
    """

    # get filepath to song data file
    song_data = os.path.join(input_data, 'song_data/A/A/B/*.json')  
    
    # read song data file
    df = spark.read.json(song_data)

    # extract columns to create songs table
    songs_table = df['song_id', 'title', 'artist_id', 'year', 'duration']
    songs_table = songs_table.dropDuplicates(['song_id'])
    
    # write songs table to parquet files partitioned by year and artist
    songs_table.write.partitionBy('year', 'artist_id').parquet(os.path.join(output_data, 'songs.parquet'), 'overwrite')

    # extract columns to create artists table
    artists_table = artists_table = df['artist_id', 'artist_name', 'artist_location', 'artist_latitude', 'artist_longitude']
    artists_table = artists_table.dropDuplicates(['artist_id'])
    
    # write artists table to parquet files
    artists_table.write.parquet(os.path.join(output_data, 'artists.parquet'), 'overwrite')


def process_log_data(spark, input_data, output_data):

    """
    Load log_data from S3 and process it by extracting the songs and artist tables and then again loaded back to S3. 
    """

    # get filepath to log data file
    log_data = os.path.join(input_data, 'log_data/2018/*/*.json')

    # read log data file
    df = spark.read.json(log_data)
    
    # filter by actions for song plays
    df = df.filter(df['page'] == 'NextSong')

    # extract columns for users table    
    users_table = df['userId', 'firstName', 'lastName', 'gender', 'level']
    users_table = users_table.dropDuplicates(['userId'])
    
    # write users table to parquet files
    users_table.write.parquet(os.path.join(output_data, 'users.parquet'), 'overwrite')

    # create timestamp column from original timestamp column
    get_timestamp = udf(lambda ms: datetime.fromtimestamp(ms / 1000.0).strftime('%Y-%m-%d %H:%M:%S'))
    df = df.withColumn('start_time', get_timestamp(df.ts)) 
    
    # extract columns to create time table
    time_table = df.select(col('datetime').alias('start_time'),
                            hour('datetime').alias('hour'),
                            dayofmonth('datetime').alias('day'),
                            weekofyear('datetime').alias('week'),
                            month('datetime').alias('month'),
                            year('datetime').alias('year')).dropDuplicates()
    
    # write time table to parquet files partitioned by year and month
    time_table.write.partitionBy('year', 'month').parquet(os.path.join(output_data, 'time.parquet'), 'overwrite')

    # read in song data to use for songplays table
    song_df = spark.read.parquet(input_data, 'song_data/A/A/B/*.json')

    combined_df = df.join(song_df, song_df.title == df.song)

    # extract columns from joined song and log datasets to create songplays table 
    songplays_table = combined_df.select(function.monotonically_increasing_id().alias('songplay_id').collect(),
                                         col('ts').alias('ts'),
                                         col('userId').alias('user_id'),
                                         col('level'),
                                         col('song_id').alias('song_id'),
                                         col('artist_id'),
                                         col('sessionId').alias('session_id'),
                                         col('location'),
                                         col('userAgent').alias('user_agent'),
                                         year('datetime').alias('year'),
                                         month('datetime').alias('month'))
    
    # write songplays table to parquet files partitioned by year and month
    songplays_table.write.partitionBy('year', 'month').parquet(os.path.join(output_data, 'songplays.parquet'), 'overwrite')

def main():
    spark = create_spark_session()
    input_data = "s3a://udacity-dend/"
    output_data = "s3a://collected-music-data-lake/"
    
    process_song_data(spark, input_data, output_data)    
    process_log_data(spark, input_data, output_data)


if __name__ == "__main__":
    main()
