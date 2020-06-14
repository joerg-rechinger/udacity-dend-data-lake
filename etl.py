#import configparser
from datetime import datetime, date
import os
from pyspark.sql.types import StringType, DateType, StructType, StructField, DoubleType, IntegerType, LongType
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col, expr, to_timestamp, to_date
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, date_format, desc, from_unixtime, dayofweek

# for running locally, uncomment line 1, 10, 11, 13 and 14 and fill in dl.cfg file with your AWS credentials
#config = configparser.ConfigParser()
#config.read('dl.cfg')

#os.environ['AWS_ACCESS_KEY_ID']=config['default']['AWS_ACCESS_KEY_ID']
#os.environ['AWS_SECRET_ACCESS_KEY']=config['default']['AWS_SECRET_ACCESS_KEY']


def create_spark_session():
    '''Initiates the SparkSession.
    Parameters:
    None

    Returns:
    SparkSession object, configured according to the specifications in .config method
    '''
    spark = SparkSession \
        .builder \
        .config('spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0') \
        .getOrCreate()
    return spark


def process_song_data(spark, input_data, output_data):
    '''Reads the song data from S3, transforms it into various tables and writes those back to S3.
    Parameters:
    spark: the SparkSession object
    input_data: path to S3 bucket where the song input data is stored
    output_data: path to your private S3 bucket where the output data should be stored.

    Returns:
    None
    '''
    # get filepath to song data file
    # using the reduced song_data input, song-data is the full input from udacity's S3
    song_data = str(input_data)+'/song_data/*/*/*/*.json'

    # read song data file
    df = spark.read.json(song_data)

    # extract columns to create songs table
    songs_table = df.select(['song_id', 'title', 'artist_id', 'year', 'duration']).dropDuplicates()

    # write songs table to parquet files partitioned by year and artist
    songs_table.write.format('parquet')\
        .partitionBy(['year', 'artist_id'])\
        .option('path', str(output_data)+'/songs')\
        .saveAsTable('songs', mode = 'overwrite')

    # extract columns to create artists table
    artists_table = df.select(['artist_id', 'artist_name', 'artist_location', 'artist_latitude', 'artist_longitude']).dropDuplicates()

    # write artists table to parquet files
    artists_table.write.format('parquet')\
        .option('path', str(output_data)+'/artists')\
        .saveAsTable('artists', mode = 'overwrite')


def process_log_data(spark, input_data, output_data):
    '''Reads the log data from S3, transforms it into various tables and writes those back to S3.
    Parameters:
    spark: the SparkSession object
    input_data: path to S3 bucket where the log input data is stored
    output_data: path to your private S3 bucket where the output data should be stored.

    Returns:
    None
    '''
    # get filepath to log data file
    # using the 'log-data' file for reduced input, log_data is the full data file on udacity's S3
    log_data = str(input_data)+'/log-data/*/*/*.json'

    # read log data file
    df = spark.read.json(log_data)

    # filter by actions for song plays
    df = df.where(df.page=='NextSong')

    # extract columns for users table
    user_table = df.select(['userId', 'firstName', 'lastName', 'gender', 'level'])\
        .dropDuplicates()\
        .orderBy('userId')

    # write users table to parquet files
    user_table.write.format('parquet')\
        .option('path', str(output_data)+'/users')\
        .saveAsTable('users', mode = 'overwrite')

    # create timestamp column from original timestamp column
    get_timestamp = udf(lambda x: datetime.fromtimestamp(x / 1000.0).strftime('%Y-%m-%d %H:%M:%S'))
    df = df.withColumn('ts_2', get_timestamp('ts')).withColumn('timestamp', to_timestamp('ts_2', 'yyyy-MM-dd HH:mm:ss'))

    # create datetime column from original timestamp column
    df = df.withColumn('date', to_date('timestamp'))

    # extract columns to create time table
    time_table = df.select(['userId', 'sessionId', 'timestamp'])\
        .withColumn('hour', hour('timestamp'))\
        .withColumn('day', dayofmonth('timestamp'))\
        .withColumn('month', month('timestamp'))\
        .withColumn('year', year('timestamp'))\
        .withColumn('weekday', dayofweek('timestamp'))\
        .dropDuplicates()

    # write time table to parquet files partitioned by year and month
    time_table.write.format('parquet')\
        .partitionBy(['year', 'month'])\
        .option('path', str(output_data)+'/time')\
        .saveAsTable('time', mode = 'overwrite')

    # read in song data to use for songplays table
    song_df = spark.read.json(str(input_data)+'/song_data/*/*/*/*.json')

    # extract columns from joined song and log datasets to create songplays table
    # creating the conditions for the join below
    cond=[df.artist==song_df.artist_name, df.song==song_df.title]
    songplays_table = df.join(song_df, cond, 'left')
    # create a unique songplay id from session id+timestamp
    songplays_table = songplays_table.withColumn('songplay_id', expr('"sessionId"+"ts"'))\
        .select(['songplay_id', 'timestamp', 'userId', 'level', 'song_id', 'artist_id', 'sessionId', 'location', 'userAgent'])\
        .withColumn('month', month('timestamp'))\
        .withColumn('year', year('timestamp'))\
        .withColumnRenamed('timestamp','start_time')\
        .dropDuplicates()

    # write songplays table to parquet files partitioned by year and month
    songplays_table.write.format('parquet')\
        .partitionBy(['year', 'month'])\
        .option('path', str(output_data)+'/songplays')\
        .saveAsTable('songplays', mode = 'overwrite')


def main():
    '''Main function that first declares the three inputs to the process_song_data and process_log_data functions and then runs them.
    Parameters:
    None
    Returns:
    None
    '''
    spark = create_spark_session()
    input_data = 's3a://udacity-dend/'
    #fill in the name of your private S3 bucket
    output_data = ''

    process_song_data(spark, input_data, output_data)
    process_log_data(spark, input_data, output_data)


if __name__ == '__main__':
    '''Executes the main function.
    '''
    main()
