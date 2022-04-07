# Data Lake with Spark

A music streaming startup has grown their user base and song database by a lot, and they wanted to move their data to a data lake. Original data resides in S3, in a directory of JSON logs on user activity on the app, as well as a directory with JSON metadata on the songs in the app.

I've built an ETL pipeline that extracts their data from S3, processes it using Spark, and loads the data back into S3 as a set of dimensional tables. This allows the analytics team to continue finding insights in what songs their users are listening to.

<hr>

### Project Datasets
You'll be working with two datasets that reside in S3. Here are the S3 links for each:
<br>
Song data: `s3://udacity-dend/song_data` <br>
Log data: `s3://udacity-dend/log_data` <br>
<br>

### Star Schema includes the following tables:<br>
* Fact Table<br>
<b>songplays</b> - records in event data associated with song plays i.e. records with page NextSong<br>

* Dimension Tables<br>
<b>users</b> - users in the app <br>
<b>songs</b> - songs in music database<br>
<b>artists</b> - artists in music database<br>
<b>time</b> - timestamps of records in <b>songplays</b> broken down into specific units<br>


### Spark Process <br>

The ETL job processes the `song files` then the `log files`. The `song files` are listed and iterated over entering relevant information in the artists and the song folders in parquet. The `log files` are filtered by the NextSong action. The subsequent dataset is then processed to extract the date, time, year etc. fields and records are then appropriately entered into the `time`, `users` and `songplays` folders in parquet for analysis.<br>

### Project Structure <br>
`dl.cfg` - Configuration file that contains info about AWS credentials
`etl.py` - The ETL to reads data from S3, processes that data using Spark, and writes them to a new S3

