# Sparkify Relational Data Lake ETL

### Table of Contents

1. [Instructions](#instructions)
2. [Project Summary](#summary)
3. [File Descriptions](#files)
4. [Schema Design](#design)

### Instructions:
1. Run the following commands in the project's root directory to extract the data and to create the relevant data tables.

    - Ensure input and output paths are specified.
    - To create the tables
        `python etl.py`


### Project Summary<a name="summary"></a>
Sparkify is a music streaming startup, that would like to move their data warehouse into a data lake given their user base growth.

The data currently resides in S3 - the Amazon object storage service that stores data in buckets - in a directory of JSON logs on user activity and a directory with JSON metadata on songs.

This ETL pipeline:
- reads data from these directories and writes the data into dimension tables in the specified data lake location.

The end result should be a data lake that can be queried for analysis.

### File Descriptions<a name="files"></a>
There is are various files available in this repo:

- dl.cfg: a settings file containing information on the Amazon  access settings.
- etl.py: a python file which reads in the data from the logs at the specified input data location, and writes the newly created dimension tables to the specified output location.

### Schema Design<a name="design"></a>
The database uses a star schema design. That is, it is designed around fact and dimension tables.

The fact tables include:
- songplays

The dimension tables include:
- users
- songs
- artists
- time

This design was chosen to simplify queries and to optimise for fast aggregations. It is expected that the data in the songplays table will be heavily used in aggregations.