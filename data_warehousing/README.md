# Sparkify Relational Database Redshift ETL

### Table of Contents

1. [Instructions](#instructions)
2. [Project Summary](#summary)
3. [File Descriptions](#files)
4. [Schema Design](#design)

### Instructions:
1. Run the following commands in the project's root directory to create the database in , its relevant tables, and to insert relevant data into the tables.

    - Ensure the dwh.cfg file is populated.
    - To create the tables in Redshift
        `python create_tables.py`
    - To insert data into the tables
        `python etl.py`


### Project Summary<a name="summary"></a>
Sparkify is a music streaming startup, that would like to move their data and processes onto the cloud given their user base growth.

The data currently resides in S3 - the Amazon object storage service that stores data in buckets - in a directory of JSON logs on user activity and a directory with JSON metadata on songs.

This ETL pipeline:
- reads data from these directories and writes the data into equivalent staging tables in Redshift.
- reads data from these newly created tables and writes them into a star schema format.

The end result should be a database that can be queried for analysis.

### File Descriptions<a name="files"></a>
There is are various files available in this repo:

- dwh.cfg: a settings file containing information on the Redshift cluster settings.
- create_tables.py: a python file which creates tables in the database hosted on Redshift
- etl.py: a python file which connects to the database hosted on Redshift and loads data into the staging tables and star schema tables.
- sql_queries.py: a file containing SQL queries which are later imported and used in other files.

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