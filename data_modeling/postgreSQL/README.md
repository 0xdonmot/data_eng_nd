# Sparkify Relational Database ETL

### Table of Contents

1. [Instructions](#instructions)
2. [Project Summary](#summary)
3. [File Descriptions](#files)
4. [Schema Design](#design)

### Instructions:
1. Run the following commands in the project's root directory to create your database, its relevant tables, and to insert relevant data into the tables.

    - To create the database and its tables
        `python create_tables.py`
    - To insert data into the tables
        `python etl.py`


### Project Summary<a name="summary"></a>
Sparkify would like to analyse their application data and have been collecting song and user activity data through the Sparkify app. This data currently resides in directories of JSON logs.

This ETL pipeline creates a Postgres database and relevant tables using a star schema format. Then, it inserts all relevant data into these tables.

The end result should be a database that can be queried for analysis.

### File Descriptions<a name="files"></a>
There is are various files available in this repo:

- data: a folder containing the user and song activity JSON logs.
- create_tables.py: a python file which creates a Postgres database and relevant tables
- etl.py: a python file which defines how to insert data into the Postgres database.
- sql_queries.py: a file containing SQL queries which are later imported and used in other files.
- etl.ipynb: an exploration jupyter notebook file, used to create the etl.py file.
- test.ipynb: a jupyter notebook file which contains standard tests used whilst designing the etl process.

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