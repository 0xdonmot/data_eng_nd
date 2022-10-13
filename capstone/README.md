# Data Engineering Capstone

### Table of Contents

1. [Instructions](#instructions)
2. [Project Summary](#summary)
3. [File Descriptions](#files)
4. [Licensing, Authors, and Acknowledgements](#licensing)

### Instructions:

1. There are a couple of extra libraries needed to run the code here beyond the Anaconda distribution of Python:

- pyspark
- cassandra

### Project Summary<a name="summary"></a>

- This project extracts data from four csv files, explores and cleans them, then loads them to a Apache Cassandra cluster.
- The project is meant to define all the building blocks to fully automate this process by spinning up a cluster in the cloud and incorporating Airflow. As such, the full data was not loaded into the Cassandra cluster created.
- The end solution is a Cassandra keyspace optimised for particular queries. The script would need to be adjusted if other types of queries would be more relevant.
- This repo only contains the exploration etl script. However, the underlying data source links can be found inside the notebook file.

### Licensing, Authors, Acknowledgements<a name="licensing"></a>

The data used comes from Kaggle, the US National Tourism and Trade Office, OpenSoft, and datahub.io.
