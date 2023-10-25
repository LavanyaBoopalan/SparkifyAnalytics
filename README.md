# Sparkify Analytical Database

## Objective:

A music streaming startup, Sparkify has grown their user base and song database.
This database need to be moved to cloud and need to meet the anaytical needs of their business.

The source data is present in the Amazon S3 tables.The source data has to be moved from S3 to intermediate staging tables in Amazon Redshift. Then feed the data from the staging tables to the analytical tables in the Amazon Redshift.

## Project Steps: 
1. Preparation of AWS environment
2. Creation of Staging and Analytical tables
3. ETL of Sparkify JSON data to Staging and Analytical Tables
4. Performing Analytics on Sparkify data

###  Preparation of AWS environment

1)  A new IAM user has to be created in AWS. The user's access key and secret has to be copied from AWS and added to the config file (dwh.cfg)

2) **aws_setup.ipynb** is responsible for the following tasks in AWS:

-   Creation of the IAM role
-   Creation the Redshift Cluster
-   Deletion of the IAM role (when cluster is no longer needed)
-   Deletion of the Redshift Cluster (when cluster is no longer needed)
 
###  Creation of Staging and Analytical tables

Data from Sparkify application is present in the following JSON format:

Song data: s3://udacity-dend/song_data
Log data: s3://udacity-dend/log_data
Metadata information for Log data : s3://udacity-dend/log_json_path.json

1) Following Staging tables are created in Redshift cluster based on the Song and Log json data files. 

- staging_events
- staging_songs

Points considered during table schema design:
- Column names are defined in the staging tables based on the name value pair in the  respective JSON.
- All the Numeric values in the JSON are defined as Integer data type for the Staging table columns.
- All the String values in the JSON are defined as Varchar data type for the Staging table columns. Size of the Varchar data type is decided based on the string length of the JSON values.
- All the decimal values in the JSON are defined as Double data type for the Staging table columns.

2) Following Analytical tables are created in Redshift cluster:
- songplays
- users 
- songs 
- artists
- timelog

Tables such as songplays, timelog are defined with Distribution style ***even*** since these tables can grow over the period of time and so it can be partitioned over the cluster nodes.
Tables such as users , songs , artists are defined with Distribution style *ALL* since these tables are smaller and can be referred easily when the data is present in each node.

**create_tables.py** -Execution of this script creates the staging tables and anlaytical tables in the Redshift cluster.

### ETL of Sparkify JSON data to Staging and Analytical Tables

**etl.py** - Execution of this script copies the JSON data from s3 to the staging tables and then insert data from the staging tables to the analytical tables.

Other files present in the workspace:

**sql_queries.py** - create_tables.py and etl.py internaly calls the the sql_queries.py to execute the needed queries.
**dwh.cfg** - Config file with all the environment related configurations.

### Performing Analytics on Sparkify data

Following analysis are performed on the  analytical tables:

Usecase 1: Total Number of records in the Staging tables & Dimensional tables
Usecase 2: What time of the day is the more number of users listening to the song
Usecase 3: Analyse the details of the free subscription users
Usecase 4: Analyse the most popular artist in the songplay

**analytics.ipynb **- This script can be run for performing analysis on the analytical tables.

## Project Execution

1)  A new IAM user has to be created in AWS. The user's access key and secret has to be copied from AWS and added to the config file (dwh.cfg)
2) Execute **aws_setup.ipynb** to create IAM role and AWS redshift cluster.
3) Add the IAM role and cluster details to the dwh.cfg
4) Execute **create_tables.py** from the notebook terminal.
5) Execute **etl.py** from the notebook terminal.
6) Execute **analytics.ipynb** to check the analytical table data and the analytical queries.








