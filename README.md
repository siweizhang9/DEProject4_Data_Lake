# Sparkify ETL with PySpark
## Project Description
Sparkify is a music streaming company that has a huge amount of data that can't be easily handled with traditional RDBMS databases. The aim of this project is to create a data pipeline with PySpark that processes the data stored in JSON format on AWS S3 and creates dimensional and fact tables for a star schema which could be easily used for analytical purposes.

## Files Description
dl.cfg: Configuration file that contains AWS access keys.
etl.py: Python script that contains the ETL pipeline to process JSON files from AWS S3 and create dimensional and fact tables.
README.md: This file.
## Dataset Description
Song Data: JSON files stored in AWS S3 bucket. Each file contains metadata about a song and the artist of that song.
Log Data: JSON files stored in AWS S3 bucket. Each file contains logs on user activity on the Sparkify music streaming app.
## Schema Design
The schema design for this project is based on a star schema, which contains one fact table and four dimension tables. The fact table is songplays, and the dimension tables are users, songs, artists, and time.
##How to run the project
Add your AWS access keys to the dl.cfg file.
Run the etl.py script by typing python etl.py on your terminal.
##Technologies used
Python 3.7
PySpark 2.4.3
AWS S3
##Contact
If you have any questions or suggestions, feel free to reach out to me on [LinkedIn](https://www.linkedin.com/in/zhangsiwei/).
