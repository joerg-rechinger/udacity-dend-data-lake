## Data Lake Project
### Udacity Data Engineering Nanodegree

#### Starting point
The fictitous music streaming startup **Sparkify** has grown their userbase and songplays. So far the data was kept in a Data Warehouse and should now be moved to a Data Lake. Currently the data resides in S3 in json format. The data covers usage data of the streaming app as well as data about the songs and artists. The goal is build a data pipeline, loading the data from S3, processing it within Spark and then saving it back to S3 in the format of parquet files that can be queried by the Analytics team at **Sparkify**.

#### How to run the script
The repository consits of three files (in addition to README.md):
- dl.cfg: If you want to run **etl.py** locally on your machine, fill in your AWS credentials into this file (for being able to access udacity's s3 bucket with the input data).
- etl.py: This script contains the ETL pipeline. Read more about how it works below. If you want to run the script locally, uncomment the lines as suggested in the script to run configParser with your AWS credentials from dl.cfg. **Important**: Also fill in your S3 bucket for the path of output_data so that the output gets written to your S3 bucket.
- bootstramp_emr.sh: If you want to run the script on an EMR cluster, use this fill when creating the cluster to have a Spark version up and running within the cluster.

#### EMR cluster setup
Instead of running the script on your local machine, you can also run it on an EMR cluster from AWS. To do so, follow the steps below:
1. Upload **etl.py** and **bootstrap_emr.sh** to a private S3 bucket owned by you
2. Create a new cluster in EMR (via the advanced options). Configure it with Spark, Hadoop, Hive and Livy, using the latest release of EMR available.
3. Make sure to include the bootstrap_emr.sh from your S3 bucket as a bootstrapping action when launching the cluster. Also include your private pem file so that you can SSH into the cluster.
4. Once the cluster is launched, SSH into the cluster. Hit the command "spark-submit + path/to/your/S3/etl.py/file" (e.g. "spark-submit S3://my-bucket/etl.py")

#### Schema design and ETL pipeline
The schema is designed in the well-known star schema, consisting of fact and dimension tables. This reduces redundancy in the saved data as well as reduces number of joins necessary to answer analytics question. The fact table contains the primary keys of the dimension tables so that they can be joined.

Within **etl.py** first a Spark Session is established. After that the script consists of two parts: first dealing with the song data and after that with the log data. The steps are the same:
- First the data is loaded from Udacity's S3 bucket (to reduce runtime, for both cases the smaller buckets are chosen instead of the full-blown versions).
- After that it is transformed and only the relevant columns are chosen for the respective tables.
- Then the tables are saved back to S3 in the format of parquet files (columnar storage).
