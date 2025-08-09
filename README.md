# IST3134-Group-Project

Hadoop Streaming with Python on EC2
Process customer ratings using Hadoop Streaming with Python mappers/reducers. Inputs/outputs live in Amazon S3; Hadoop runs on a single EC2 node.

What this job does?
mapper.py reads each CSV row, classifies ratings (4–5 positive, 1–2 negative, 3 neutral), and aggregates per movie_id.

reducer.py sums per-movie stats across mappers and outputs:


movie_id,avg_rating,positive_count,negative_count,neutral_count,total_count
Prerequisites
EC2 Linux instance with Hadoop installed (single-node/pseudo-distributed is fine)

Java 11, Python 3, AWS CLI

S3 bucket: project-bigdata-1 (region: us-east-1)

IAM role/credentials that can read/write this bucket

Setup & Run
Run these in order. Lines starting with # are just comments.

```bash
# Setup
sudo apt update -y
sudo apt install -y openjdk-11-jdk python3 python3-pip awscli
```

# Start Hadoop (single node)
```bash
sudo su - hadoop
start-all.sh
```

# Create a directory for the project
```bash
mkdir bigdata-project
cd bigdata-project
```

# Create directories for input and mapreduce files
```bash
mkdir input
mkdir mapreduce
```

# Enter the input directory and download the dataset from S3
```bash
cd input
aws s3 cp s3://project-bigdata-1/input/customer_ratings/customers_rating.csv . --region us-east-1
```

# Load dataset into HDFS
```
hdfs dfs -mkdir -p /user/hadoop/input
# NOTE: use the path where you actually saved the CSV. If you followed above, it’s in ~/bigdata-project/input/
hdfs dfs -put ~/bigdata-project/input/customers_rating.csv /user/hadoop/input/
```

# Enter the MapReduce directory and download the Python files from S3
```bash
cd ../mapreduce
aws s3 cp s3://project-bigdata-1/mapreduce/mapper.py . --region us-east-1
aws s3 cp s3://project-bigdata-1/mapreduce/reducer.py . --region us-east-1
```

# Run the MapReduce task
```bash
hadoop jar /home/hadoop/pig-0.17.0/test/e2e/pig/lib/hadoop-streaming.jar \
  -files mapper.py,reducer.py \
  -mapper "python3 mapper.py" \
  -reducer "python3 reducer.py" \
  -input /user/hadoop/input/customers_rating.csv \
  -output /user/hadoop/output/netflix_results
```

# (Optional) Preview first 20 lines of the output in HDFS
```bash
hdfs dfs -cat /user/hadoop/output/netflix_results/part-* | head -n 20
```

# Download the output locally and upload it to S3
```
hadoop fs -get /user/hadoop/output/netflix_results /home/hadoop/netflix_results
aws s3 cp /home/hadoop/netflix_results s3://project-bigdata-1/output/ --recursive
```
