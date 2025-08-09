# Project Description (Python MapReduce + Amazon Athena)
This project analyzes customer movie ratings stored in Amazon S3 using two complementary approaches:

Python MapReduce (Hadoop Streaming on EC2):
A single-node Hadoop setup runs Python mapper.py and reducer.py. The mapper reads CSV rows, classifies ratings (4–5 positive, 1–2 negative, 3 neutral) and aggregates per movie_id. The reducer sums partials from all mappers and computes final metrics: average rating, positive/negative/neutral counts, and totals. Outputs (CSV) are written to HDFS, fetched to EC2, and uploaded back to S3 for persistence.

Amazon Athena (Serverless SQL on S3):
External tables are defined over the same S3 CSVs (customers_rating, movie_titles). SQL queries (JOIN + GROUP BY) compute per-movie averages, positive/negative percentages, and leaderboards (top/bottom by average or sentiment). A CTAS step can materialize results to S3 in Parquet for faster, cheaper querying.


# File Structure in S3
As both approaches utilzes S3 as a storage, it is important to acknowledge the directories and file names.
```
s3://project-bigdata-1/
├─ input/
│  ├─ customer_ratings/
│  │  └─ customers_rating.csv
│  └─ titles/ 
│     └─ movies_titles.csv
├─ mapreduce/
│  ├─ mapper.py
│  └─ reducer.py
└─ output/
   └─ netflix_results/
      ├─ _SUCCESS
      └─ part-00000
```
