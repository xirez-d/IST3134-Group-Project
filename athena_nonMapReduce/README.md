# Amazon Athena: Ratings & Titles Analysis (Serverless SQL on S3)

# Step 1: Creating Tables
A customers_ratings table will be created to store each customer's rating for a specific movie. The data is read directly from the CSV files stored in S3.
```
CREATE EXTERNAL TABLE customers_rating (
    customer_id INT,   -- Unique ID for the customer
    movie_id INT,      -- Unique ID for the movie
    rating INT         -- Rating value (likely 1 to 5)
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
WITH SERDEPROPERTIES (
  'separatorChar' = ',',  -- Columns are separated by commas
  'quoteChar' = '"'       -- Values may be quoted with double quotes
)
LOCATION 's3://project-bigdata-1/input/customer_ratings/'
TBLPROPERTIES ('skip.header.line.count' = '1');
```

A movies table will also be created to store the information of the movies
```
CREATE EXTERNAL TABLE movie_titles (
    movie_id INT,   -- Unique ID for the movie
    year STRING,    -- Release year (stored as string)
    title STRING    -- Movie title
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
WITH SERDEPROPERTIES (
  'separatorChar' = ',',  -- Columns are separated by commas
  'quoteChar' = '"'       -- Values may be quoted with double quotes
)
LOCATION 's3://project-bigdata-1/input/movie_titles/'
TBLPROPERTIES ('skip.header.line.count' = '0');
```

# Step 2: Data Verification
View the first 10 rows of each table to verify correct loading
```
SELECT * FROM customers_rating LIMIT 10;
SELECT * FROM movie_titles LIMIT 10;
```

# Step 3: Basic Join Test
Joins customer ratings with movie titles to check if matching works
```
SELECT 
    r.movie_id,
    t.title,
    r.rating
FROM customers_rating r
LEFT JOIN movie_titles t
    ON r.movie_id = t.movie_id
LIMIT 10;
```

# Step 4: Metrics Calculation
This section computes per-movie statistics: average rating, positive %, negative % and total ratings.
-> Positive rating: rating >= 4
-> Negative rating: rating <= 2
```
WITH rating_stats AS (
    SELECT
        r.movie_id,
        t.title,
        ROUND(AVG(r.rating), 2) AS avg_rating,                           -- Average rating (rounded to 2 decimals)
        COUNT(CASE WHEN r.rating >= 4 THEN 1 END) AS positive_count,     -- Number of positive ratings
        COUNT(CASE WHEN r.rating <= 2 THEN 1 END) AS negative_count,     -- Number of negative ratings
        COUNT(*) AS total_ratings                                        -- Total ratings for the movie
    FROM customers_rating r
    LEFT JOIN movie_titles t
        ON r.movie_id = t.movie_id
    GROUP BY r.movie_id, t.title
)
SELECT
    movie_id,
    title,
    avg_rating,
    ROUND(positive_count * 100.0 / total_ratings, 2) AS positive_percentage, -- Positive ratings percentage
    ROUND(negative_count * 100.0 / total_ratings, 2) AS negative_percentage, -- Negative ratings percentage
    total_ratings
FROM rating_stats
WHERE total_ratings >= 100  -- Consider movies with at least 100 ratings
ORDER BY avg_rating DESC;
```

# Step 5: Leaderboard Analysis
# Calculation of the ratings
```
WITH rating_stats AS (
    SELECT
        r.movie_id,
        t.title,
        ROUND(AVG(r.rating), 2) AS avg_rating,  ## Average rating
        ROUND(100.0 * COUNT(CASE WHEN r.rating >= 4 THEN 1 END) / COUNT(*), 2) AS positive_percentage, -- % of ratings >= 4
        ROUND(100.0 * COUNT(CASE WHEN r.rating <= 2 THEN 1 END) / COUNT(*), 2) AS negative_percentage, -- % of ratings <= 2
        COUNT(*) AS total_ratings
    FROM customers_rating r
    JOIN movie_titles t
        ON r.movie_id = t.movie_id
    GROUP BY r.movie_id, t.title
    HAVING COUNT(*) >= 100  ## Only include movies with at least 100 ratings
),
```

# Analysis 1: Best Performing Movies

Top 3 movies by highest average rating:
```
high_avg AS (
    SELECT 'Top 3 Highest Avg Ratings' AS category, title, avg_rating, positive_percentage, negative_percentage
    FROM rating_stats
    ORDER BY avg_rating DESC
    LIMIT 3
),
```

Top 3 movies by highest positive rating percentage:
```
high_positive AS (
    SELECT 'Top 3 Highest Positive %' AS category, title, avg_rating, positive_percentage, negative_percentage
    FROM rating_stats
    ORDER BY positive_percentage DESC
    LIMIT 3
),
```

# Analysis 2: Worst Performing Movies

Top 3 movies by lowest average rating:
```
low_avg AS (
    SELECT 'Top 3 Lowest Avg Ratings' AS category, title, avg_rating, positive_percentage, negative_percentage
    FROM rating_stats
    ORDER BY avg_rating ASC
    LIMIT 3
),
```

Top 3 movies by highest negative rating percentage:
```
high_negative AS (
    SELECT 'Top 3 Highest Negative %' AS category, title, avg_rating, positive_percentage, negative_percentage
    FROM rating_stats
    ORDER BY negative_percentage DESC
    LIMIT 3
)
```

# Combine all leaderboards into one result set
```
SELECT * FROM high_avg
UNION ALL
SELECT * FROM high_positive
UNION ALL
SELECT * FROM low_avg
UNION ALL
SELECT * FROM high_negative;
```


