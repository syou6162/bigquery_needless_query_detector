## bigquery_needless_query_detector
`bigquery_needless_query_detector` is a command line tool to detect needless queries, which generates tables but nobody use them. 

## Install

```sh
% go install github.com/syou6162/bigquery_needless_query_detector@latest
```

## Usage

```sh
% ./bigquery_needlessness_query_detector \
    --project ml-news --region asia-northeast1 \
    --min_distance_threshold 10 \
    --creation_time 2021-02-07 --type PROJECT > out.json

% cat out.json | \
    jq -c 'sort_by(.total_bytes_processed) | reverse | .[] | select(.total_bytes_processed > 0) | {"total_bytes_processed": .total_bytes_processed, "query": .query[0:50], "count": .count, "user_email": .user_email, "destination_table": .destination_table}' | \
    head -n 2 | \
    jq .
{
  "total_bytes_processed": 84113438269,
  "query": "create or replace table `ml-news.mart.google_bot_e",
  "count": 134,
  "user_email": "ml-news-dataform@ml-news.iam.gserviceaccount.com",
  "destination_table": "ml-news:mart.google_bot_example_page"
}
{
  "total_bytes_processed": 40423015212,
  "query": "/* {\"app\": \"dbt\", \"dbt_version\": \"0.20.0\", \"profil",
  "count": 361,
  "user_email": "dbt-runner@ml-news.iam.gserviceaccount.com",
  "destination_table": "ml-news:test_test.examples_with_tweet_stats_v_0_0_2"
}
```
