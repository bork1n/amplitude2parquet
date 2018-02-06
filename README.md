# amplitude2parquet
Simple script to convert Amplitude's exported JSON files to a Parquet format for analytics pusposes. Analytics can be done with AWS Athena or an in-house cluster.

Gets list of files from command-line, and saves parquet files in a directory `parquet` with the same filepath.


It's advised to store files under directories like `year=2018/month=01/day=30/hour=10`

# Usage

1. `find -type f -name \*json.gz | xargs python3 convert_to_parquet.py`

2. Create table based on schema - execute printed SQL-statement

3. Upload files to AWS S3 (if needed)
`aws s3 cp parquet s3://bucketname/parquet/`



# TODOs
- move schema out of script
- fix `timestamp` field
- add support for embedded structures (metadata, etc). Currently is ignored.


# Links
- https://github.com/andrewgross/json2parquet
- https://parquet.apache.org/
- https://aws.amazon.com/athena/
