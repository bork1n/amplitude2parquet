from json2parquet import ingest_data
import json
import sys
import os
import pyarrow as pa
import pyarrow.parquet as pq
import re
import gzip

scheme_data = [
    ["event_id", "int64"], ["idfa", "string"], [
        "server_upload_time", "timestamp[ns]"],
    ["amplitude_event_type", "string"], ["app", "int64"],
    ["country",     "string"],
    ["ip_address", "string"], ["device_brand", "string"],
    ["client_event_time", "timestamp[ns]"], [
        "uuid", "string"], ["language", "string"],
    ["device_type", "string"], ["$schema", "int64"], ["paying", "string"],
    ["location_lat", "float64"], ["device_model", "string"],
    ["processed_time", "timestamp[ns]"], ["amplitude_id", "int64"],
    ["location_lng", "float64"],
    ["start_version", "string"], ["device_family", "string"],
    ["client_upload_time", "timestamp[ns]"], ["session_id", "int64"], ["device_carrier", "string"], ["user_creation_time", "timestamp[ns]"], ["device_id", "string"], ["adid", "string"], ["os_name", "string"], ["platform", "string"], ["user_id", "string"], ["library", "string"], ["is_attribution_event", "int8"], ["sample_rate", "string"], ["os_version", "string"], ["$insert_id", "string"], ["dma", "string"], ["event_time", "timestamp[ns]"], ["device_manufacturer", "string"], ["city", "string"], ["event_type", "string"], ["region", "string"], ["version_name", "string"]]


def do_schema():
    l = []
    print("generating schema")
    sql = """
CREATE EXTERNAL TABLE `amplitude_parquet`(
%s
  )
ROW FORMAT SERDE
  'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
STORED AS INPUTFORMAT
  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat'
OUTPUTFORMAT
  'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
LOCATION
  's3://test-amplitude111/parquet/'
PARTITIONED BY(year int, month int, day int, hour int)
TBLPROPERTIES (
  'has_encrypted_data'='false',
  "parquet.compress"="GZIP"
  )
  """
    sql_fields = []
    for d in scheme_data:
        l.append(pa.field(d[0], d[1]))
        t = d[1]
        t = t.replace('[ns]', '')
        t = t.replace('int8', 'smallint')
        t = t.replace('int64', 'bigint')
        t = t.replace('float64', 'double')
        sql_fields.append("`%s` %s" % (d[0], t))

    print(sql % (",".join(sql_fields)))

    schema = pa.schema(l)
    return schema


def process_file(filename):
    json_data = []
    parquetfile = 'parquet/' + filename + '.parquet'
    if os.path.exists(parquetfile):
        return ""
    with gzip.open(filename, "r") as f:
        for line in f:
            decoded_line = line.decode()
            if decoded_line:
                # print(line.decode())
                a = json.loads(decoded_line)
                json_data.append(a)

    data = ingest_data(json_data, schema)
    table = pa.Table.from_batches([data])
    directory = os.path.dirname(parquetfile)
    if not os.path.exists(directory):
        os.makedirs(directory)
    pq.write_table(table, parquetfile, compression='gzip')
    return ("Processing %s\n" % (filename))


pattern = re.compile("time")
schema = do_schema()
from multiprocessing import Pool

with Pool(2) as p:
    print("".join(p.map(process_file, sys.argv[1:])))
    # for i in range(1, len(sys.argv)):
    # process_file(sys.argv[i])


"""
OK:

SELECT user_id, SUM(1)
FROM amplitude."amplitude_parquet"
where event_type like 'PIN_OPEN'
group by user_id
HAVING sum(1)>10

select user_id from amplitude_parquet where user_id in (
select user_id from amplitude_parquet WHERE
   event_type = 'PIN_OPEN'
   GROUP BY user_id, event_type
   HAVING
    SUM(1) > 10
  ) and

   event_type = 'HINT_ALBUM_VIEW'
   GROUP BY user_id, event_type
   HAVING
    SUM(1) > 100

#  (Run time: 10.08 seconds, Data scanned: 700.12MB)


CREATE EXTERNAL TABLE `amplitude_parquet`(
  `event_id` bigint,
  `idfa` string,
  `server_upload_time` timestamp,
  `amplitude_event_type` string,
  `app` bigint,
  `country` string,
  `ip_address` string,
  `device_brand` string,
  `client_event_time` timestamp,
  `uuid` string,
  `language` string,
  `device_type` string,
  `$schema` bigint,
  `paying` string,
  `location_lat` double,
  `device_model` string,
  `processed_time` timestamp,
  `amplitude_id` bigint,
  `location_lng` double,
  `start_version` string,
  `device_family` string,
  `client_upload_time` timestamp,
  `session_id` bigint,
  `device_carrier` string,
  `user_creation_time` timestamp,
  `device_id` string,
  `adid` string,
  `os_name` string,
  `platform` string,
  `user_id` string,
  `library` string,
  `is_attribution_event` smallint,
  `sample_rate` string,
  `os_version` string,
  `$insert_id` string,
  `dma` string,
  `event_time` timestamp,
  `device_manufacturer` string,
  `city` string,
  `event_type` string,
  `region` string,
  `version_name` string)
PARTITIONED BY (
  `year` int,
  `month` int,
  `day` int,
  hour int)
ROW FORMAT SERDE
  'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
STORED AS INPUTFORMAT
  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat'
OUTPUTFORMAT
  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
LOCATION
  's3://test-amplitude111/parquet'
TBLPROPERTIES (
  'has_encrypted_data'='false',
  'parquet.compress'='GZIP',
  'transient_lastDdlTime'='1517684779')
"""
