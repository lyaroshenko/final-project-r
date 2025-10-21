library(duckdb)
con <- dbConnect(duckdb::duckdb())

dbExecute(con, "INSTALL httpfs; LOAD httpfs; SET enable_http_metadata_cache=true;")

dbExecute(con, "CREATE TABLE taxi_jan AS SELECT * FROM read_parquet('C:\\Users\\zhuko\\Documents\\final project\\data\\taxi_trip_data_jan_2023.parquet')")
dbExecute(con, "CREATE TABLE taxi_feb AS SELECT * FROM read_parquet('C:\\Users\\zhuko\\Documents\\final project\\data\\taxi_trip_data_feb_2023.parquet')")
dbExecute(con, "SET unsafe_disable_etag_checks = true;")
dbExecute(con, "CREATE TABLE traffic_speed AS SELECT * FROM read_csv_auto('https://data.cityofnewyork.us/resource/i4gi-tjb9.csv')")
dbExecute(con, "CREATE TABLE crash_data AS SELECT * FROM read_csv_auto('https://data.cityofnewyork.us/resource/h9gi-nx95.csv')")
dbExecute(con, "CREATE TABLE taxi_all AS SELECT * FROM taxi_jan UNION ALL SELECT * FROM taxi_feb")
dbExecute(con, "INSTALL json")
dbExecute(con, "LOAD json")
dbExecute(con, "CREATE TABLE taxi_zones AS SELECT * FROM read_json_auto('C:/Users/zhuko/Documents/final project/data/taxi_zones.geojson')")

dbGetQuery(con, "PRAGMA table_info('traffic_speed')")

dbExecute(con, "
  CREATE TABLE taxi_with_speed AS
  SELECT t.*, s.*
  FROM taxi_all t
  LEFT JOIN traffic_speed s
  ON DATE(t.tpep_pickup_datetime) = DATE(s.data_as_of)
  AND CAST(t.PULocationID AS VARCHAR) = CAST(s.link_id AS VARCHAR)
")
dbGetQuery(con, "PRAGMA table_info('taxi_zones')")
dbGetQuery(con, "SELECT features[1].properties FROM taxi_zones")
dbExecute(con, "
  CREATE TABLE flattened_zones AS
  SELECT 
    f.value.properties.locationid::INT AS location_id,
    f.value.properties.zone AS zone,
    f.value.properties.borough AS borough
  FROM taxi_zones,
  UNNEST(taxi_zones.features) AS f(value)
")
dbExecute(con, "
  CREATE TABLE taxi_enriched AS
  SELECT t.*, z.zone AS pickup_zone, z.borough AS pickup_borough
  FROM taxi_with_speed t
  LEFT JOIN flattened_zones z
  ON t.PULocationID = z.location_id
")
