psql --command="DROP DATABASE dwh_takeaway;" postgresql://postgres:admin@127.0.0.1:5432/
sleep 1
psql --command="CREATE DATABASE dwh_takeaway;" postgresql://postgres:admin@127.0.0.1:5432/
sleep 1
psql --command="\c dwh_takeaway;" postgresql://postgres:admin@127.0.0.1:5432/

sleep 1
psql --command="DROP TABLE IF EXISTS fct_reviews;" postgresql://postgres:admin@127.0.0.1:5432/dwh_takeaway
sleep 1
psql --command="CREATE TABLE fct_reviews(
  productID VARCHAR,
  rating INTEGER,
  reviewerID VARCHAR,
  reviewername VARCHAR,
  reviewTime DATE
);" postgresql://postgres:admin@127.0.0.1:5432/dwh_takeaway
sleep 1

psql --command="DROP TABLE IF EXISTS dim_reviewers;" postgresql://postgres:admin@127.0.0.1:5432/dwh_takeaway
sleep 1
psql --command="CREATE TABLE dim_reviewers(
  reviewerID VARCHAR NULL,
  reviewerName VARCHAR NULL
);" postgresql://postgres:admin@127.0.0.1:5432/dwh_takeaway
sleep 1

psql --command="DROP TABLE IF EXISTS dim_products;" postgresql://postgres:admin@127.0.0.1:5432/dwh_takeaway
sleep 1
psql --command="CREATE TABLE dim_products(
  productID VARCHAR,
  category VARCHAR,
  title VARCHAR,
  price DOUBLE PRECISION,
  bucketID DOUBLE PRECISION
);" postgresql://postgres:admin@127.0.0.1:5432/dwh_takeaway

sleep 1
psql --command="DROP TABLE IF EXISTS dim_pricebucket;" postgresql://postgres:admin@127.0.0.1:5432/dwh_takeaway
sleep 1
psql --command="CREATE TABLE dim_pricebucket(
  bucketNumber VARCHAR PRIMARY KEY,
  minPrice DOUBLE PRECISION,
  maxPrice DOUBLE PRECISION
);" postgresql://postgres:admin@127.0.0.1:5432/dwh_takeaway

sleep 1
psql --command="DROP TABLE IF EXISTS dim_categories;" postgresql://postgres:admin@127.0.0.1:5432/dwh_takeaway
sleep 1
psql --command="CREATE TABLE dim_categories(
  category VARCHAR PRIMARY KEY
);" postgresql://postgres:admin@127.0.0.1:5432/dwh_takeaway
