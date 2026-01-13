drop database if exists flipkart_analytics;
create database flipkart_analytics character set utf8mb4 collate utf8mb4_0900_ai_ci;
use flipkart_analytics;
drop table if exists flipkart_sale;
create table flipkart_sale (
uniq_id varchar(64),
crawl_timestamp varchar(64),
product_url Longtext,
product_name Longtext,
product_category_tree Longtext,
pid varchar(64),
retail_price varchar(64),
discounted_price varchar(64),
image longtext,
is_FK_Advantage_product varchar(64),
description longtext,
product_rating varchar(64),
overall_rating varchar(64),
brand varchar(255),
product_specification longtext ) character set utf8mb4 collate utf8mb4_unicode_ci; 

set global local_infile =1;

Load data local infile 'C:/Users/Owner/OneDrive/Desktop/Flipkart/flipkart_utf8.csv'
into table flipkart_sale
character set utf8mb4
fields 
	terminated by ',' 
    optionally enclosed by '"'
    escaped by '"'
lines 
	terminated by '\r\n'
ignore 1 lines
(
  uniq_id, crawl_timestamp, product_url, product_name,
  product_category_tree, pid,
  retail_price, discounted_price,
  image, is_FK_advantage_product, description,
  product_rating, overall_rating, brand, product_specification
)
SET product_specification = replace(product_specification, '\r', '');

 select * from flipkart_sale limit 5;
select count(*) from flipkart_products;

CREATE TABLE flipkart_product AS
SELECT
  uniq_id,
  crawl_timestamp,
  product_url,
  product_name,
  product_category_tree,
  pid,

  CAST(NULLIF(TRIM(retail_price),'') AS DECIMAL(12,2))       AS retail_price,
  CAST(NULLIF(TRIM(discounted_price),'') AS DECIMAL(12,2))  AS discounted_price,

  image,
  is_FK_advantage_product,
  description,

  CASE
    WHEN TRIM(product_rating) REGEXP '^[0-9]+(\\.[0-9]+)?$'
      THEN CAST(TRIM(product_rating) AS DECIMAL(4,2))
    ELSE NULL
  END AS product_rating,

  CASE
    WHEN TRIM(overall_rating) REGEXP '^[0-9]+(\\.[0-9]+)?$'
      THEN CAST(TRIM(overall_rating) AS DECIMAL(4,2))
    ELSE NULL
  END AS overall_rating,

  NULLIF(TRIM(brand),'') AS brand,
  product_specification,

  CASE
    WHEN CAST(NULLIF(TRIM(retail_price),'') AS DECIMAL(12,2)) > 0
      THEN ROUND(
        (
          CAST(NULLIF(TRIM(retail_price),'') AS DECIMAL(12,2)) -
          CAST(NULLIF(TRIM(discounted_price),'') AS DECIMAL(12,2))
        ) / CAST(NULLIF(TRIM(retail_price),'') AS DECIMAL(12,2)) * 100
      , 2)
    ELSE NULL
  END AS discount_pct
FROM flipkart_analytics.flipkart_sale; 


DROP TABLE IF EXISTS flipkart_product;

CREATE TABLE flipkart_product AS
SELECT
  uniq_id,
  crawl_timestamp,
  product_url,
  product_name,
  product_category_tree,
  pid,

  /* numeric-safe conversions */
  CASE
    WHEN retail_price REGEXP '^[0-9]+(\\.[0-9]+)?$' THEN CAST(retail_price AS DECIMAL(10,2))
    ELSE NULL
  END AS retail_price,

  CASE
    WHEN discounted_price REGEXP '^[0-9]+(\\.[0-9]+)?$' THEN CAST(discounted_price AS DECIMAL(10,2))
    ELSE NULL
  END AS discounted_price,

  image,
  is_FK_Advantage_product,
  description,

  CASE
    WHEN product_rating REGEXP '^[0-9]+(\\.[0-9]+)?$' THEN CAST(product_rating AS DECIMAL(3,2))
    ELSE NULL
  END AS product_rating,

  CASE
    WHEN overall_rating REGEXP '^[0-9]+(\\.[0-9]+)?$' THEN CAST(overall_rating AS DECIMAL(3,2))
    ELSE NULL
  END AS overall_rating,

  NULLIF(brand,'') AS brand,
  product_specification,

  /* discount % */
  CASE
    WHEN retail_price REGEXP '^[0-9]+(\\.[0-9]+)?$'
     AND discounted_price REGEXP '^[0-9]+(\\.[0-9]+)?$'
     AND CAST(retail_price AS DECIMAL(10,2)) > 0
    THEN ROUND(
      (CAST(retail_price AS DECIMAL(10,2)) - CAST(discounted_price AS DECIMAL(10,2)))
      / CAST(retail_price AS DECIMAL(10,2)) * 100
    , 2)
    ELSE NULL
  END AS discount_pct

FROM flipkart_sale;

Alter table flipkart_product
	add index idx_brand (brand(50)),
    add index idx_pid (pid(50));
    
CREATE OR REPLACE view flipkart_pbi AS
SELECT
  brand,
  COUNT(*) AS products,
  AVG(discount_pct) AS avg_discount_pct,
  AVG(overall_rating) AS avg_overall_rating,
  AVG(discounted_price) AS avg_selling_price
FROM flipkart_product
GROUP BY brand;



    
    show warnings;
    
    show global variables like 'local_infile';