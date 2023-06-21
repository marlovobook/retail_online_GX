CREATE SCHEMA IF NOT EXISTS dbo;

DROP TABLE IF EXISTS dbo.table_product_demand;

CREATE TABLE dbo.table_product_demand (
  shop_id VARCHAR(100),
  date TIMESTAMP,
  product_name VARCHAR(100),
  demand VARCHAR(100)
);

COPY dbo.table_product_demand(shop_id, date, product_name, demand)
FROM '/data/table_product_demand.csv' DELIMITER ',' CSV HEADER;


set datestyle = 'ISO, YMD';
CREATE SCHEMA IF NOT EXISTS wh;


DROP TABLE IF EXISTS dbo.table_online_retail_origin;

            
CREATE TABLE dbo.table_online_retail_origin (
                id INT,
                Invoice VARCHAR(100),
                StockCode VARCHAR(100),
                Description VARCHAR(100),
                Quantity INT,
                InvoiceDate DATE DEFAULT (CURRENT_DATE::DATE),
                Price FLOAT,
                Customer_ID VARCHAR(100),
                Country VARCHAR(100),
                last_updated DATE DEFAULT (CURRENT_DATE::DATE),
                operation char(1),
                constraint table_online_retail_origin_pk primary key (id)
            );

COPY dbo.table_online_retail_origin(id, Invoice, StockCode, Description,Quantity,InvoiceDate,Price,Customer_ID,Country,last_updated)
FROM '/data/online_retail_origin.csv' DELIMITER ',' CSV HEADER;


DROP TABLE IF EXISTS dbo.table_online_retail_stage;

CREATE TABLE dbo.table_online_retail_stage (
                id INT,
                Invoice VARCHAR(100),
                StockCode VARCHAR(100),
                Description VARCHAR(100),
                Quantity INT,
                InvoiceDate DATE DEFAULT (CURRENT_DATE::DATE),
                Price FLOAT,
                Customer_ID VARCHAR(100),
                Country VARCHAR(100),
                last_updated DATE DEFAULT (CURRENT_DATE::DATE),
                operation char(1),
                constraint table_online_retail_stage_pk primary key (id, last_updated)
            );

COPY dbo.table_online_retail_stage(id, Invoice, StockCode, Description, Quantity, InvoiceDate, Price, Customer_ID, Country, last_updated, operation)
FROM '/data/online_retail_stage.csv' DELIMITER ',' CSV HEADER;


DROP TABLE IF EXISTS wh.table_online_retail_origin;
CREATE TABLE wh.table_online_retail_origin (
                id INT,
                Invoice VARCHAR(100),
                StockCode VARCHAR(100),
                Description VARCHAR(100),
                Quantity INT,
                InvoiceDate DATE DEFAULT (CURRENT_DATE::DATE),
                Price FLOAT,
                Customer_ID VARCHAR(100),
                Country VARCHAR(100),
                last_updated DATE DEFAULT (CURRENT_DATE::DATE),
                operation char(1),
                constraint table_online_retail_origin_pk primary key (id)
            );
