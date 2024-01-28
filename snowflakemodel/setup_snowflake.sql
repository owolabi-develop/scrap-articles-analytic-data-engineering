use role ACCOUNTADMIN;
--CREATE WAREHOUSE
CREATE OR REPLACE WAREHOUSE ARTICLE_WH WAREHOUSE_SIZE = XSMALL, AUTO_SUSPEND = 300, AUTO_RESUME= TRUE;

--USE WAREHOUSE
USE WAREHOUSE ARTICLE_WH;

 -- CREATE DATABASE
CREATE OR REPLACE DATABASE LATESTARTICLES;

--DROP PUBLIC SCHEMA
DROP SCHEMA LATESTARTICLES.PUBLIC;
-- CREATE RAW_NEWSARTICLES
CREATE OR REPLACE SCHEMA LATESTARTICLES.RAW_NEWSARTICLES;

create or replace schema enhance_NEWSARTICLES;
CREATE OR REPLACE SCHEMA LATESTARTICLES.ingestpip;

USE SCHEMA LATESTARTICLES.RAW_NEWSARTICLES;

--CREATE FILE FORMAT 
CREATE OR REPLACE FILE FORMAT article_csv_format
  TYPE = 'JSON'
  STRIP_OUTER_ARRAY = TRUE;
  

--CREATE STAGE
CREATE or replace STAGE articles_s3_stage
  STORAGE_INTEGRATION = articles_int
  URL = 's3://latestarticles/';
  


-- create table table to load raw json file

create or replace table LATESTARTICLES.RAW_NEWSARTICLES.raw (
    id number default enhance1.NEXTVAL,
    filename varchar,
    file_row_number int,
    file_content_key varchar,
    file_last_modified timestamp_ntz,
    start_scan_time timestamp_ltz,
    headlines varchar(255),
    links string,
    image string,
    summary string,
    retrieval_date Date,
    website string,
    country string
);


--- create  SEQUENCE 
CREATE OR REPLACE SEQUENCE enhance1;
 --- create table to load clean json data
create or replace table LATESTARTICLES.enhance_NEWSARTICLES.enhance(
    id number default enhance1.NEXTVAL,
    file_row_number int,
    headlines varchar(255),
    links string,
    image string,
    summary string,
    retrieval_date Date,
    website string,
    country string
);









