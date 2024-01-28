
use role ACCOUNTADMIN;
list @articles_s3_stage/;

use role ACCOUNTADMIN;
-- auto ingest data snowpip
--create or replace pipe LATESTARTICLES.ingestpip.article_pips auto_ingest=true as
copy into LATESTARTICLES.RAW_NEWSARTICLES.raw(
    filename,
    file_row_number,
    file_content_key, 
    file_last_modified ,
    start_scan_time,
    headlines,
    links,
    image,
    summary,
    retrieval_date, 
    website,
    country
)
from
(select 
    METADATA$FILENAME, 
    METADATA$FILE_ROW_NUMBER, 
    METADATA$FILE_CONTENT_KEY, 
    METADATA$FILE_LAST_MODIFIED, 
    METADATA$START_SCAN_TIME,
    articles.$1:headlines as headlines,
    articles.$1:links as links,
    articles.$1:image as image,
    articles.$1:summary as summary,
    to_timestamp(articles.$1:retrieval_date) as retrieval_date,
    articles.$1:website as website,
    articles.$1:maincountry as country
    from @articles_s3_stage/raw-transform-dataSunday-28-January-2024tranformdata.json
    (FILE_FORMAT => article_csv_format) articles);

SHOW PIPES;

-------------------------------------------------------------------------------------------
--- create stream to capture insert to the raw table

create or replace stream raw_table_stream1 on table LATESTARTICLES.RAW_NEWSARTICLES.raw;

-- confirm if stream have raw data
select * from raw_table_stream1;

create or replace task raw_table_to_enhance_table
warehouse = DATAENGINE
schedule = '2 minutes'
when 
system$stream_has_data('raw_table_stream1')
as
merge into LATESTARTICLES.enhance_NEWSARTICLES.enhance enh
 using(SELECT FILE_ROW_NUMBER,headlines,links, image,summary,
    retrieval_date,website,
    country
FROM raw_table_stream1) raw1 on enh.FILE_ROW_NUMBER = raw1.FILE_ROW_NUMBER
--- when column match
when matched then update set enh.headlines=raw1.headlines, enh.links=raw1.links, 
enh.image=raw1.image, enh.summary=raw1.summary, enh.retrieval_date=raw1.retrieval_date,
enh.website=raw1.website, enh.country=raw1.country
-- when column dont matct
when not matched then insert (
    FILE_ROW_NUMBER,headlines,links, image,summary,
    retrieval_date,website,
    country) 
    values (
        raw1.FILE_ROW_NUMBER, 
        raw1.headlines, raw1.links, 
        raw1.image, raw1.summary,
    raw1.retrieval_date, raw1.website,
    raw1.country
    );

truncate table LATESTARTICLES.RAW_NEWSARTICLES.raw;
