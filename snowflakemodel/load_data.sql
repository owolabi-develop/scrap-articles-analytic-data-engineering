
use role ACCOUNTADMIN;
list @articles_s3_stage/;

use role ACCOUNTADMIN;
-- auto ingest data snowpip
create or replace pipe LATESTARTICLES.ingestpip.article_pips auto_ingest=true as
copy into LATESTARTICLES.RAW_NEWSARTICLES.raw
    from @articles_s3_stage/
    FILE_FORMAT = (FORMAT_NAME = article_csv_format);
    
-- show pip to copy the sqs arn
SHOW PIPES;

SELECT 
    articles:headlines::STRING AS headlines,
    articles:links::STRING AS links,
    articles:image::STRING AS image,
    articles:summary::STRING AS summary,
    TO_TIMESTAMP(articles:retrieval_date) AS retrieval_date,
    articles:website::STRING AS website,
    articles:maincountry::STRING AS country
FROM LATESTARTICLES.RAW_NEWSARTICLES.raw;



