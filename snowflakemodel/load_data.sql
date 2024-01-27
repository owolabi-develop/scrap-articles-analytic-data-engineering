

list @articles_s3_stage/;


copy into raw
    from @articles_s3_stage/
    FILE_FORMAT = (FORMAT_NAME = article_csv_format);
   

select articles:headlines::string as headlines, articles:links::string as links from raw;