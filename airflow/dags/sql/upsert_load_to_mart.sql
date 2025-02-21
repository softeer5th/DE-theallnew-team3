CREATE OR REPLACE FUNCTION convert_unix_ts_to_seoul(unix_ts BIGINT)
RETURNS TIMESTAMP
IMMUTABLE
AS $$
  SELECT CONVERT_TIMEZONE('UTC', 'Asia/Seoul', TIMESTAMP 'epoch' + $1 * INTERVAL '1 second')
$$ LANGUAGE SQL;

MERGE INTO mart.tb_posts
USING staging.tb_posts AS stage
ON mart.tb_posts.post_id = stage.post_id
WHEN MATCHED THEN 
    UPDATE SET  title = stage.title,
                author = stage.author,
                article = stage.article,
                create_date = convert_unix_ts_to_seoul(create_timestamp)
WHEN NOT MATCHED THEN
    INSERT (post_id, title, author, article, create_date)
    VALUES (stage.post_id, stage.title, stage.author, stage.article, convert_unix_ts_to_seoul(create_timestamp));


MERGE INTO mart.tb_comments
USING staging.tb_comments AS stage
ON mart.tb_comments.comment_id = stage.comment_id
WHEN MATCHED THEN 
    UPDATE SET  author = stage.author,
                content = stage.content,
                create_date = convert_unix_ts_to_seoul(create_timestamp)
WHEN NOT MATCHED THEN
    INSERT (comment_id, author, content, create_date)
    VALUES (stage.comment_id, stage.author, stage.content, convert_unix_ts_to_seoul(create_timestamp));