CREATE OR REPLACE FUNCTION convert_unix_ts_to_seoul(unix_ts BIGINT)
RETURNS TIMESTAMP
IMMUTABLE
AS $$
  SELECT CONVERT_TIMEZONE('UTC', 'Asia/Seoul', TIMESTAMP 'epoch' + $1 * INTERVAL '1 second')
$$ LANGUAGE SQL;

MERGE INTO mart.tb_posts
USING (
    SELECT *
    FROM (
        SELECT 
            stage.*,
            ROW_NUMBER() OVER (PARTITION BY post_id ORDER BY post_id) AS rn
        FROM staging.tb_posts AS stage
    ) sub
    WHERE rn = 1
) AS stage
ON mart.tb_posts.post_id = stage.post_id
WHEN MATCHED THEN 
    UPDATE SET title       = stage.title,
               author      = stage.author,
               article     = stage.article,
               create_date = convert_unix_ts_to_seoul(create_timestamp)
WHEN NOT MATCHED THEN
    INSERT (post_id, title, author, article, create_date)
    VALUES (stage.post_id, stage.title, stage.author, stage.article, convert_unix_ts_to_seoul(create_timestamp));


MERGE INTO mart.tb_comments
USING (
    SELECT 
        sub.*
    FROM (
        SELECT 
            stage_p.post_id,
            stage_c.comment_id,
            stage_c.author,
            stage_c.content,
            stage_c.create_timestamp,
            ROW_NUMBER() OVER (PARTITION BY comment_id ORDER BY comment_id) AS rn
        FROM staging.tb_comments AS stage_c
            JOIN  staging.tb_posts AS stage_p ON stage_c.post_uuid = stage_p.post_uuid
    ) sub
    WHERE rn = 1
) AS stage
ON mart.tb_comments.comment_id = sub.comment_id
WHEN MATCHED THEN 
    UPDATE SET author      = stage.author,
               content     = stage.content,
               create_date = convert_unix_ts_to_seoul(stage.create_timestamp)
WHEN NOT MATCHED THEN
    INSERT (post_id, comment_id, author, content, create_date)
    VALUES (stage.post_id, stage.comment_id, stage.author, stage.content, convert_unix_ts_to_seoul(stage.create_timestamp));