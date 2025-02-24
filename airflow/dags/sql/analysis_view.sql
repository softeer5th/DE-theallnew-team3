CREATE MATERIALIZED VIEW social_listening AS
    SELECT
        k.post_id,
        CASE WHEN k."type" = 'comment' THEN k.comment_id ELSE NULL END AS comment_id,
        k."type",
        k.sentence,
        k.category,
        k.sentiment_score,
        CASE 
            WHEN k."type" = 'post' THEN p.create_date
            WHEN k."type" = 'comment' THEN cm.create_date
        END AS create_date,
        k.ingestion_date,
        k.car_name,
        c.pre_car_name,
        c.release_date,
        k.source,
        w.age
    FROM mart.tb_keywords AS k
    JOIN mart.tb_car AS c ON k.car_name = c.car_name
    JOIN mart.tb_web_source AS w ON k.source = w.source
    JOIN mart.tb_posts AS p ON k.post_id = p.post_id
    LEFT JOIN (
        SELECT c.comment_id, c.create_date
        FROM mart.tb_comments c
    ) AS cm ON k.comment_id = cm.comment_id;
