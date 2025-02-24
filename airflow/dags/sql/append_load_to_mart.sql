INSERT INTO mart.tb_posts_metric(
    post_id,
    car_name,
    source,
    like_cnt,
    dislike_cnt,
    view_cnt,
    comment_cnt,
    ingestion_date
)
SELECT post_id,
    car_name,
    source,
    like_cnt,
    dislike_cnt,
    view_cnt,
    comment_cnt,
    CURRENT_DATE as ingestion_date
FROM staging.tb_posts
ORDER BY car_name, source;

INSERT INTO mart.tb_comments_metric(
    post_id,
    comment_id,
    car_name,
    source,
    like_cnt,
    dislike_cnt,
    ingestion_date
)
SELECT p.post_id,
    c.comment_uuid,
    p.car_name,
    p.source,
    c.like_cnt,
    c.dislike_cnt,
    CURRENT_DATE as ingestion_date
FROM staging.tb_comments as c
    JOIN staging.tb_posts as p
        ON p.post_uuid = c.post_uuid
ORDER BY car_name, source;

INSERT INTO mart.tb_keywords(
    post_id,
    comment_id,
    car_name,
    source,
    type,
    sentence,
    category,
    keyword,
    sentiment_score,
    ingestion_date
)
SELECT p.post_id,
    CASE
        WHEN s."type" = 'post' THEN NULL
        WHEN s."type" = 'comment' THEN c.comment_id
    END AS comment_id,
    p.car_name,
    p.source,
    s."type",
    s.sentence,
    k.category,
    k.keyword,
    k.sentiment_score,
    CURRENT_DATE as ingestion_date
FROM staging.tb_sentences as s
    JOIN staging.tb_keywords as k
        ON s.sentence_uuid = k.sentence_uuid
    JOIN staging.tb_posts as p
        ON s.post_uuid = p.post_uuid
    LEFT JOIN staging.tb_comments as c
        ON s.comment_uuid = c.comment_uuid
ORDER BY car_name, source;