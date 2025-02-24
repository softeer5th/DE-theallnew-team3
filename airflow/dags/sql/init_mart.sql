-- DO NOT USE --

CREATE TABLE IF NOT EXISTS mart.tb_posts(
    post_id       VARCHAR(255)      PRIMARY KEY,
    title         VARCHAR(255)      NOT NULL,
    author        VARCHAR(255)      NOT NULL,
    article       VARCHAR(65535)    NOT NULL,
    create_date   DATE              NOT NULL
);

CREATE TABLE IF NOT EXISTS mart.tb_comments(
    post_id       VARCHAR(255)      NOT NULL,
    comment_id    VARCHAR(255)      NOT NULL,
    author        VARCHAR(255)      NOT NULL,
    content       VARCHAR(65535)    NOT NULL,
    create_date   DATE              NOT NULL,

    PRIMARY KEY (post_id, comment_id)
);

CREATE TABLE IF NOT EXISTS mart.tb_car(
    car_name        VARCHAR(255) PRIMARY KEY,
    pre_car_name    VARCHAR(255) NULL,
    release_date    DATE        NOT NULL
);

CREATE TABLE IF NOT EXISTS mart.tb_web_source(
    source VARCHAR(255) PRIMARY KEY,
    age    VARCHAR(255) NOT NULL
);

CREATE TABLE IF NOT EXISTS mart.tb_posts_metric(
    post_id         VARCHAR(255) NOT NULL,
    car_name        VARCHAR(255) NOT NULL,
    source          VARCHAR(255) NOT NULL,
    like_cnt        BIGINT NULL,
    dislike_cnt     BIGINT NULL,
    view_cnt        BIGINT NULL,
    comment_cnt     BIGINT NULL,
    ingestion_date  DATE   NOT NULL,

    FOREIGN KEY (post_id, comment_id) REFERENCES mart.tb_posts  (post_id, comment_id),
    FOREIGN KEY (car_name)  REFERENCES mart.tb_car          (car_name),
    FOREIGN KEY (source)    REFERENCES mart.tb_web_source   (source)
);

CREATE TABLE IF NOT EXISTS mart.tb_comments_metric(
    post_id         VARCHAR(255) NOT NULL,
    comment_id      VARCHAR(255) NOT NULL,
    car_name        VARCHAR(255) NOT NULL,
    source          VARCHAR(255) NOT NULL,
    like_cnt        BIGINT NULL,
    dislike_cnt     BIGINT NULL,
    ingestion_date  VARCHAR(255) NOT NULL,

    FOREIGN KEY (post_id)      REFERENCES mart.tb_posts         (post_id),
    FOREIGN KEY (comment_id)   REFERENCES mart.tb_comments      (comment_id),
    FOREIGN KEY (car_name)     REFERENCES mart.tb_car           (car_name),
    FOREIGN KEY (source)       REFERENCES mart.tb_web_source    (source)
);

CREATE TABLE IF NOT EXISTS mart.tb_keywords(
    post_id         VARCHAR(255)        NOT NULL,
    comment_id      VARCHAR(255)        NOT NULL,
    car_name        VARCHAR(255)        NOT NULL,
    source          VARCHAR(255)        NOT NULL,
    type            VARCHAR(255)        NOT NULL,
    sentence        VARCHAR(65535)      NOT NULL,
    category        VARCHAR(255)        NOT NULL,
    keyword         VARCHAR(255)        NOT NULL,
    sentiment_score DOUBLE PRECISION    NOT NULL,
    ingestion_date  DATE                NOT NULL,

    FOREIGN KEY (post_id)      REFERENCES mart.tb_posts         (post_id),
    FOREIGN KEY (comment_id)   REFERENCES mart.tb_comments      (comment_id),
    FOREIGN KEY (car_name)              REFERENCES mart.tb_car          (car_name),
    FOREIGN KEY (source)                REFERENCES mart.tb_web_source   (source)
);

CREATE MATERIALIZED VIEW social_listening AS
    SELECT
        k.post_id,
        CASE WHEN k."type" = 'comment' THEN k.comment_id ELSE NULL END AS comment_id,
        k."type",
        k.sentence,
        k.category,
        k.sentiment_score,
        k.ingestion_date,
        k.car_name,
        c.pre_car_name,
        c.release_date,
        k.source,
        w.age
    FROM mart.tb_keywords AS k
    JOIN mart.tb_car AS c ON k.car_name = c.car_name
    JOIN mart.tb_web_source AS w ON k.source = w.source;

CREATE MATERIALIZED VIEW social_attention AS
    SELECT
       p.post_id,
       c.comment_id,
        CASE
            WHEN comment_id IS NULL THEN p.like_cnt
            ELSE c.like_cnt
        END AS like_cnt,
        CASE
            WHEN comment_id IS NULL THEN p.dislike_cnt
            ELSE c.dislike_cnt
        END AS dislike_cnt,
        p.view_cnt,
        p.comment_cnt,
        CASE
            WHEN comment_id IS NULL THEN CAST(p.ingestion_date)
            ELSE CAST(c.ingestion_date)
        END AS ingestion_date,
        p.car_name,
        car.pre_car_name,
        car.release_date,
        p.source,
        w.age
    FROM mart.tb_posts_metric AS p
        LEFT JOIN mart.tb_comments_metric AS c ON p.post_id = c.post_id
        JOIN mart.tb_car AS car ON p.car_name = car.car_name
        JOIN mart.tb_web_source AS w ON p.source = w.source