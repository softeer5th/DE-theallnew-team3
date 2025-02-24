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