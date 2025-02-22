CREATE TABLE IF NOT EXISTS staging.tb_posts (
    post_uuid           CHAR(36)          NOT NULL,
    post_id             VARCHAR(255)      NOT NULL,
    title               VARCHAR(255)      NOT NULL,
    author              VARCHAR(255)      NOT NULL,
    article             VARCHAR(65535)    NOT NULL,
    create_timestamp    BIGINT            NOT NULL,
    like_cnt            BIGINT            NULL,
    dislike_cnt         BIGINT            NULL,
    view_cnt            BIGINT            NULL,
    comment_cnt         BIGINT            NULL,
    car_name            VARCHAR(255)      NOT NULL,
    source              VARCHAR(255)      NOT NULL
);

CREATE TABLE IF NOT EXISTS staging.tb_comments (
    post_uuid           CHAR(36)          NOT NULL,
    comment_uuid        CHAR(36)          NOT NULL,
    comment_id          VARCHAR(255)      NOT NULL,
    author              VARCHAR(255)      NOT NULL,
    content             VARCHAR(65535)    NOT NULL,
    create_timestamp    BIGINT            NOT NULL,
    like_cnt            BIGINT            NULL,
    dislike_cnt         BIGINT            NULL
);

CREATE TABLE IF NOT EXISTS staging.tb_sentences (
    post_uuid     CHAR(36)          NOT NULL,
    comment_uuid  CHAR(36)          NULL,
    sentence_uuid CHAR(36)          NOT NULL,
    type          VARCHAR(255)      NOT NULL,
    sentence      VARCHAR(65535)    NOT NULL
);


CREATE TABLE IF NOT EXISTS staging.tb_keywords (
    sentence_uuid    CHAR(36)           NOT NULL,
    sentiment_score  DOUBLE PRECISION   NOT NULL,
    category         VARCHAR(255)       NOT NULL,
    keyword          VARCHAR(255)       NOT NULL
);