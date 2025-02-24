REGION = "ap-northeast-2"
WORKGROUP_NAME = "the-all-new-workgroup"
DATABASE = "dev"
COPY_PARAMS = [
    {"TABLE_NAME": "tb_posts", "S3_KEY": "post_data/"},
    {"TABLE_NAME": "tb_comments", "S3_KEY": "comment_data/"},
    {"TABLE_NAME": "tb_sentences", "S3_KEY": "sentence_data/"},
    {"TABLE_NAME": "tb_keywords", "S3_KEY": "classified/"},
]
