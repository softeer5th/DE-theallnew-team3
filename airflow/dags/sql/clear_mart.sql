-- DO NOT USE --

DROP TABLE IF EXISTS mart.tb_posts_metric CASCADE;
DROP TABLE IF EXISTS mart.tb_comments_metric CASCADE;
DROP TABLE IF EXISTS mart.tb_keywords CASCADE;

DROP TABLE IF EXISTS mart.tb_posts CASCADE;
DROP TABLE IF EXISTS mart.tb_comments CASCADE;
DROP TABLE IF EXISTS mart.tb_car CASCADE;
DROP TABLE IF EXISTS mart.tb_web_source CASCADE;

DROP MATERIALIZED VIEW IF EXISTS mart.vw_social_listening;
DROP MATERIALIZED VIEW IF EXISTS mart.vw_social_attention;