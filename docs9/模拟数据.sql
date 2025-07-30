-- 创建数据库
CREATE DATABASE IF NOT EXISTS ecommerce_analytics;
USE ecommerce_analytics;

-- 创建数据表
drop table if exists ecommerce_analytics.teemp_tbl;
CREATE table ecommerce_analytics.teemp_tbl (
    id int,
    store_id int not null,
    item_id int,
    page_type varchar(50) not null,
    visit_identifier varchar(100) not null,
    visit_time timestamp not null,
    primary key (id) not enforced
);



-- 生成10万条测试数据
INSERT INTO ecommerce_analytics.teemp_tbl (store_id, item_id, page_type, visit_identifier, visit_time)
SELECT
    store_id,
    -- 根据页面类型生成商品ID（首页没有商品ID）
    CASE
        WHEN page_type = 'home' THEN NULL
        ELSE FLOOR(1 + RAND() * 500)
        END AS item_id,
    page_type,
    UUID() AS visit_identifier,
    from_unixtime(
            unix_timestamp() -
            FLOOR(RAND() * 30 * 24 * 60 * 60) -  -- 30天内的随机秒数
            FLOOR(RAND() * 24 * 60 * 60) -       -- 随机小时转换为秒
            FLOOR(RAND() * 60 * 60)              -- 随机分钟转换为秒
    ) AS visit_time
FROM (
         SELECT
             -- 生成1-100的店铺ID
             FLOOR(1 + RAND() * 100) AS store_id,
             -- 先生成页面类型
             CASE
                 WHEN RAND() < 0.3 THEN 'home'
                 ELSE CONCAT('item_detail_', FLOOR(1 + RAND() * 500))
                 END AS page_type
         FROM
             (SELECT 1 UNION SELECT 2 UNION SELECT 3 UNION SELECT 4 UNION SELECT 5 UNION SELECT 6 UNION SELECT 7 UNION SELECT 8 UNION SELECT 9 UNION SELECT 10) t1,
             (SELECT 1 UNION SELECT 2 UNION SELECT 3 UNION SELECT 4 UNION SELECT 5 UNION SELECT 6 UNION SELECT 7 UNION SELECT 8 UNION SELECT 9 UNION SELECT 10) t2,
             (SELECT 1 UNION SELECT 2 UNION SELECT 3 UNION SELECT 4 UNION SELECT 5 UNION SELECT 6 UNION SELECT 7 UNION SELECT 8 UNION SELECT 9 UNION SELECT 10) t3,
             (SELECT 1 UNION SELECT 2 UNION SELECT 3 UNION SELECT 4 UNION SELECT 5 UNION SELECT 6 UNION SELECT 7 UNION SELECT 8 UNION SELECT 9 UNION SELECT 10) t4,
             (SELECT 1 UNION SELECT 2 UNION SELECT 3 UNION SELECT 4 UNION SELECT 5 UNION SELECT 6 UNION SELECT 7 UNION SELECT 8 UNION SELECT 9 UNION SELECT 10) t5,
             (SELECT 1 UNION SELECT 2 UNION SELECT 3 UNION SELECT 4 UNION SELECT 5 UNION SELECT 6 UNION SELECT 7 UNION SELECT 8 UNION SELECT 9 UNION SELECT 10) t6
         LIMIT 100000
     ) AS subquery;

-- 验证数据
SELECT COUNT(*) FROM ecommerce_analytics.teemp_tbl;
SELECT page_type, COUNT(*) FROM ecommerce_analytics.teemp_tbl GROUP BY page_type;
SELECT store_id, COUNT(*) FROM ecommerce_analytics.teemp_tbl GROUP BY store_id ORDER BY COUNT(*) DESC LIMIT 10;
