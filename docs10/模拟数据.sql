-- 创建数据库
CREATE DATABASE IF NOT EXISTS ecommerce_analytics;
USE ecommerce_analytics;


-- 删除原表（注意：删除表会丢失数据，若表中已有数据且需要保留，需先备份数据）
-- 重建 teemp_tbl，增加 device_type_id 字段
drop table if exists ecommerce_analytics.teemp_tbl;
CREATE TABLE ecommerce_analytics.teemp_tbl (
    id INT COMMENT '记录唯一ID',
    visit_identifier STRING COMMENT '访问会话标识',
    store_id INT COMMENT '店铺ID',
    item_id INT COMMENT '商品ID',
    page_type STRING COMMENT '页面类型（如首页、商品详情页）',
    device_type_id INT COMMENT '设备类型ID（1=移动端，2=PC端）',  -- 新增字段
    traffic_source_id INT COMMENT '流量入口ID（PC端用）',
    visit_time TIMESTAMP COMMENT '访问时间'
)
    COMMENT '电商访问行为临时表';


INSERT INTO ecommerce_analytics.teemp_tbl (
    id, store_id, item_id, page_type,
    visit_identifier, visit_time, traffic_source_id
)
SELECT
    sub.id,
    sub.store_id,
    sub.item_id,
    sub.page_type,
    sub.visit_identifier,
    sub.visit_time,
    -- 外层引用子查询的 page_type
    CASE
        WHEN RAND() < 0.5 AND sub.page_type like 'item_detail_%'
            THEN FLOOR(1 + RAND() * 8)
        ELSE NULL
        END AS traffic_source_id
FROM (
         -- 子查询封装派生字段 page_type
         SELECT
             row_number() OVER () AS id,
             FLOOR(1 + RAND() * 100) AS store_id,
             CASE
                 WHEN inner_page_type = 'home' THEN NULL
                 ELSE FLOOR(1 + RAND() * 500)
                 END AS item_id,
             inner_page_type AS page_type,  -- 显式定义为字段
             UUID() AS visit_identifier,
             from_unixtime(
                     unix_timestamp('2024-07-30 00:00:00') + FLOOR(RAND() * 86400)
             ) AS visit_time
         FROM (
                  -- 生成 page_type 的基础子查询
                  SELECT
                      CASE
                          WHEN RAND() < 0.3 THEN 'home'
                          ELSE CONCAT('item_detail_', FLOOR(1 + RAND() * 500))
                          END AS inner_page_type
                  FROM (
                           SELECT 1 UNION SELECT 2 UNION SELECT 3 UNION SELECT 4 UNION SELECT 5
                       ) t1
                           CROSS JOIN (
                      SELECT 1 UNION SELECT 2 UNION SELECT 3 UNION SELECT 4 UNION SELECT 5
                  ) t2
              ) AS page_type_subquery
     ) AS sub
LIMIT 100000;

select * from ecommerce_analytics.teemp_tbl;

-- 设备类型枚举表
drop table if exists ecommerce_analytics.device_type;
CREATE TABLE ecommerce_analytics.device_type (
    id INT COMMENT '设备类型ID',
    type_name STRING COMMENT '设备类型：移动端、PC端',
    description STRING COMMENT '类型描述',
    create_time TIMESTAMP COMMENT '创建时间'
)
    COMMENT '设备类型表，区分移动端和PC端'
    ROW FORMAT DELIMITED
        FIELDS TERMINATED BY '\t'
    STORED AS ORC;


-- 清空表数据
TRUNCATE TABLE ecommerce_analytics.device_type;

-- 插入设备类型数据
INSERT INTO ecommerce_analytics.device_type (id, type_name, description, create_time)
SELECT 1, '移动端', '包括手机、平板等无线设备访问', '2024-07-30 09:15:30'
UNION ALL
SELECT 2, 'PC端', '电脑端网页访问', '2024-07-30 14:22:10';

select * from ecommerce_analytics.device_type;



-- 页面类型枚举表
drop table if exists ecommerce_analytics.page_type;
CREATE TABLE ecommerce_analytics.page_type (
    id INT COMMENT '页面类型ID',
    page_category STRING COMMENT '页面大类：店铺页、商品详情页、店铺其他页',
    page_name STRING COMMENT '页面名称：首页、活动页、分类页等',
    page_desc STRING COMMENT '页面描述',
    create_time TIMESTAMP COMMENT '创建时间'
)
    COMMENT '页面类型表，定义各类页面信息'
    ROW FORMAT DELIMITED
        FIELDS TERMINATED BY '\t'
    STORED AS ORC;


TRUNCATE TABLE ecommerce_analytics.page_type;

INSERT INTO ecommerce_analytics.page_type (id, page_category, page_name, page_desc, create_time)
SELECT 1, '店铺页', '首页', '店铺首页，展示店铺整体信息', '2024-07-30 08:30:00' UNION ALL
SELECT 2, '店铺页', '活动页', '店铺促销活动专题页', '2024-07-30 09:45:00' UNION ALL
SELECT 3, '店铺页', '分类页', '商品分类导航页', '2024-07-30 10:10:00' UNION ALL
SELECT 4, '店铺页', '新品页', '新上架商品展示页', '2024-07-30 11:20:00' UNION ALL
SELECT 5, '商品详情页', '宝贝页', '单个商品的详细信息页', '2024-07-30 13:15:00' UNION ALL
SELECT 6, '店铺其他页', '订阅页', '用户订阅店铺的页面', '2024-07-30 14:50:00' UNION ALL
SELECT 7, '店铺其他页', '直播页', '店铺直播展示页', '2024-07-30 15:30:00' UNION ALL
SELECT 8, '店铺其他页', '会员页', '会员专属服务页', '2024-07-30 16:40:00' UNION ALL
SELECT 9, '店铺页', '热卖页', '热销商品推荐页', '2024-07-30 17:25:00' UNION ALL
SELECT 10, '商品详情页', '预售页', '预售商品详情页', '2024-07-30 18:10:00';

select * from ecommerce_analytics.page_type;

-- 流量入口枚举表（主要用于PC端）
drop table if exists ecommerce_analytics.traffic_source;
CREATE TABLE ecommerce_analytics.traffic_source (
    id INT COMMENT '流量入口ID',
    source_name STRING COMMENT '流量入口名称',
    source_desc STRING COMMENT '入口描述',
    create_time TIMESTAMP COMMENT '创建时间'
)
    COMMENT '流量入口表，记录PC端流量来源'
    ROW FORMAT DELIMITED
        FIELDS TERMINATED BY '\t'
    STORED AS ORC;

TRUNCATE TABLE ecommerce_analytics.traffic_source;

INSERT INTO ecommerce_analytics.traffic_source (id, source_name, source_desc, create_time)
SELECT 1, '搜索引擎', '百度、谷歌等搜索引擎跳转', '2024-07-30 07:50:00' UNION ALL
SELECT 2, '直接访问', '用户直接输入网址访问', '2024-07-30 08:20:00' UNION ALL
SELECT 3, '外部链接', '其他网站跳转链接', '2024-07-30 09:10:00' UNION ALL
SELECT 4, '社交媒体', '微信、微博等社交平台', '2024-07-30 10:30:00' UNION ALL
SELECT 5, '广告投放', '付费广告引流（如直通车）', '2024-07-30 11:45:00' UNION ALL
SELECT 6, '平台推荐', '电商平台首页推荐', '2024-07-30 12:50:00' UNION ALL
SELECT 7, '收藏夹', '用户从浏览器收藏夹访问', '2024-07-30 14:20:00' UNION ALL
SELECT 8, '短信链接', '营销短信中的跳转链接', '2024-07-30 16:10:00';

select * from ecommerce_analytics.traffic_source;

-- 店铺访问主表（按日期分区）
-- 删除旧表（备份数据后执行，或跳过删除直接新增字段）
drop table if exists ecommerce_analytics.shop_visit_stats;

-- 重建表：增加 create_time 字段
CREATE TABLE ecommerce_analytics.shop_visit_stats (
    id BIGINT COMMENT '记录ID',
    store_id INT COMMENT '店铺ID',
    device_type_id INT COMMENT '设备类型ID（1=移动端，2=PC端）',
    page_type_id INT COMMENT '页面类型ID',
    traffic_source_id INT COMMENT '流量入口ID（PC端用）',
    visitor_count INT COMMENT '访客数',
    buyer_count INT COMMENT '下单买家数',
    pv INT COMMENT '浏览量',
    avg_stay_time DECIMAL(10,2) COMMENT '平均停留时长(秒)',
    create_time TIMESTAMP COMMENT '数据创建时间',  -- 新增字段
    update_time TIMESTAMP COMMENT '数据更新时间',
    stat_date DATE COMMENT '统计日期'  -- 移除分区，或调整分区逻辑（根据需求）
)
    COMMENT '店铺访问统计表'
    PARTITIONED BY (
        dt STRING COMMENT '分区字段，按天分区（格式：yyyy-MM-dd）'  -- 统一分区为 dt
        )
    STORED AS ORC;

-- 重新插入 shop_visit_stats 数据（增加 store_id）
INSERT INTO ecommerce_analytics.shop_visit_stats (
    id, store_id, device_type_id, page_type_id, traffic_source_id,
    visitor_count, buyer_count, pv, avg_stay_time,
    create_time, update_time, stat_date, dt  -- 分区字段 dt
)
SELECT
    row_number() OVER () AS id,
    FLOOR(1 + RAND() * 100) AS store_id,
    FLOOR(1 + RAND() * 2) AS device_type_id,
    FLOOR(1 + RAND() * 10) AS page_type_id,
    CASE WHEN FLOOR(1 + RAND() * 2) = 2 THEN FLOOR(1 + RAND() * 8) ELSE NULL END AS traffic_source_id,
    FLOOR(10 + RAND() * 490) AS visitor_count,
    FLOOR(FLOOR(10 + RAND() * 490) * (0.01 + RAND() * 0.04)) AS buyer_count,
    FLOOR(FLOOR(10 + RAND() * 490) * (1 + RAND() * 2)) AS pv,
    ROUND(10 + RAND() * 290, 2) AS avg_stay_time,
    current_timestamp() AS create_time,  -- 填充创建时间
    current_timestamp() AS update_time,  -- 填充更新时间
    date_add(current_date(), CAST(-FLOOR(RAND() * 30) AS INT)) AS stat_date,
    current_date() AS dt  -- 分区字段（格式：yyyy-MM-dd）
FROM (
         SELECT 1 UNION SELECT 2 UNION SELECT 3 UNION SELECT 4 UNION SELECT 5
     ) t1, (SELECT 1 UNION SELECT 2 UNION SELECT 3 UNION SELECT 4 UNION SELECT 5) t2,
     (SELECT 1 UNION SELECT 2 UNION SELECT 3 UNION SELECT 4 UNION SELECT 5) t3,
     (SELECT 1 UNION SELECT 2 UNION SELECT 3 UNION SELECT 4 UNION SELECT 5) t4,
     (SELECT 1 UNION SELECT 2 UNION SELECT 3 UNION SELECT 4 UNION SELECT 5) t5,
     (SELECT 1 UNION SELECT 2 UNION SELECT 3 UNION SELECT 4 UNION SELECT 5) t6,
     (SELECT 1 UNION SELECT 2 UNION SELECT 3 UNION SELECT 4 UNION SELECT 5) t7
LIMIT 100000;

select * from ecommerce_analytics.shop_visit_stats;


drop table if exists ecommerce_analytics.shop_path_analysis;
CREATE TABLE ecommerce_analytics.shop_path_analysis (
    id BIGINT COMMENT '记录ID',
    store_id INT COMMENT '店铺ID',
    device_type_id INT COMMENT '设备类型ID',
    page_type_id INT COMMENT '页面类型ID',
    path_sequence STRING COMMENT '页面路径序列',
    conversion_rate DECIMAL(8,4) COMMENT '转化率',
    update_time TIMESTAMP COMMENT '更新时间'
)
    COMMENT '店内路径分析表'
    PARTITIONED BY (
        stat_date DATE COMMENT '统计日期',
        dt STRING COMMENT '分区字段，按天分区'  -- 补充 dt 分区
        )
    STORED AS ORC;

-- 重新插入 shop_path_analysis 数据（增加 store_id）
INSERT INTO ecommerce_analytics.shop_path_analysis (
    id, store_id, device_type_id, page_type_id, path_sequence, visit_count,
    conversion_rate, avg_stay_time, create_time, update_time, stat_date
)
SELECT

    row_number() OVER () AS id,
    FLOOR(1 + RAND() * 100) AS store_id,  -- 1-100的店铺ID
    FLOOR(1 + RAND() * 2) AS device_type_id,
    FLOOR(1 + RAND() * 10) AS page_type_id,  -- 1-10，关联page_type表
    CONCAT(
            CASE FLOOR(1 + RAND() * 10)
                WHEN 1 THEN '首页' WHEN 2 THEN '活动页' WHEN 3 THEN '分类页' WHEN 4 THEN '新品页' WHEN 5 THEN '宝贝页'
                WHEN 6 THEN '订阅页' WHEN 7 THEN '直播页' WHEN 8 THEN '会员页' WHEN 9 THEN '热卖页' WHEN 10 THEN '预售页'
                END,
            '->',
            CASE FLOOR(1 + RAND() * 10)
                WHEN 1 THEN '首页' WHEN 2 THEN '活动页' WHEN 3 THEN '分类页' WHEN 4 THEN '新品页' WHEN 5 THEN '宝贝页'
                WHEN 6 THEN '订阅页' WHEN 7 THEN '直播页' WHEN 8 THEN '会员页' WHEN 9 THEN '热卖页' WHEN 10 THEN '预售页'
                END
    ) AS path_sequence,
    FLOOR(5 + RAND() * 495) AS visit_count,
    ROUND(0.01 + RAND() * 0.14, 4) AS conversion_rate,
    ROUND(60 + RAND() * 540, 2) AS avg_stay_time,
    current_timestamp() AS create_time,
    current_timestamp() AS update_time,
    date_add(current_date(), CAST(-FLOOR(RAND() * 30) AS INT)) AS stat_date
FROM (
         SELECT 1 UNION SELECT 2 UNION SELECT 3 UNION SELECT 4 UNION SELECT 5
     ) t1, (SELECT 1 UNION SELECT 2 UNION SELECT 3 UNION SELECT 4 UNION SELECT 5) t2,
     (SELECT 1 UNION SELECT 2 UNION SELECT 3 UNION SELECT 4 UNION SELECT 5) t3
LIMIT 50000;

select * from ecommerce_analytics.shop_path_analysis;


-- 页面访问排行表（按日期分区）


drop table if exists ecommerce_analytics.page_visit_ranking;
CREATE TABLE ecommerce_analytics.page_visit_ranking (
    id BIGINT COMMENT '记录ID',
    store_id INT COMMENT '店铺ID',
    device_type_id INT COMMENT '设备类型ID',
    page_type_id INT COMMENT '页面类型ID',
    page_name STRING COMMENT '页面名称',
    pv INT COMMENT '浏览量',
    bounce_rate DECIMAL(8,4) COMMENT '跳出率',
    avg_stay_time DECIMAL(10,2) COMMENT '平均停留时长',
    ranking INT COMMENT '访问排行',
    update_time TIMESTAMP COMMENT '更新时间'
)
    COMMENT '页面访问排行表'
    PARTITIONED BY (
        stat_date DATE COMMENT '统计日期',
        dt STRING COMMENT '分区字段，按天分区'  -- 补充 dt 分区
        )
    STORED AS ORC;

-- 插入数据（关键：在子查询中补充 store_id）
INSERT INTO ecommerce_analytics.page_visit_ranking (
    id, store_id, device_type_id, page_type_id, page_name,
    pv, bounce_rate, avg_stay_time, ranking, stat_date, dt
)
SELECT
    row_number() OVER () AS id,
    sub.store_id,  -- 从子查询拿 store_id
    2 AS device_type_id,  -- PC端
    CASE
        WHEN sub.page_name = '首页' THEN 1
        WHEN sub.page_name LIKE 'item_detail_%' THEN 5
        ELSE FLOOR(3 + RAND() * 7)
        END AS page_type_id,
    sub.page_name,
    FLOOR(100 + RAND() * 900) AS pv,
    ROUND(RAND() * 0.5, 4) AS bounce_rate,
    ROUND(30 + RAND() * 300, 2) AS avg_stay_time,
    ROW_NUMBER() OVER (PARTITION BY sub.store_id ORDER BY RAND()) AS ranking,  -- 用子查询的 store_id 分区
    current_date() AS stat_date,
    current_date() AS dt
FROM (
         -- 子查询：同时生成 page_name 和 store_id
         SELECT
             CASE
                 WHEN RAND() < 0.3 THEN '首页'
                 ELSE CONCAT('item_detail_', FLOOR(1 + RAND() * 500))
                 END AS page_name,
             FLOOR(1 + RAND() * 100) AS store_id  -- 补充 store_id（1-100，与 teemp_tbl 对齐）
         FROM (
                  SELECT 1 UNION SELECT 2 UNION SELECT 3
              ) t1
                  CROSS JOIN (
             SELECT 1 UNION SELECT 2 UNION SELECT 3
         ) t2
                  CROSS JOIN (
             SELECT 1 UNION SELECT 2 UNION SELECT 3
         ) t3
     ) AS sub  -- 子查询别名：sub
LIMIT 10000;  -- 控制插入数据量

-- 验证查询
select * from ecommerce_analytics.page_visit_ranking;







----------------------------------------------
drop table if exists ecommerce_analytics.dwd_wireless_visit_detail;
create table ecommerce_analytics.dwd_wireless_visit_detail (
    id bigint comment '记录唯一 ID',
    visit_identifier string comment '访问会话唯一标识',
    store_id int comment '店铺 ID',
    item_id int comment '商品 ID',
    device_type string comment '设备类型：移动端',
    page_category string comment '页面大类：店铺页/商品详情页/店铺其他页',
    page_name string comment '页面名称：首页/活动页等',
    visitor_count int comment '访客数',
    buyer_count int comment '下单买家数',
    pv int comment '浏览量',
    avg_stay_time decimal(10,2) comment '平均停留时长(秒)',
    conversion_rate decimal(8,4) comment '转化率',
    path_sequence string comment '页面路径序列',
    visit_time timestamp comment '访问时间',
    stat_date date comment '统计日期',
    create_time timestamp comment '数据创建时间',
    update_time timestamp comment '数据更新时间'
)
    comment 'DWD 层：移动端端访问行为明细，含路径、转化数据'
    partitioned by (dt string comment '分区字段，按天分区')
    stored as orc
    tblproperties ('orc.compress'='SNAPPY');


-- 重新插入 DWD 表
insert overwrite table ecommerce_analytics.dwd_wireless_visit_detail
    partition(dt='2025-07-31')
select
    t.id,
    t.visit_identifier,
    t.store_id,
    t.item_id,
    d.type_name as device_type,
    pt.page_category,
    pt.page_name,
    s.visitor_count,
    s.buyer_count,
    s.pv,
    s.avg_stay_time,
    pa.conversion_rate,
    pa.path_sequence,
    t.visit_time,
    s.stat_date,
    current_timestamp() as create_time,
    current_timestamp() as update_time
from ecommerce_analytics.teemp_tbl t
         left join ecommerce_analytics.device_type d
                   on d.id = 1
         left join ecommerce_analytics.page_type pt
                   on case
                          when t.page_type = 'home' then '首页'
                          when t.page_type like 'item_detail_%' then '宝贝页'
                          else t.page_type
                          end = pt.page_name
         left join ecommerce_analytics.shop_visit_stats s
                   on t.store_id = s.store_id
                       and case
                               when t.page_type = 'home' then 1
                               when t.page_type like 'item_detail_%' then 5
                               else 0
                               end = s.page_type_id
                       and s.device_type_id = 1
                       and s.stat_date = '2025-07-31'
         left join ecommerce_analytics.shop_path_analysis pa
                   on t.store_id = pa.store_id
                       and case
                               when t.page_type = 'home' then 1
                               when t.page_type like 'item_detail_%' then 5
                               else 0
                               end = pa.page_type_id
                       and pa.device_type_id = 1
                       and pa.stat_date = '2025-07-31'
    distribute by t.visit_identifier
    sort by t.visit_time asc;

-- 查询结果验证

select * from ecommerce_analytics.dwd_wireless_visit_detail;



drop table if exists ecommerce_analytics.dwd_pc_traffic_detail;
create table ecommerce_analytics.dwd_pc_traffic_detail (
    id bigint comment '记录唯一 ID',
    visit_identifier string comment '访问会话唯一标识',
    store_id int comment '店铺 ID',
    item_id int comment '商品 ID',
    device_type string comment '设备类型：PC 端',
    page_category string comment '页面大类：店铺页/商品详情页/店铺其他页',
    page_name string comment '页面名称：首页/活动页等',
    traffic_source string comment '流量入口（PC 端有效）',
    visitor_count int comment '访客数',
    pv int comment '浏览量',
    bounce_rate decimal(8,4) comment '跳出率',
    avg_stay_time decimal(10,2) comment '平均停留时长(秒)',
    visit_time timestamp comment '访问时间',
    stat_date date comment '统计日期',
    create_time timestamp comment '数据创建时间',
    update_time timestamp comment '数据更新时间'
)
    comment 'DWD 层：PC 端流量分析明细，含流量入口、跳出率'
    partitioned by (dt string comment '分区字段，按天分区')
    stored as orc
    tblproperties ('orc.compress'='SNAPPY');


insert overwrite table ecommerce_analytics.dwd_pc_traffic_detail partition(dt='${hiveconf:current_date}')
select
    t.id,
    t.visit_identifier,
    t.store_id,
    t.item_id,
    d.type_name as device_type,
    pt.page_category,
    pt.page_name,
    ts.source_name as traffic_source,
    s.visitor_count,
    s.pv,
    p.bounce_rate,
    s.avg_stay_time,
    t.visit_time,
    s.stat_date,
    current_timestamp() as create_time,
    current_timestamp() as update_time
from ecommerce_analytics.teemp_tbl t
-- 关联设备类型（仅 PC 端）
         left join ecommerce_analytics.device_type d
                   on d.id = 2  -- 2=PC 端
-- 关联页面类型
         left join ecommerce_analytics.page_type pt
                   on t.page_type = pt.page_name
-- 关联流量入口（仅 PC 端）
         left join ecommerce_analytics.traffic_source ts
                   on ts.id = t.traffic_source_id
                       and d.type_name = 'PC端'
-- 关联店铺访问主表（PC 端数据）
         left join ecommerce_analytics.shop_visit_stats s
                   on t.store_id = s.store_id
                       and t.page_type = pt.page_name
                       and s.device_type_id = 2  -- 2=PC 端
-- 关联页面访问排行表
         left join ecommerce_analytics.page_visit_ranking p
                   on t.store_id = p.store_id
                       and t.page_type = pt.page_name
                       and p.device_type_id = 2  -- 2=PC 端
-- 去重逻辑
    distribute by t.visit_identifier
    sort by t.visit_time asc;

select * from ecommerce_analytics.dwd_pc_traffic_detail;




drop table if exists ecommerce_analytics.dwd_common_dimension_map;
create table ecommerce_analytics.dwd_common_dimension_map (
    device_type_id int comment '设备类型 ID',
    device_type string comment '设备类型：移动端/PC 端',
    page_type_id int comment '页面类型 ID',
    page_category string comment '页面大类：店铺页/商品详情页/店铺其他页',
    page_name string comment '页面名称：首页/活动页等',
    traffic_source_id int comment '流量入口 ID',
    traffic_source string comment '流量入口名称'
)
    comment 'DWD 层：通用维度映射表，关联设备、页面、流量入口'
    stored as orc
    tblproperties ('orc.compress'='SNAPPY');



insert overwrite table ecommerce_analytics.dwd_common_dimension_map
select
    d.id as device_type_id,
    d.type_name as device_type,
    pt.id as page_type_id,
    pt.page_category,
    pt.page_name,
    ts.id as traffic_source_id,
    ts.source_name as traffic_source
from ecommerce_analytics.device_type d
         cross join ecommerce_analytics.page_type pt
         left join ecommerce_analytics.traffic_source ts
                   on d.type_name = 'PC端';  -- PC 端关联流量入口，移动端为 NULL



drop table if exists ecommerce_analytics.dwd_shop_visit_detail;
create table ecommerce_analytics.dwd_shop_visit_detail (
    id bigint comment '记录唯一 ID',
    visit_identifier string comment '访问会话唯一标识',
    store_id int comment '店铺 ID',
    item_id int comment '商品 ID',
    device_type string comment '设备类型：移动端/PC 端',
    page_category string comment '页面大类：店铺页/商品详情页/店铺其他页',
    page_name string comment '页面名称：首页/活动页等',
    traffic_source string comment '流量入口（PC 端有效）',
    visitor_count int comment '访客数',
    buyer_count int comment '下单买家数',
    pv int comment '浏览量',
    avg_stay_time decimal(10,2) comment '平均停留时长(秒)',
    bounce_rate decimal(8,4) comment '跳出率',
    conversion_rate decimal(8,4) comment '转化率',
    path_sequence string comment '页面路径序列',
    visit_time timestamp comment '访问时间',
    stat_date date comment '统计日期',
    create_time timestamp comment '数据创建时间',
    update_time timestamp comment '数据更新时间'
)
    comment 'DWD 层：电商访问行为明细宽表'
    partitioned by (dt string comment '分区字段，按天分区')
    stored as orc
    tblproperties ('orc.compress'='SNAPPY');

-- 插入数据（修正 dt 分区引用和关联逻辑）
insert overwrite table ecommerce_analytics.dwd_shop_visit_detail
    partition(dt='${hiveconf:current_date}')
select
    coalesce(t.id, s.id, p.id, pa.id) as id,
    t.visit_identifier,
    t.store_id,
    t.item_id,
    d.type_name as device_type,
    pt.page_category,
    pt.page_name,
    ts.source_name as traffic_source,
    coalesce(s.visitor_count, 0) as visitor_count,
    coalesce(s.buyer_count, 0) as buyer_count,
    coalesce(s.pv, 0) as pv,
    coalesce(s.avg_stay_time, 0.00) as avg_stay_time,
    coalesce(p.bounce_rate, 0.0000) as bounce_rate,
    coalesce(pa.conversion_rate, 0.0000) as conversion_rate,
    pa.path_sequence,
    t.visit_time,
    coalesce(s.stat_date, p.stat_date, pa.stat_date) as stat_date,
    current_timestamp() as create_time,
    current_timestamp() as update_time
from ecommerce_analytics.teemp_tbl t
-- 关联设备类型（用 t.device_type_id 关联）
         left join ecommerce_analytics.device_type d
                   on t.device_type_id = d.id
-- 关联页面类型
         left join ecommerce_analytics.page_type pt
                   on t.page_type = pt.page_name
-- 关联流量入口（PC端专用）
         left join ecommerce_analytics.traffic_source ts
                   on t.traffic_source_id = ts.id
                       and d.type_name = 'PC端'
-- 关联店铺访问主表（用 dt 分区过滤）
         left join (
    select * from ecommerce_analytics.shop_visit_stats
    where dt = '${hiveconf:current_date}'  -- 现在 dt 分区存在
        distribute by stat_date, device_type_id, page_type_id
        sort by update_time desc
    limit 1
) s on t.store_id = s.store_id
    and t.device_type_id = s.device_type_id  -- 补充设备类型关联
    and pt.id = s.page_type_id  -- 用 page_type 表的 ID 关联（更可靠）
-- 关联页面访问排行表（用 dt 分区过滤）
         left join (
    select * from ecommerce_analytics.page_visit_ranking
    where dt = '${hiveconf:current_date}'
        distribute by stat_date, device_type_id, page_type_id
        sort by update_time desc
    limit 1
) p on t.store_id = p.store_id
    and t.device_type_id = p.device_type_id
    and pt.id = p.page_type_id
-- 关联店内路径表（用 dt 分区过滤）
         left join (
    select * from ecommerce_analytics.shop_path_analysis
    where dt = '${hiveconf:current_date}'
        distribute by stat_date, device_type_id, page_type_id
        sort by update_time desc
    limit 1
) pa on t.store_id = pa.store_id
    and t.device_type_id = pa.device_type_id
    and pt.id = pa.page_type_id
-- 去重逻辑
    distribute by t.visit_identifier, t.page_type
    sort by t.visit_time asc;
