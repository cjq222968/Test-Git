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

-- 设置MapReduce资源（根据集群情况调整）
set mapreduce.map.memory.mb=2048;  -- 每个Map任务的内存
set mapreduce.reduce.memory.mb=4096;  -- 每个Reduce任务的内存
set mapreduce.map.cpu.vcores=1;  -- 每个Map任务的CPU核心
set mapreduce.reduce.cpu.vcores=2;  -- 每个Reduce任务的CPU核心

-- 启用Hive本地模式（小数据量适用，减少集群调度开销）
set hive.exec.mode.local.auto=true;
set hive.exec.mode.local.auto.inputbytes.max=50000000;  -- 本地模式处理的最大数据量

--重新插入 shop_path_analysis 数据（增加 store_id）
-- 先删除表（如果需要重新创建）
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
        dt STRING COMMENT '分区字段，按天分区'
        )
    STORED AS ORC;

-- 重新插入数据，使用替代方式生成ID
INSERT INTO ecommerce_analytics.shop_path_analysis (
    id, store_id, device_type_id, page_type_id, path_sequence,
    conversion_rate, update_time, stat_date, dt
)
SELECT
    -- 对于不支持monotonically_increasing_id()的Hive版本，使用这种方式生成唯一ID
    cast(unix_timestamp(current_timestamp()) as bigint) + row_number() over () AS id,
    FLOOR(1 + RAND() * 100) AS store_id,
    FLOOR(1 + RAND() * 2) AS device_type_id,
    FLOOR(1 + RAND() * 10) AS page_type_id,
    CONCAT(
            CASE FLOOR(1 + RAND() * 10)
                WHEN 1 THEN '首页' WHEN 2 THEN '活动页' WHEN 3 THEN '分类页' WHEN 4 THEN '新品页' WHEN 5 THEN '宝贝页'
                WHEN 6 THEN '订阅页' WHEN 7 THEN '直播页' WHEN 8 THEN '会员页' WHEN 9 THEN '热卖页' WHEN 10 THEN '预售页'
                ELSE '未知页'
                END,
            '->',
            CASE FLOOR(1 + RAND() * 10)
                WHEN 1 THEN '首页' WHEN 2 THEN '活动页' WHEN 3 THEN '分类页' WHEN 4 THEN '新品页' WHEN 5 THEN '宝贝页'
                WHEN 6 THEN '订阅页' WHEN 7 THEN '直播页' WHEN 8 THEN '会员页' WHEN 9 THEN '热卖页' WHEN 10 THEN '预售页'
                ELSE '未知页'
                END
    ) AS path_sequence,
    ROUND(0.01 + RAND() * 0.14, 4) AS conversion_rate,
    current_timestamp() AS update_time,
    date_add(current_date(), -CAST(FLOOR(RAND() * 30) AS INT)) AS stat_date,
    date_format(date_add(current_date(), -CAST(FLOOR(RAND() * 30) AS INT)), 'yyyyMMdd') AS dt
FROM (
         SELECT 1 UNION SELECT 2 UNION SELECT 3
     ) t1, (SELECT 1 UNION SELECT 2) t2,
     (SELECT 1 UNION SELECT 2 UNION SELECT 3 UNION SELECT 4) t3  -- 增加一个表来生成更多数据
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


insert overwrite table ecommerce_analytics.dwd_pc_traffic_detail partition(dt='2025-07-31')
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

select * from ecommerce_analytics.dwd_common_dimension_map;



-- 删除旧表（若存在）
-- 删除旧表
drop table if exists ecommerce_analytics.dws_wireless_visit_agg;

-- 创建表时修改path_count类型为map<string,string>
create table ecommerce_analytics.dws_wireless_visit_agg (
    store_id int comment '店铺 ID',
    time_dim string comment '时间维度',
    time_type string comment '时间类型',
    page_category string comment '页面大类',
    page_name string comment '页面名称',
    uv bigint comment '访客数',
    buyer_uv bigint comment '下单买家数',
    pv bigint comment '浏览量',
    avg_stay_time decimal(10,2) comment '平均停留时长',
    conversion_rate decimal(8,4) comment '转化率',
    path_count map<string,string> comment '路径流转统计（兼容类型）',  -- 改为string
    stat_date date comment '统计日期',
    create_time timestamp comment '创建时间',
    update_time timestamp comment '更新时间'
)
    comment 'DWS 层：移动端访问行为汇总'
    partitioned by (dt string comment '按天分区')
    stored as orc
    tblproperties ('orc.compress'='SNAPPY');

-- 插入数据（保持原逻辑）
insert overwrite table ecommerce_analytics.dws_wireless_visit_agg
    partition(dt='2025-07-31')
select
    store_id,
    time_dim,
    time_type,
    page_category,
    page_name,
    max(uv) as uv,
    max(buyer_uv) as buyer_uv,
    sum(pv) as pv,
    avg(avg_stay_time) as avg_stay_time,
    avg(conversion_rate) as conversion_rate,
    str_to_map(
            concat_ws(',', collect_list(concat(path_seq, '=', cast(cnt as string)))),
            ',', '='
    ) as path_count,
    stat_date,
    current_timestamp() as create_time,
    current_timestamp() as update_time
from (
         select
             sub2.store_id,
             sub2.time_dim,
             sub2.time_type,
             sub2.page_category,
             sub2.page_name,
             sub2.uv,
             sub2.buyer_uv,
             sub2.pv,
             sub2.avg_stay_time,
             sub2.conversion_rate,
             sub2.stat_date,
             split(exploded.path_kv, '=')[0] as path_seq,
             cast(split(exploded.path_kv, '=')[1] as bigint) as cnt
         from (
                  select
                      store_id,
                      time_dim,
                      time_type,
                      page_category,
                      page_name,
                      uv,
                      buyer_uv,
                      pv,
                      avg_stay_time,
                      conversion_rate,
                      stat_date,
                      concat_ws(',', collect_list(concat(path_sequence, '=', path_cnt))) as path_kv_str
                  from (
                           select
                               t.store_id,
                               date_format(t.stat_date, 'yyyyMMdd') as time_dim,
                               'day' as time_type,
                               t.page_category,
                               t.page_name,
                               t.stat_date,
                               t.path_sequence,
                               count(distinct t.visit_identifier) over (
                                   partition by t.store_id, date_format(t.stat_date, 'yyyyMMdd'), t.page_category, t.page_name
                                   ) as uv,
                               count(distinct case when t.buyer_count > 0 then t.visit_identifier end) over (
                                   partition by t.store_id, date_format(t.stat_date, 'yyyyMMdd'), t.page_category, t.page_name
                                   ) as buyer_uv,
                               t.pv,
                               t.avg_stay_time,
                               t.conversion_rate,
                               count(1) over (
                                   partition by t.store_id, date_format(t.stat_date, 'yyyyMMdd'), t.page_category, t.page_name, t.path_sequence
                                   ) as path_cnt
                           from ecommerce_analytics.dwd_wireless_visit_detail t
                           where t.dt = '2025-07-31'
                       ) sub1
                  group by
                      store_id, time_dim, time_type, page_category, page_name, uv, buyer_uv, pv, avg_stay_time, conversion_rate, stat_date
              ) sub2
                  lateral view explode(split(path_kv_str, ',')) exploded as path_kv
     ) tmp
group by
    store_id, time_dim, time_type, page_category, page_name, stat_date;

select * from ecommerce_analytics.dws_wireless_visit_agg;



drop table if exists ecommerce_analytics.dws_pc_visit_agg;
create table ecommerce_analytics.dws_pc_visit_agg (
    store_id int comment '店铺 ID',
    time_dim string comment '时间维度',
    time_type string comment '时间类型',
    page_category string comment '页面大类',
    page_name string comment '页面名称',
    traffic_source string comment '流量入口',
    uv bigint comment '访客数',
    pv bigint comment '浏览量',
    bounce_rate decimal(8,4) comment '跳出率',
    avg_stay_time decimal(10,2) comment '平均停留时长',
    path_count_str string comment '路径流转统计（格式：路径1=计数1,路径2=计数2）',  -- 改用字符串
    stat_date date comment '统计日期',
    create_time timestamp comment '创建时间',
    update_time timestamp comment '更新时间'
)
    comment 'DWS 层：PC 端访问行为汇总'
    partitioned by (dt string comment '按天分区')
    stored as orc
    tblproperties ('orc.compress'='SNAPPY');


insert overwrite table ecommerce_analytics.dws_pc_visit_agg
    partition(dt='2025-07-31')
select
    main.store_id,
    main.time_dim,
    main.time_type,
    main.page_category,
    main.page_name,
    main.traffic_source,
    main.uv,
    main.pv,
    main.bounce_rate,
    main.avg_stay_time,
    -- 路径统计：用字符串存储“路径=计数”，逗号分隔
    concat_ws(',', collect_list(concat(path_agg.path_seq, '=', path_agg.path_cnt))) as path_count_str,
    main.stat_date,
    current_timestamp() as create_time,
    current_timestamp() as update_time
from (
         -- 主维度聚合
         select
             store_id,
             date_format(stat_date, 'yyyyMMdd') as time_dim,
             'day' as time_type,
             page_category,
             page_name,
             traffic_source,
             count(distinct visit_identifier) as uv,
             sum(pv) as pv,
             avg(bounce_rate) as bounce_rate,
             avg(avg_stay_time) as avg_stay_time,
             stat_date
         from ecommerce_analytics.dwd_pc_traffic_detail t
         where t.dt = '2025-07-31'
         group by
             store_id,
             date_format(stat_date, 'yyyyMMdd'),
             'day',
             page_category,
             page_name,
             traffic_source,
             stat_date
     ) main
         left join (
    -- 路径统计子查询
    select
        store_id,
        date_format(stat_date, 'yyyyMMdd') as time_dim,
        'day' as time_type,
        page_category,
        page_name,
        traffic_source,
        stat_date,
        -- 生成路径序列
        concat_ws('->', collect_list(page_name) over (
            partition by visit_identifier
            order by visit_time
            )) as path_seq,
        -- 路径计数
        count(1) as path_cnt
    from ecommerce_analytics.dwd_pc_traffic_detail t
    where t.dt = '2025-07-31'
    group by
        store_id,
        date_format(stat_date, 'yyyyMMdd'),
        'day',
        page_category,
        page_name,
        traffic_source,
        stat_date,
        visit_identifier,
        visit_time
) path_agg
                   on main.store_id = path_agg.store_id
                       and main.time_dim = path_agg.time_dim
                       and main.page_category = path_agg.page_category
                       and main.page_name = path_agg.page_name
                       and main.traffic_source = path_agg.traffic_source
                       and main.stat_date = path_agg.stat_date
group by
    main.store_id,
    main.time_dim,
    main.time_type,
    main.page_category,
    main.page_name,
    main.traffic_source,
    main.stat_date,
    main.uv,
    main.pv,
    main.bounce_rate,
    main.avg_stay_time;

select * from ecommerce_analytics.dws_pc_visit_agg;




drop table if exists ecommerce_analytics.ads_wireless_visit_agg;
create table ecommerce_analytics.ads_wireless_visit_agg (
    store_id int comment '店铺 ID',
    page_category string comment '页面大类',
    page_name string comment '页面名称',
    -- 1 天指标
    uv_1d bigint comment '1 天访客数',
    buyer_uv_1d bigint comment '1 天下单买家数',
    pv_1d bigint comment '1 天浏览量',
    avg_stay_time_1d decimal(10,2) comment '1 天平均停留时长',
    conversion_rate_1d decimal(8,4) comment '1 天转化率',
    -- 7 天指标
    uv_7d bigint comment '7 天访客数',
    buyer_uv_7d bigint comment '7 天下单买家数',
    pv_7d bigint comment '7 天浏览量',
    avg_stay_time_7d decimal(10,2) comment '7 天平均停留时长',
    conversion_rate_7d decimal(8,4) comment '7 天转化率',
    -- 30 天指标
    uv_30d bigint comment '30 天访客数',
    buyer_uv_30d bigint comment '30 天下单买家数',
    pv_30d bigint comment '30 天浏览量',
    avg_stay_time_30d decimal(10,2) comment '30 天平均停留时长',
    conversion_rate_30d decimal(8,4) comment '30 天转化率',
    stat_date date comment '统计日期（取最大日期，如 30 天周期则为第 30 天日期）',
    create_time timestamp comment '创建时间',
    update_time timestamp comment '更新时间'
)
    comment 'ADS 层：移动端多周期访问行为聚合'
    partitioned by (dt string comment '按天分区')
    stored as orc
    tblproperties ('orc.compress'='SNAPPY');


insert overwrite table ecommerce_analytics.ads_wireless_visit_agg
    partition(dt='2025-07-31') -- 假设以 2025-07-31 作为统计截止日，可根据实际动态传入
select
    store_id,
    page_category,
    page_name,
    -- 1 天数据（dt = 2025-07-31 对应 1 天周期 ）
    max(case when time_dim = date_format('2025-07-31', 'yyyyMMdd') then uv else 0 end) as uv_1d,
    max(case when time_dim = date_format('2025-07-31', 'yyyyMMdd') then buyer_uv else 0 end) as buyer_uv_1d,
    max(case when time_dim = date_format('2025-07-31', 'yyyyMMdd') then pv else 0 end) as pv_1d,
    max(case when time_dim = date_format('2025-07-31', 'yyyyMMdd') then avg_stay_time else 0.00 end) as avg_stay_time_1d,
    max(case when time_dim = date_format('2025-07-31', 'yyyyMMdd') then conversion_rate else 0.0000 end) as conversion_rate_1d,
    -- 7 天数据（dt 在 2025-07-25 至 2025-07-31  ）
    sum(case when time_dim between date_format(date_sub('2025-07-31', 6), 'yyyyMMdd') and date_format('2025-07-31', 'yyyyMMdd') then uv else 0 end) as uv_7d,
    sum(case when time_dim between date_format(date_sub('2025-07-31', 6), 'yyyyMMdd') and date_format('2025-07-31', 'yyyyMMdd') then buyer_uv else 0 end) as buyer_uv_7d,
    sum(case when time_dim between date_format(date_sub('2025-07-31', 6), 'yyyyMMdd') and date_format('2025-07-31', 'yyyyMMdd') then pv else 0 end) as pv_7d,
    avg(case when time_dim between date_format(date_sub('2025-07-31', 6), 'yyyyMMdd') and date_format('2025-07-31', 'yyyyMMdd') then avg_stay_time else 0.00 end) as avg_stay_time_7d,
    avg(case when time_dim between date_format(date_sub('2025-07-31', 6), 'yyyyMMdd') and date_format('2025-07-31', 'yyyyMMdd') then conversion_rate else 0.0000 end) as conversion_rate_7d,
    -- 30 天数据（dt 在 2025-07-02 至 2025-07-31  ）
    sum(case when time_dim between date_format(date_sub('2025-07-31', 29), 'yyyyMMdd') and date_format('2025-07-31', 'yyyyMMdd') then uv else 0 end) as uv_30d,
    sum(case when time_dim between date_format(date_sub('2025-07-31', 29), 'yyyyMMdd') and date_format('2025-07-31', 'yyyyMMdd') then buyer_uv else 0 end) as buyer_uv_30d,
    sum(case when time_dim between date_format(date_sub('2025-07-31', 29), 'yyyyMMdd') and date_format('2025-07-31', 'yyyyMMdd') then pv else 0 end) as pv_30d,
    avg(case when time_dim between date_format(date_sub('2025-07-31', 29), 'yyyyMMdd') and date_format('2025-07-31', 'yyyyMMdd') then avg_stay_time else 0.00 end) as avg_stay_time_30d,
    avg(case when time_dim between date_format(date_sub('2025-07-31', 29), 'yyyyMMdd') and date_format('2025-07-31', 'yyyyMMdd') then conversion_rate else 0.0000 end) as conversion_rate_30d,
    '2025-07-31' as stat_date, -- 统计日期取截止日
    current_timestamp() as create_time,
    current_timestamp() as update_time
from ecommerce_analytics.dws_wireless_visit_agg
where dt between date_format(date_sub('2025-07-31', 29), 'yyyy-MM-dd') and '2025-07-31' -- 限定 30 天数据范围
group by store_id, page_category, page_name;


select * from ecommerce_analytics.ads_wireless_visit_agg;



drop table if exists ecommerce_analytics.ads_pc_visit_agg;
create table ecommerce_analytics.ads_pc_visit_agg (
    store_id int comment '店铺 ID',
    page_category string comment '页面大类',
    page_name string comment '页面名称',
    traffic_source string comment '流量入口',
    -- 1 天指标
    uv_1d bigint comment '1 天访客数',
    pv_1d bigint comment '1 天浏览量',
    bounce_rate_1d decimal(8,4) comment '1 天跳出率',
    avg_stay_time_1d decimal(10,2) comment '1 天平均停留时长',
    -- 7 天指标
    uv_7d bigint comment '7 天访客数',
    pv_7d bigint comment '7 天浏览量',
    bounce_rate_7d decimal(8,4) comment '7 天跳出率',
    avg_stay_time_7d decimal(10,2) comment '7 天平均停留时长',
    -- 30 天指标
    uv_30d bigint comment '30 天访客数',
    pv_30d bigint comment '30 天浏览量',
    bounce_rate_30d decimal(8,4) comment '30 天跳出率',
    avg_stay_time_30d decimal(10,2) comment '30 天平均停留时长',
    stat_date date comment '统计日期（取最大日期，如 30 天周期则为第 30 天日期）',
    create_time timestamp comment '创建时间',
    update_time timestamp comment '更新时间'
)
    comment 'ADS 层：PC 端多周期访问行为聚合'
    partitioned by (dt string comment '按天分区')
    stored as orc
    tblproperties ('orc.compress'='SNAPPY');



insert overwrite table ecommerce_analytics.ads_pc_visit_agg
    partition(dt='2025-07-31') -- 假设以 2025-07-31 作为统计截止日，可根据实际动态传入
select
    store_id,
    page_category,
    page_name,
    traffic_source,
    -- 1 天数据（dt = 2025-07-31 对应 1 天周期 ）
    max(case when time_dim = date_format('2025-07-31', 'yyyyMMdd') then uv else 0 end) as uv_1d,
    max(case when time_dim = date_format('2025-07-31', 'yyyyMMdd') then pv else 0 end) as pv_1d,
    max(case when time_dim = date_format('2025-07-31', 'yyyyMMdd') then bounce_rate else 0.0000 end) as bounce_rate_1d,
    max(case when time_dim = date_format('2025-07-31', 'yyyyMMdd') then avg_stay_time else 0.00 end) as avg_stay_time_1d,
    -- 7 天数据（dt 在 2025-07-25 至 2025-07-31  ）
    sum(case when time_dim between date_format(date_sub('2025-07-31', 6), 'yyyyMMdd') and date_format('2025-07-31', 'yyyyMMdd') then uv else 0 end) as uv_7d,
    sum(case when time_dim between date_format(date_sub('2025-07-31', 6), 'yyyyMMdd') and date_format('2025-07-31', 'yyyyMMdd') then pv else 0 end) as pv_7d,
    avg(case when time_dim between date_format(date_sub('2025-07-31', 6), 'yyyyMMdd') and date_format('2025-07-31', 'yyyyMMdd') then bounce_rate else 0.0000 end) as bounce_rate_7d,
    avg(case when time_dim between date_format(date_sub('2025-07-31', 6), 'yyyyMMdd') and date_format('2025-07-31', 'yyyyMMdd') then avg_stay_time else 0.00 end) as avg_stay_time_7d,
    -- 30 天数据（dt 在 2025-07-02 至 2025-07-31  ）
    sum(case when time_dim between date_format(date_sub('2025-07-31', 29), 'yyyyMMdd') and date_format('2025-07-31', 'yyyyMMdd') then uv else 0 end) as uv_30d,
    sum(case when time_dim between date_format(date_sub('2025-07-31', 29), 'yyyyMMdd') and date_format('2025-07-31', 'yyyyMMdd') then pv else 0 end) as pv_30d,
    avg(case when time_dim between date_format(date_sub('2025-07-31', 29), 'yyyyMMdd') and date_format('2025-07-31', 'yyyyMMdd') then bounce_rate else 0.0000 end) as bounce_rate_30d,
    avg(case when time_dim between date_format(date_sub('2025-07-31', 29), 'yyyyMMdd') and date_format('2025-07-31', 'yyyyMMdd') then avg_stay_time else 0.00 end) as avg_stay_time_30d,
    '2025-07-31' as stat_date, -- 统计日期取截止日
    current_timestamp() as create_time,
    current_timestamp() as update_time
from ecommerce_analytics.dws_pc_visit_agg
where dt between date_format(date_sub('2025-07-31', 29), 'yyyy-MM-dd') and '2025-07-31' -- 限定 30 天数据范围
group by store_id, page_category, page_name, traffic_source;


select * from ecommerce_analytics.ads_pc_visit_agg;


-- 在原查询基础上，先不聚合，看关联后数据
select
    main.store_id,
    main.time_dim,
    main.page_category,
    main.page_name,
    main.traffic_source,
    main.uv,
    main.pv,
    main.bounce_rate,
    main.avg_stay_time,
    path_agg.path_seq,
    path_agg.path_cnt
from (
         -- 主维度聚合
         select
             store_id,
             date_format(stat_date, 'yyyyMMdd') as time_dim,
             'day' as time_type,
             page_category,
             page_name,
             traffic_source,
             count(distinct visit_identifier) as uv,
             sum(pv) as pv,
             avg(bounce_rate) as bounce_rate,
             avg(avg_stay_time) as avg_stay_time,
             stat_date
         from ecommerce_analytics.dwd_pc_traffic_detail t
         where t.dt = '2025-07-31'
         group by
             store_id,
             date_format(stat_date, 'yyyyMMdd'),
             'day',
             page_category,
             page_name,
             traffic_source,
             stat_date
     ) main
         left join (
    -- 路径统计子查询
    select
        store_id,
        date_format(stat_date, 'yyyyMMdd') as time_dim,
        'day' as time_type,
        page_category,
        page_name,
        traffic_source,
        stat_date,
        -- 生成路径序列
        concat_ws('->', collect_list(page_name) over (
            partition by visit_identifier
            order by visit_time
            )) as path_seq,
        -- 路径计数
        count(1) as path_cnt
    from ecommerce_analytics.dwd_pc_traffic_detail t
    where t.dt = '2025-07-31'
    group by
        store_id,
        date_format(stat_date, 'yyyyMMdd'),
        'day',
        page_category,
        page_name,
        traffic_source,
        stat_date,
        visit_identifier,
        visit_time
) path_agg
                   on main.store_id = path_agg.store_id
                       and main.time_dim = path_agg.time_dim
                       and main.page_category = path_agg.page_category
                       and main.page_name = path_agg.page_name
                       and main.traffic_source = path_agg.traffic_source
                       and main.stat_date = path_agg.stat_date
limit 100;