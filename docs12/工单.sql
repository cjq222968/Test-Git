-- 访问行为临时表（ODS核心表，补充设备类型ID）
create database ecommerce_analytics;

DROP TABLE IF EXISTS ecommerce_analytics.teemp_tbl;
CREATE TABLE ecommerce_analytics.teemp_tbl (
    id INT COMMENT '记录唯一ID',
    visit_identifier STRING COMMENT '访问会话标识',
    store_id INT COMMENT '店铺ID',
    item_id INT COMMENT '商品ID',
    page_type STRING COMMENT '页面类型（关联page_type表page_name）',
    device_type_id INT COMMENT '设备类型ID（1=移动端，2=PC端）',  -- 必传字段
    traffic_source_id INT COMMENT '流量入口ID（PC端必传，移动端为0）',  -- 非NULL
    visit_time TIMESTAMP COMMENT '访问时间'
) COMMENT '电商访问行为ODS层表';

-- 插入数据，修复device_type_id全行一致问题
INSERT INTO ecommerce_analytics.teemp_tbl (
    id, store_id, item_id, page_type, device_type_id,
    visit_identifier, visit_time, traffic_source_id
)
SELECT
    row_number() OVER () AS id,
    FLOOR(1 + RAND() * 100) AS store_id,  -- 1-100有效店铺ID
    CASE
        WHEN pt.page_name IN ('宝贝页', '预售页') THEN FLOOR(1 + RAND() * 500)
        ELSE 0
        END AS item_id,
    pt.page_name AS page_type,  -- 取自page_type表的所有页面类型
    -- 每行独立生成设备类型：1=移动端，2=PC端
    CASE WHEN RAND() > 0.5 THEN 1 ELSE 2 END AS device_type_id,
    UUID() AS visit_identifier,
    from_unixtime(unix_timestamp('2025-07-31 00:00:00') + FLOOR(RAND() * 86400)) AS visit_time,
    -- 流量入口ID：根据当前行device_type_id动态生成
    CASE
        WHEN CASE WHEN RAND() > 0.5 THEN 1 ELSE 2 END = 2 THEN FLOOR(1 + RAND() * 8)
        ELSE 0
        END AS traffic_source_id
FROM
    ecommerce_analytics.page_type pt
        -- 交叉连接扩大数据量
        CROSS JOIN (
        SELECT 1 AS n UNION SELECT 2 UNION SELECT 3 UNION SELECT 4 UNION SELECT 5
        UNION SELECT 6 UNION SELECT 7 UNION SELECT 8 UNION SELECT 9 UNION SELECT 10
    ) t1
        CROSS JOIN (
        SELECT 1 AS n UNION SELECT 2 UNION SELECT 3 UNION SELECT 4 UNION SELECT 5
    ) t2
LIMIT 500;

-- 验证数据分布
SELECT
    device_type_id,
    COUNT(*) AS count,
    ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER (), 2) AS percentage
FROM ecommerce_analytics.teemp_tbl
GROUP BY device_type_id;


select * from ecommerce_analytics.teemp_tbl;


-- 设备类型表（ODS维度表）
drop table if exists ecommerce_analytics.device_type;
CREATE TABLE ecommerce_analytics.device_type (
    id INT COMMENT '设备类型ID',
    type_name STRING COMMENT '设备类型：移动端、PC端',
    description STRING COMMENT '类型描述'
) COMMENT '设备类型维度表';

INSERT INTO ecommerce_analytics.device_type (id, type_name, description)
SELECT 1, '移动端', '手机、平板等无线设备' UNION ALL
SELECT 2, 'PC端', '电脑端网页访问';

select * from ecommerce_analytics.device_type;


-- 页面类型表（ODS维度表，补充"未知页"确保关联全覆盖）
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


-- 流量入口表（ODS维度表）
drop table if exists ecommerce_analytics.traffic_source;
CREATE TABLE ecommerce_analytics.traffic_source (
    id INT COMMENT '流量入口ID',
    source_name STRING COMMENT '流量入口名称',
    source_desc STRING COMMENT '入口描述'
) COMMENT '流量入口维度表';

INSERT INTO ecommerce_analytics.traffic_source (id, source_name, source_desc)
SELECT 1, '搜索引擎', '百度、谷歌等' UNION ALL
SELECT 2, '直接访问', '用户直接输入网址' UNION ALL
SELECT 3, '外部链接', '其他网站跳转' UNION ALL
SELECT 4, '社交媒体', '微信、微博等' UNION ALL
SELECT 5, '广告投放', '付费广告' UNION ALL
SELECT 6, '平台推荐', '电商平台首页推荐' UNION ALL
SELECT 7, '收藏夹', '浏览器收藏夹' UNION ALL
SELECT 8, '短信链接', '营销短信跳转';  -- 新增移动端默认值

select * from ecommerce_analytics.traffic_source;

-- 创建店铺访问统计事实表
DROP TABLE IF EXISTS ecommerce_analytics.shop_visit_stats;
CREATE TABLE ecommerce_analytics.shop_visit_stats (
    store_id INT COMMENT '店铺ID',
    device_type_id INT COMMENT '设备类型ID',
    page_type_id INT COMMENT '页面类型ID',
    visitor_count INT COMMENT '访客数',
    buyer_count INT COMMENT '下单买家数',
    pv INT COMMENT '浏览量',
    avg_stay_time DECIMAL(10,2) COMMENT '平均停留时长(秒)',
    stat_date DATE COMMENT '统计日期'
) COMMENT '店铺访问统计事实表'
    PARTITIONED BY (dt STRING COMMENT '分区字段');

-- 插入店铺访问统计数据（修正了buyer_count的计算逻辑）
INSERT INTO ecommerce_analytics.shop_visit_stats (
    store_id, device_type_id, page_type_id, visitor_count, buyer_count,
    pv, avg_stay_time, stat_date, dt
)
SELECT
    FLOOR(1 + RAND() * 100) AS store_id,
    FLOOR(1 + RAND() * 2) AS device_type_id,
    p.id AS page_type_id,
    FLOOR(10 + RAND() * 490) AS visitor_count,
    -- 修正：直接使用访客数的计算逻辑，避免同层引用别名
    FLOOR(1 + RAND() * LEAST(20, FLOOR(10 + RAND() * 490) - 1)) AS buyer_count,
    FLOOR(50 + RAND() * 950) AS pv,
    ROUND(10 + RAND() * 290, 2) AS avg_stay_time,
    '2025-07-31' AS stat_date,
    '2025-07-31' AS dt
FROM
    -- 生成1000条基础数据（10×10×10）
    (
        SELECT 1 AS n
        FROM (SELECT 1 AS n UNION SELECT 2 UNION SELECT 3 UNION SELECT 4 UNION SELECT 5 UNION SELECT 6 UNION SELECT 7 UNION SELECT 8 UNION SELECT 9 UNION SELECT 10) t1
                 CROSS JOIN (SELECT 1 AS n UNION SELECT 2 UNION SELECT 3 UNION SELECT 4 UNION SELECT 5 UNION SELECT 6 UNION SELECT 7 UNION SELECT 8 UNION SELECT 9 UNION SELECT 10) t2
                 CROSS JOIN (SELECT 1 AS n UNION SELECT 2 UNION SELECT 3 UNION SELECT 4 UNION SELECT 5 UNION SELECT 6 UNION SELECT 7 UNION SELECT 8 UNION SELECT 9 UNION SELECT 10) t3
    ) base_data
        -- 关联页面类型表的所有10条记录
        JOIN ecommerce_analytics.page_type p ON 1=1
GROUP BY
    base_data.n,
    p.id,
    p.page_category,
    p.page_name;

-- 查询验证插入结果
SELECT * FROM ecommerce_analytics.shop_visit_stats;





-- 店铺路径分析表（ODS事实表）
DROP TABLE IF EXISTS ecommerce_analytics.shop_path_analysis;
CREATE TABLE ecommerce_analytics.shop_path_analysis (
    store_id INT COMMENT '店铺ID',
    device_type_id INT COMMENT '设备类型ID',
    page_type_id INT COMMENT '页面类型ID',
    path_sequence STRING COMMENT '页面路径序列',
    conversion_rate DECIMAL(8,4) COMMENT '转化率',
    stat_date DATE COMMENT '统计日期'
) COMMENT '店铺路径分析事实表'
    PARTITIONED BY (dt STRING COMMENT '分区字段');

-- 生成用于路径起点的临时表
WITH StartPage AS (
    SELECT
        page_type_id,
        page_category,
        page_name,
        RAND() AS rnd,
        -- 为每个分类生成随机排序，用于获取同分类随机起点
        ROW_NUMBER() OVER (PARTITION BY page_category ORDER BY RAND()) AS cat_rn
    FROM (
             SELECT
                 id AS page_type_id,
                 page_category,
                 page_name
             FROM ecommerce_analytics.page_type
             WHERE page_name IN ('首页','活动页','分类页','新品页','宝贝页','订阅页','直播页','会员页','热卖页','预售页')
         ) t
),
-- 生成用于路径终点的临时表
     EndPage AS (
         SELECT
             page_name,
             RAND() AS rnd,
             -- 为全局页面生成随机排序，用于获取全局随机终点
             ROW_NUMBER() OVER (ORDER BY RAND()) AS global_rn
         FROM (
                  SELECT
                      page_name
                  FROM ecommerce_analytics.page_type
                  WHERE page_name IN ('首页','活动页','分类页','新品页','宝贝页','订阅页','直播页','会员页','热卖页','预售页')
              ) t
     )
-- 插入数据到目标表
INSERT INTO ecommerce_analytics.shop_path_analysis (
    store_id, device_type_id, page_type_id, path_sequence, conversion_rate,
    stat_date, dt
)
SELECT
    FLOOR(1 + RAND() * 100) AS store_id,
    1 AS device_type_id,
    rp.page_type_id,
    -- 拼接路径：同分类随机起点 -> 全局随机终点 -> 随机数字后缀
    CONCAT(
            sp.page_name,
            '->',
            ep.page_name,
            '->',
            FLOOR(1 + RAND() * 10)
    ) AS path_sequence,
    ROUND(0.01 + RAND() * 0.14, 4) AS conversion_rate,
    '2025-07-31' AS stat_date,
    '2025-07-31' AS dt
FROM (
         SELECT
             id AS page_type_id,
             page_category,
             page_name,
             RAND() AS rnd
         FROM ecommerce_analytics.page_type
         WHERE page_name IN ('首页','活动页','分类页','新品页','宝贝页','订阅页','直播页','会员页','热卖页','预售页')
     ) rp
-- 关联同分类随机起点（取每个分类的第一条随机记录）
         JOIN StartPage sp ON rp.page_category = sp.page_category AND sp.cat_rn = 1
-- 关联全局随机终点（取全局的第一条随机记录）
         JOIN EndPage ep ON ep.global_rn = 1
-- 4 重 CROSS JOIN 生成 10×10×10×10 = 10000 条基础记录
         CROSS JOIN (
    SELECT 1 AS n UNION SELECT 2 UNION SELECT 3 UNION SELECT 4 UNION SELECT 5
    UNION SELECT 6 UNION SELECT 7 UNION SELECT 8 UNION SELECT 9 UNION SELECT 10
) t1
         CROSS JOIN (
    SELECT 1 AS n UNION SELECT 2 UNION SELECT 3 UNION SELECT 4 UNION SELECT 5
    UNION SELECT 6 UNION SELECT 7 UNION SELECT 8 UNION SELECT 9 UNION SELECT 10
) t2
GROUP BY
    rp.page_type_id,
    rp.page_category,
    rp.page_name,
    rp.rnd,
    sp.page_name,
    ep.page_name;

-- 验证数据
SELECT * FROM ecommerce_analytics.shop_path_analysis;


-- 页面访问排行表（ODS事实表）
DROP TABLE IF EXISTS ecommerce_analytics.page_visit_ranking;
CREATE TABLE ecommerce_analytics.page_visit_ranking (
    store_id INT COMMENT '店铺ID',
    device_type_id INT COMMENT '设备类型ID',
    page_type_id INT COMMENT '页面类型ID',
    page_name STRING COMMENT '页面名称',
    bounce_rate DECIMAL(8,4) COMMENT '跳出率',
    stat_date DATE COMMENT '统计日期'
) COMMENT '页面访问排行事实表'
    PARTITIONED BY (dt STRING COMMENT '分区字段');

INSERT INTO ecommerce_analytics.page_visit_ranking (
    store_id, device_type_id, page_type_id, page_name, bounce_rate,
    stat_date, dt
)
SELECT
    FLOOR(1 + RAND() * 100) AS store_id,  -- 随机生成1 - 100的店铺ID
    -- 动态生成设备类型ID，1代表移动端，2代表PC端，可根据实际类型扩展
    CASE
        WHEN RAND() < 0.6 THEN 1  -- 假设60%概率是移动端
        ELSE 2  -- 40%概率是PC端，比例可按需调整
        END AS device_type_id,
    pt.id AS page_type_id,  -- 从page_type表取page_type_id
    pt.page_name AS page_name,  -- 从page_type表取page_name
    ROUND(0.05 + RAND() * 0.45, 4) AS bounce_rate,  -- 生成0.05 - 0.5的跳出率
    '2025-07-31' AS stat_date,
    '2025-07-31' AS dt
FROM (
         -- 通过3重交叉连接生成基础记录，用于扩展数据量
         SELECT 1 AS n UNION SELECT 2 UNION SELECT 3 UNION SELECT 4 UNION SELECT 5
         UNION SELECT 6 UNION SELECT 7 UNION SELECT 8 UNION SELECT 9 UNION SELECT 10
     ) t1
         CROSS JOIN (
    SELECT 1 AS n UNION SELECT 2 UNION SELECT 3 UNION SELECT 4 UNION SELECT 5
    UNION SELECT 6 UNION SELECT 7 UNION SELECT 8 UNION SELECT 9 UNION SELECT 10
) t2
-- 关联page_type表，随机选取页面类型（这里用CROSS JOIN + RAND()实现随机，不同引擎语法有差异，Hive中可这样用）
         CROSS JOIN (
    SELECT id, page_name
    FROM ecommerce_analytics.page_type  -- 替换为实际的page_type表名
    -- 可根据需要添加WHERE条件筛选页面类型，比如只选特定分类
    -- WHERE page_category = '店铺页'
) pt
-- 按比例随机选（如果想控制各类型占比，可调整RAND()条件，这里简单随机）
WHERE RAND() < 1;  -- 1表示全选，可改0.5之类的比例控制选取概率
;

select * from ecommerce_analytics.page_visit_ranking;




----------------------------------dwd
-- DWD 用户访问明细宽表
DROP TABLE IF EXISTS ecommerce_analytics.dwd_user_visit_detail;
CREATE TABLE ecommerce_analytics.dwd_user_visit_detail (
    id BIGINT COMMENT '记录唯一ID',
    visit_identifier STRING COMMENT '访问会话标识',
    store_id INT COMMENT '店铺ID',
    item_id INT COMMENT '商品ID',
    page_type STRING COMMENT '页面类型（如首页、商品详情页）',
    page_category STRING COMMENT '页面大类（关联page_type维度）',
    page_name STRING COMMENT '页面名称（关联page_type维度）',
    device_type STRING COMMENT '设备类型（移动端/PC端）',
    traffic_source STRING COMMENT '流量入口名称',
    visit_time TIMESTAMP COMMENT '访问时间',
    visit_date DATE COMMENT '访问日期（拆分维度）',
    hour_of_day INT COMMENT '访问小时（0-23）'
) COMMENT 'DWD层-用户访问明细宽表';

INSERT INTO ecommerce_analytics.dwd_user_visit_detail
SELECT
    t.id,
    t.visit_identifier,
    t.store_id,
    t.item_id,
    t.page_type,  -- 保留原始 page_type 字段
    pt.page_category,
    pt.page_name,
    dt.type_name AS device_type,
    ts.source_name AS traffic_source,
    t.visit_time,
    DATE(t.visit_time) AS visit_date,
    HOUR(t.visit_time) AS hour_of_day
FROM ecommerce_analytics.teemp_tbl t
         JOIN ecommerce_analytics.device_type dt
              ON t.device_type_id = dt.id
-- 关键优化：用页面名称动态关联，替代硬编码 CASE WHEN
         JOIN ecommerce_analytics.page_type pt
              ON t.page_type = pt.page_name
         JOIN ecommerce_analytics.traffic_source ts
              ON t.traffic_source_id = ts.id;


select * from ecommerce_analytics.dwd_user_visit_detail;

-- DWD 店铺访问汇总表
DROP TABLE IF EXISTS ecommerce_analytics.dwd_shop_visit_summary;
CREATE TABLE ecommerce_analytics.dwd_shop_visit_summary (
    store_id INT COMMENT '店铺ID',
    device_type STRING COMMENT '设备类型（移动端/PC端）',
    page_type STRING COMMENT '页面类型',
    page_category STRING COMMENT '页面大类',
    page_name STRING COMMENT '页面名称',
    visitor_count INT COMMENT '访客数（来自shop_visit_stats）',
    buyer_count INT COMMENT '下单买家数（来自shop_visit_stats）',
    pv INT COMMENT '浏览量（来自shop_visit_stats）',
    avg_stay_time DECIMAL(10,2) COMMENT '平均停留时长(秒)',
    stat_date DATE COMMENT '统计日期'
) COMMENT 'DWD层-店铺访问汇总表';

INSERT INTO ecommerce_analytics.dwd_shop_visit_summary
SELECT
    s.store_id,
    dt.type_name AS device_type,
    pt.page_name AS page_type,  -- 用维度表替换原始ID
    pt.page_category,
    pt.page_name,
    s.visitor_count,
    s.buyer_count,
    s.pv,
    s.avg_stay_time,
    s.stat_date
FROM ecommerce_analytics.shop_visit_stats s
         JOIN ecommerce_analytics.device_type dt
              ON s.device_type_id = dt.id
         JOIN ecommerce_analytics.page_type pt
              ON s.page_type_id = pt.id;


select * from ecommerce_analytics.dwd_shop_visit_summary;



-- DWD 页面路径分析宽表
DROP TABLE IF EXISTS ecommerce_analytics.dwd_shop_path_analysis_detail;
CREATE TABLE ecommerce_analytics.dwd_shop_path_analysis_detail (
    store_id INT COMMENT '店铺ID',
    device_type STRING COMMENT '设备类型（仅移动端）',
    page_type STRING COMMENT '页面类型',
    page_category STRING COMMENT '页面大类',
    page_name STRING COMMENT '页面名称',
    path_sequence STRING COMMENT '页面路径序列',
    conversion_rate DECIMAL(8,4) COMMENT '转化率',
    stat_date DATE COMMENT '统计日期'
) COMMENT 'DWD层-页面路径分析宽表';

INSERT INTO ecommerce_analytics.dwd_shop_path_analysis_detail
SELECT
    p.store_id,
    dt.type_name AS device_type,  -- 仅移动端，可直接关联
    pt.page_name AS page_type,
    pt.page_category,
    pt.page_name,
    p.path_sequence,
    p.conversion_rate,
    p.stat_date
FROM ecommerce_analytics.shop_path_analysis p
         JOIN ecommerce_analytics.device_type dt
              ON p.device_type_id = dt.id
         JOIN ecommerce_analytics.page_type pt
              ON p.page_type_id = pt.id;


select * from ecommerce_analytics.dwd_shop_path_analysis_detail;




----------------------------------dws
-- DWS 无线端流量与路径汇总表
DROP TABLE IF EXISTS ecommerce_analytics.dws_wireless_traffic_path_summary;
CREATE TABLE ecommerce_analytics.dws_wireless_traffic_path_summary (
    stat_period STRING COMMENT '统计周期（日/7天/30天/月）',
    page_type STRING COMMENT '页面类型',
    entry_page_category STRING COMMENT '进店页面类型（首页/商品详情页/其他）',
    entry_page_name STRING COMMENT '进店页面名称',
    visitor_count INT COMMENT '访客数',
    buyer_count INT COMMENT '下单买家数',
    pv INT COMMENT '浏览量',
    avg_stay_time DECIMAL(10,2) COMMENT '平均停留时长(秒)',
    path_conversion_rate DECIMAL(8,4) COMMENT '路径转化率',
    stat_date_start DATE COMMENT '统计周期起始日期',
    stat_date_end DATE COMMENT '统计周期结束日期'
) COMMENT 'DWS层-无线端流量与路径汇总表';

-- 先清空可能存在的旧数据（可选）
TRUNCATE TABLE ecommerce_analytics.dws_wireless_traffic_path_summary;

-- 插入统计数据，支持多周期
INSERT INTO ecommerce_analytics.dws_wireless_traffic_path_summary
SELECT
    CASE
        WHEN periods.diff_days = 1 THEN '日'
        WHEN periods.diff_days = 7 THEN '7天'
        WHEN periods.diff_days = 30 THEN '30天'
        END AS stat_period,
    t.page_type,
    t.page_category AS entry_page_category,
    t.page_name AS entry_page_name,
    SUM(s.visitor_count) AS visitor_count,
    SUM(s.buyer_count) AS buyer_count,
    SUM(s.pv) AS pv,
    AVG(s.avg_stay_time) AS avg_stay_time,
    AVG(p.conversion_rate) AS path_conversion_rate,
    DATE(t.visit_time) AS stat_date_start,
    DATE_ADD(DATE(t.visit_time), periods.diff_days - 1) AS stat_date_end
FROM
    (SELECT 1 AS diff_days UNION ALL
     SELECT 7 AS diff_days UNION ALL
     SELECT 30 AS diff_days) AS periods
        CROSS JOIN ecommerce_analytics.dwd_user_visit_detail t
        JOIN ecommerce_analytics.dwd_shop_visit_summary s
             ON t.page_type = s.page_type
                 AND t.page_category = s.page_category
                 AND t.visit_time BETWEEN s.stat_date AND DATE_ADD(s.stat_date, periods.diff_days - 1)
        JOIN ecommerce_analytics.dwd_shop_path_analysis_detail p
             ON t.page_type = p.page_type
                 AND t.page_category = p.page_category
                 AND t.visit_time BETWEEN p.stat_date AND DATE_ADD(p.stat_date, periods.diff_days - 1)
WHERE t.device_type = '移动端'
  AND DATE(t.visit_time) >= '2025-07-01'  -- 确保有足够历史数据
GROUP BY
    periods.diff_days,
    t.page_type,
    t.page_category,
    t.page_name,
    DATE(t.visit_time);

SELECT * FROM ecommerce_analytics.dws_wireless_traffic_path_summary;


-- DWS PC 端流量入口与排行表
DROP TABLE IF EXISTS ecommerce_analytics.dws_pc_traffic_source_ranking;
CREATE TABLE ecommerce_analytics.dws_pc_traffic_source_ranking (
    stat_period STRING COMMENT '统计周期（日/7天/30天/月）',
    traffic_source STRING COMMENT '流量入口名称',
    page_type STRING COMMENT '页面类型',
    page_name STRING COMMENT '页面名称',
    visitor_count INT COMMENT '访客数',
    pv INT COMMENT '浏览量',
    avg_stay_time DECIMAL(10,2) COMMENT '平均停留时长(秒)',
    source_rank INT COMMENT '流量入口TOP20排名',
    stat_date_start DATE COMMENT '统计周期起始日期',
    stat_date_end DATE COMMENT '统计周期结束日期'
) COMMENT 'DWS层-PC端流量入口与排行表';

-- 插入多周期统计数据，计算流量入口TOP20
INSERT INTO ecommerce_analytics.dws_pc_traffic_source_ranking
SELECT * FROM (
      SELECT
          CASE
              WHEN periods.diff_days = 1 THEN '日'
              WHEN periods.diff_days = 7 THEN '7天'
              WHEN periods.diff_days = 30 THEN '30天'
              END AS stat_period,
          t.traffic_source,
          t.page_type,
          t.page_name,
          SUM(s.visitor_count) AS visitor_count,
          SUM(s.pv) AS pv,
          AVG(s.avg_stay_time) AS avg_stay_time,
          ROW_NUMBER() OVER (
              PARTITION BY periods.diff_days, DATE(t.visit_time)
              ORDER BY SUM(s.visitor_count) DESC
              ) AS source_rank,
          DATE(t.visit_time) AS stat_date_start,
          DATE_ADD(DATE(t.visit_time), periods.diff_days - 1) AS stat_date_end
      FROM
          (SELECT 1 AS diff_days UNION ALL
           SELECT 7 AS diff_days UNION ALL
           SELECT 30 AS diff_days) AS periods
              CROSS JOIN ecommerce_analytics.dwd_user_visit_detail t
              JOIN ecommerce_analytics.dwd_shop_visit_summary s
                   ON t.page_type = s.page_type
                       AND t.visit_time BETWEEN s.stat_date AND DATE_ADD(s.stat_date, periods.diff_days - 1)
      WHERE t.device_type = 'PC端'
        AND DATE(t.visit_time) >= '2025-07-01'  -- 确保有足够历史数据
      GROUP BY
          periods.diff_days,
          t.traffic_source,
          t.page_type,
          t.page_name,
          DATE(t.visit_time)
  ) ranked_data
WHERE source_rank <= 20;  -- 使用子查询过滤排名前20

SELECT * FROM ecommerce_analytics.dws_pc_traffic_source_ranking;



-- DWS 店内路径流转汇总表
DROP TABLE IF EXISTS ecommerce_analytics.dws_shop_internal_path_flow;
CREATE TABLE ecommerce_analytics.dws_shop_internal_path_flow (
    stat_period STRING COMMENT '统计周期（日/7天/30天/月）',
    source_page_type STRING COMMENT '来源页面类型',
    target_page_type STRING COMMENT '去向页面类型',
    visitor_count INT COMMENT '访客数',
    conversion_rate DECIMAL(8,4) COMMENT '路径转化率',
    stat_date_start DATE COMMENT '统计周期起始日期',
    stat_date_end DATE COMMENT '统计周期结束日期'
) COMMENT 'DWS层-店内路径流转汇总表';
-- 插入多周期统计数据，计算店内路径流转情况
INSERT INTO ecommerce_analytics.dws_shop_internal_path_flow
SELECT
    CASE
        WHEN periods.diff_days = 1 THEN '日'
        WHEN periods.diff_days = 7 THEN '7天'
        WHEN periods.diff_days = 30 THEN '30天'
        END AS stat_period,
    t.page_type AS source_page_type,
    p.page_type AS target_page_type,
    COUNT (DISTINCT t.visit_identifier) AS visitor_count,
    AVG (p.conversion_rate) AS conversion_rate,
    DATE (t.visit_time) AS stat_date_start,
    DATE_ADD (DATE (t.visit_time), periods.diff_days - 1) AS stat_date_end
FROM
    (SELECT 1 AS diff_days UNION ALL
     SELECT 7 AS diff_days UNION ALL
     SELECT 30 AS diff_days) AS periods
        CROSS JOIN ecommerce_analytics.dwd_user_visit_detail t
        JOIN ecommerce_analytics.dwd_shop_path_analysis_detail p
             ON t.page_type = p.page_type
                 AND t.visit_time BETWEEN p.stat_date AND DATE_ADD (p.stat_date, periods.diff_days - 1)
WHERE DATE (t.visit_time) >= '2025-07-01' -- 确保有足够历史数据
GROUP BY
    periods.diff_days,
    t.page_type,
    p.page_type,
    DATE (t.visit_time);

SELECT * FROM ecommerce_analytics.dws_shop_internal_path_flow;




-- 无线端核心指标看板（面向运营决策，聚合关键指标）
DROP TABLE IF EXISTS ecommerce_analytics.ads_wireless_traffic_dashboard;
CREATE TABLE ecommerce_analytics.ads_wireless_traffic_dashboard (
    stat_period STRING COMMENT '统计周期（日/7天/30天/月）',
    entry_page_category STRING COMMENT '进店页面类型',
    entry_page_name STRING COMMENT '进店页面名称',
    visitor_count INT COMMENT '访客数',
    buyer_count INT COMMENT '下单买家数',
    pv INT COMMENT '浏览量',
    avg_stay_time DECIMAL(10,2) COMMENT '平均停留时长(秒)',
    path_conversion_rate DECIMAL(8,4) COMMENT '路径转化率',
    stat_date_start DATE COMMENT '统计周期起始日期',
    stat_date_end DATE COMMENT '统计周期结束日期',
    -- 新增业务决策指标
    pv_per_visitor DECIMAL(10,2) COMMENT '人均浏览量',
    buyer_ratio DECIMAL(8,4) COMMENT '下单买家占比'
) COMMENT 'ADS层-无线端流量路径核心看板';

INSERT INTO ecommerce_analytics.ads_wireless_traffic_dashboard
SELECT
    stat_period,
    entry_page_category,
    entry_page_name,
    visitor_count,
    buyer_count,
    pv,
    avg_stay_time,
    path_conversion_rate,
    stat_date_start,
    stat_date_end,
    pv / visitor_count AS pv_per_visitor,
    buyer_count / visitor_count AS buyer_ratio
FROM ecommerce_analytics.dws_wireless_traffic_path_summary
WHERE stat_period IN ('日', '7天', '30天')
;


select * from ecommerce_analytics.ads_wireless_traffic_dashboard;




-- PC端流量入口排行看板（突出TOP20及占比）
DROP TABLE IF EXISTS ecommerce_analytics.ads_pc_traffic_source_top20;
CREATE TABLE ecommerce_analytics.ads_pc_traffic_source_top20 (
    stat_period STRING COMMENT '统计周期（日/7天/30天/月）',
    traffic_source STRING COMMENT '流量入口名称',
    page_type STRING COMMENT '页面类型',
    page_name STRING COMMENT '页面名称',
    visitor_count INT COMMENT '访客数',
    pv INT COMMENT '浏览量',
    avg_stay_time DECIMAL(10,2) COMMENT '平均停留时长(秒)',
    source_rank INT COMMENT '流量入口TOP20排名',
    stat_date_start DATE COMMENT '统计周期起始日期',
    stat_date_end DATE COMMENT '统计周期结束日期',
    visitor_count_ratio DECIMAL(8,4) COMMENT '访客数占比',
    pv_ratio DECIMAL(8,4) COMMENT '浏览量占比'
) COMMENT 'ADS层-PC端流量入口TOP20看板';
INSERT INTO ecommerce_analytics.ads_pc_traffic_source_top20
SELECT
    d.stat_period,
    d.traffic_source,
    d.page_type,
    d.page_name,
    d.visitor_count,
    d.pv,
    d.avg_stay_time,
    d.source_rank,
    d.stat_date_start,
    d.stat_date_end,
-- 计算占比（处理除以 0 的情况）
    CASE WHEN t.total_visitor = 0 THEN 0 ELSE d.visitor_count/t.total_visitor END AS visitor_count_ratio,
    CASE WHEN t.total_pv = 0 THEN 0 ELSE d.pv/t.total_pv END AS pv_ratio
FROM ecommerce_analytics.dws_pc_traffic_source_ranking d
         JOIN (
-- 将 WITH 子句转换为子查询
    SELECT
-- 注意：原 DWS 表中没有 diff_days 字段，需用 stat_period 映射
CASE
    WHEN stat_period = '日' THEN 1
    WHEN stat_period = '7天' THEN 7
    WHEN stat_period = '30天' THEN 30
    END AS diff_days,
SUM (visitor_count) AS total_visitor,
SUM (pv) AS total_pv,
stat_date_start
    FROM ecommerce_analytics.dws_pc_traffic_source_ranking
    WHERE stat_period IN ('日', '7天', '30天')
    GROUP BY stat_period, stat_date_start -- 用 stat_period 替代 diff_days 分组
) t ON
-- 用 stat_period 映射关系替代 diff_days 的直接关联
    (d.stat_period = '日' AND t.diff_days = 1) OR
    (d.stat_period = '7天' AND t.diff_days = 7) OR
    (d.stat_period = '30天' AND t.diff_days = 30)
        AND d.stat_date_start = t.stat_date_start
WHERE d.source_rank <= 20;
SELECT * FROM ecommerce_analytics.ads_pc_traffic_source_top20;




-- 店内路径流转洞察（聚焦转化与路径优化）
DROP TABLE IF EXISTS ecommerce_analytics.ads_shop_internal_path_insight;
CREATE TABLE ecommerce_analytics.ads_shop_internal_path_insight (
    stat_period STRING COMMENT '统计周期（日/7天/30天/月）',
    source_page_type STRING COMMENT '来源页面类型',
    target_page_type STRING COMMENT '去向页面类型',
    visitor_count INT COMMENT '访客数',
    conversion_rate DECIMAL(8,4) COMMENT '路径转化率',
    stat_date_start DATE COMMENT '统计周期起始日期',
    stat_date_end DATE COMMENT '统计周期结束日期',
    conversion_rate_trend DECIMAL(8,4) COMMENT '转化率变化率'
) COMMENT 'ADS层-店内路径流转洞察表';

INSERT INTO ecommerce_analytics.ads_shop_internal_path_insight
SELECT
    d.stat_period,
    d.source_page_type,
    d.target_page_type,
    d.visitor_count,
    d.conversion_rate,
    d.stat_date_start,
    d.stat_date_end,
    (d.conversion_rate - COALESCE(p.prev_conversion, 0))
        / COALESCE(p.prev_conversion, 1) AS conversion_rate_trend
FROM (
         SELECT
             CASE stat_period
                 WHEN '日' THEN 1
                 WHEN '7天' THEN 7
                 WHEN '30天' THEN 30
                 END AS diff_days,
             stat_period,
             source_page_type,
             target_page_type,
             visitor_count,
             conversion_rate,
             stat_date_start,
             stat_date_end
         FROM ecommerce_analytics.dws_shop_internal_path_flow
         WHERE stat_period IN ('日', '7天', '30天')
     ) d
         LEFT JOIN (
    SELECT
        CASE stat_period
            WHEN '日' THEN 1
            WHEN '7天' THEN 7
            WHEN '30天' THEN 30
            END AS diff_days,
        source_page_type,
        target_page_type,
        conversion_rate AS prev_conversion,
        stat_date_start
    FROM ecommerce_analytics.dws_shop_internal_path_flow
    WHERE stat_period IN ('日', '7天', '30天')
) p
                   ON d.diff_days = p.diff_days
                       AND d.source_page_type = p.source_page_type
                       AND d.target_page_type = p.target_page_type
                       AND d.stat_date_start = DATE_SUB(p.stat_date_start, d.diff_days)
WHERE d.stat_period IN ('日', '7天', '30天');


select * from ecommerce_analytics.ads_shop_internal_path_insight;