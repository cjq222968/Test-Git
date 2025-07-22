from pyspark.sql import SparkSession
import os
import re
import shutil
import atexit
import time
from datetime import datetime


# 初始化SparkSession
spark = SparkSession.builder \
    .appName("TmsAdsIntegration") \
    .master("local[*]") \
    .config("spark.hadoop.hive.metastore.uris", "thrift://cdh01:9083") \
    .config("spark.sql.warehouse.dir", "/home/user/hive/warehouse") \
    .config("spark.hadoop.hive.support.quoted.identifiers", "none") \
    .config("spark.hadoop.hive.exec.dynamic.partition.mode", "nonstrict") \
    .enableHiveSupport() \
    .getOrCreate()

sc = spark.sparkContext
sc.setLogLevel("ERROR")

# 日期变量
date = "2025-07-18"


# 临时目录管理
script_dir = os.path.dirname(os.path.abspath(__file__)) if "__file__" in locals() else os.getcwd()
temp_dir = os.path.join(script_dir, "spark-temp-dws")
os.makedirs(temp_dir, exist_ok=True)
os.chmod(temp_dir, 0o755)
spark.conf.set("spark.local.dir", temp_dir)


def create_and_load_ads_table(drop_sql, create_sql, insert_sql, check_sql):
    try:
        table_match = re.search(r'create external table (\w+\.\w+)', create_sql, re.IGNORECASE)
        if not table_match:
            raise ValueError("无法提取表名")
        full_table_name = table_match.group(1)
        table_name = full_table_name.split('.')[1]

        # 1. 删除旧表
        spark.sql(drop_sql)
        print(f"已删除旧表: {full_table_name}")

        # 2. 创建新表
        spark.sql(create_sql)
        print(f"已创建新表: {full_table_name}")

        # 3. 插入数据
        spark.sql("set hive.exec.dynamic.partition.mode=nonstrict;")
        spark.sql(insert_sql)
        print(f"表 {table_name} 数据插入完成")

        # 4. 验证数据
        print(f"表 {table_name} 数据验证:")
        spark.sql(check_sql).show(5)
        print(f"表处理成功: {table_name}\n")

    except Exception as e:
        print(f"表 {table_name} 处理失败: {str(e)}")
        clean_temp_dir()
        spark.stop()
        exit(1)


def clean_temp_dir(force=False):
    if not os.path.exists(temp_dir):
        print(f"临时目录 {temp_dir} 不存在，无需清理")
        return

    max_attempts = 3
    delay_seconds = 1
    for attempt in range(max_attempts):
        try:
            shutil.rmtree(temp_dir, ignore_errors=not force)
            if not os.path.exists(temp_dir):
                print(f"临时目录 {temp_dir} 已清理")
                return
            if attempt < max_attempts - 1:
                time.sleep(delay_seconds)
                delay_seconds *= 2
        except Exception as e:
            print(f"清理临时目录尝试 {attempt + 1} 失败: {str(e)}")
            if attempt < max_attempts - 1:
                time.sleep(delay_seconds)
                delay_seconds *= 2

    if os.name == 'nt':
        try:
            import ctypes
            ctypes.windll.kernel32.SetFileAttributesW(temp_dir, 0x80)
            shutil.rmtree(temp_dir, ignore_errors=True)
            print(f"已强制清理临时目录 {temp_dir}")
        except Exception as e:
            print(f"最终清理临时目录失败: {str(e)}")


atexit.register(clean_temp_dir, force=True)


# 1. 创建tms_ads数据库
spark.sql(f"""
create database IF NOT EXISTS tms_ads
    LOCATION 'hdfs://cdh01:8020/bigdata_warehouse/tms/tms_ads/';
""")
print("数据库 tms_ads 检查/创建成功")


# 2. 运单相关统计表
drop_sql = "drop table if exists tms_ads.ads_trans_order_stats;"
create_sql = f"""
create external table tms_ads.ads_trans_order_stats(
    recent_days tinyint COMMENT '最近天数,1:最近1天,7:最近7天,30:最近30天',
    receive_order_count bigint COMMENT '接单总数',
    receive_order_amount decimal(16,2) COMMENT '接单金额',
    dispatch_order_count bigint COMMENT '发单总数',
    dispatch_order_amount decimal(16,2) COMMENT '发单金额'
) comment '运单相关统计'
    partitioned by(dt string COMMENT '统计日期')
    stored as orc
    location 'hdfs://cdh01:8020/bigdata_warehouse/tms/tms_ads/ads_trans_order_stats'
    tblproperties('orc.compress' = 'snappy');
"""
insert_sql = f"""
insert overwrite table tms_ads.ads_trans_order_stats
    partition (dt)
select recent_days,
       receive_order_count,
       receive_order_amount,
       dispatch_order_count,
       dispatch_order_amount,
       '2025-07-18' as dt
from (
    select nvl(receive_1d.recent_days, dispatch_1d.recent_days) as recent_days,
           receive_order_count,
           receive_order_amount,
           dispatch_order_count,
           dispatch_order_amount
    from (
             select 1 as recent_days,
                    sum(order_count) as receive_order_count,
                    sum(order_amount) as receive_order_amount
             from tms_dws.dws_trans_org_receive_1d
             where dt = '2025-07-18'
         ) receive_1d
         full outer join (
        select 1 as recent_days,
               order_count as dispatch_order_count,
               order_amount as dispatch_order_amount
        from tms_dws.dws_trans_dispatch_1d
        where dt = '2025-07-18'
    ) dispatch_1d
                         on receive_1d.recent_days = dispatch_1d.recent_days
    union all
    select nvl(receive_nd.recent_days, dispatch_nd.recent_days) as recent_days,
           receive_order_count,
           receive_order_amount,
           dispatch_order_count,
           dispatch_order_amount
    from (
             select recent_days,
                    sum(order_count) as receive_order_count,
                    sum(order_amount) as receive_order_amount
             from tms_dws.dws_trans_org_receive_nd
             where dt = '2025-07-18'
               and recent_days in (7, 30)
             group by recent_days
         ) receive_nd
         full outer join (
        select recent_days,
               order_count as dispatch_order_count,
               order_amount as dispatch_order_amount
        from tms_dws.dws_trans_dispatch_nd
        where dt = '2025-07-18'
          and recent_days in (7, 30)
    ) dispatch_nd on receive_nd.recent_days = dispatch_nd.recent_days
);
"""
check_sql = "select dt, recent_days, receive_order_count from tms_ads.ads_trans_order_stats limit 5;"
create_and_load_ads_table(drop_sql, create_sql, insert_sql, check_sql)


# 3. 运输综合统计表
drop_sql = "drop table if exists tms_ads.ads_trans_stats;"
create_sql = f"""
create external table tms_ads.ads_trans_stats (
    recent_days tinyint COMMENT '最近天数，1:最近1天,7:最近7天,30:最近30天',
    trans_finish_count bigint COMMENT '完成运输次数',
    trans_finish_distance decimal (16,2) COMMENT '完成运输里程',
    trans_finish_dur_sec bigint COMMENT '完成运输时长，单位：秒'
) comment '运输综合统计'
    partitioned by(dt string COMMENT '统计日期')
    stored as orc
    location 'hdfs://cdh01:8020/bigdata_warehouse/tms/tms_ads/ads_trans_stats'
    tblproperties('orc.compress' = 'snappy');
"""
insert_sql = f"""
insert overwrite table tms_ads.ads_trans_stats
    partition (dt)
select recent_days,
       trans_finish_count,
       trans_finish_distance,
       trans_finish_dur_sec,
       '2025-07-18' as dt
from (
    -- 最近1天数据
    select 1 as recent_days,
           sum (trans_finish_count) as trans_finish_count,
           sum (trans_finish_distance) as trans_finish_distance,
           sum (trans_finish_dur_sec) as trans_finish_dur_sec
    from tms_dws.dws_trans_org_truck_model_type_trans_finish_1d
    where dt = '2025-07-18'
    union all
    -- 最近7天和30天数据
    select recent_days,
           sum (trans_finish_count) as trans_finish_count,
           sum (trans_finish_distance) as trans_finish_distance,
           sum (trans_finish_dur_sec) as trans_finish_dur_sec
    from tms_dws.dws_trans_shift_trans_finish_nd
    where dt = '2025-07-18'
      and recent_days in (7, 30)
    group by recent_days
);
"""
check_sql = "select dt, recent_days, trans_finish_count from tms_ads.ads_trans_stats limit 5;"
create_and_load_ads_table(drop_sql, create_sql, insert_sql, check_sql)


# 4. 历史至今运单统计表
drop_sql = "drop table if exists tms_ads.ads_trans_order_stats_td;"
create_sql = f"""
create external table tms_ads.ads_trans_order_stats_td(
    bounding_order_count bigint COMMENT '运输中运单总数',
    bounding_order_amount decimal(16,2) COMMENT '运输中运单金额'
) comment '历史至今运单统计'
    partitioned by(dt string COMMENT '统计日期')
    stored as orc
    location 'hdfs://cdh01:8020/bigdata_warehouse/tms/tms_ads/ads_trans_order_stats_td'
    tblproperties('orc.compress' = 'snappy');
"""
insert_sql = f"""
insert overwrite table tms_ads.ads_trans_order_stats_td
    partition (dt)
select sum(order_count) as bounding_order_count,
       sum(order_amount) as bounding_order_amount,
       dt
from (
    select dt,
           order_count,
           order_amount
    from tms_dws.dws_trans_dispatch_td
    where dt = '2025-07-18'
    union
    select dt,
           order_count * (-1) as order_count,
           order_amount * (-1) as order_amount
    from tms_dws.dws_trans_bound_finish_td
    where dt = '2025-07-18'
) new
group by dt;
"""
check_sql = "select bounding_order_count, bounding_order_amount, dt from tms_ads.ads_trans_order_stats_td limit 5;"
create_and_load_ads_table(drop_sql, create_sql, insert_sql, check_sql)


# 5. 运单综合统计表
drop_sql = "drop table if exists tms_ads.ads_order_stats;"
create_sql = f"""
create external table tms_ads.ads_order_stats(
    recent_days tinyint COMMENT '最近天数,1:最近1天,7:最近7天,30:最近30天',
    order_count bigint COMMENT '下单数',
    order_amount decimal(16,2) COMMENT '下单金额'
) comment '运单综合统计'
    partitioned by(dt string COMMENT '统计日期')
    stored as orc
    location 'hdfs://cdh01:8020/bigdata_warehouse/tms/tms_ads/ads_order_stats'
    tblproperties('orc.compress' = 'snappy');
"""
insert_sql = f"""
insert overwrite table tms_ads.ads_order_stats
    partition (dt)
select recent_days,
       order_count,
       order_amount,
       '2025-07-18' as dt
from (
    select 1 as recent_days,
           sum(order_count) as order_count,
           sum(order_amount) as order_amount
    from tms_dws.dws_trade_org_cargo_type_order_1d
    where dt = '2025-07-18'
    union
    select recent_days,
           sum(order_count) as order_count,
           sum(order_amount) as order_amount
    from tms_dws.dws_trade_org_cargo_type_order_nd
    where dt = '2025-07-18'
    group by recent_days
);
"""
check_sql = "select dt, recent_days, order_count from tms_ads.ads_order_stats order by recent_days limit 5;"
create_and_load_ads_table(drop_sql, create_sql, insert_sql, check_sql)


# 6. 各类型货物运单统计表
drop_sql = "drop table if exists tms_ads.ads_order_cargo_type_stats;"
create_sql = f"""
create external table tms_ads.ads_order_cargo_type_stats(
    cargo_type string COMMENT '货物类型',
    cargo_type_name string COMMENT '货物类型名称',
    recent_days tinyint COMMENT '最近天数,1:最近1天,7:最近7天,30:最近30天',
    order_count bigint COMMENT '下单数',
    order_amount decimal(16,2) COMMENT '下单金额'
) comment '各类型货物运单统计'
    partitioned by(dt string COMMENT '统计日期')
    stored as orc
    location 'hdfs://cdh01:8020/bigdata_warehouse/tms/tms_ads/ads_order_cargo_type_stats'
    tblproperties('orc.compress' = 'snappy');
"""
insert_sql = f"""
insert overwrite table tms_ads.ads_order_cargo_type_stats
    partition (dt)
select cargo_type,
       cargo_type_name,
       recent_days,
       order_count,
       order_amount,
       '2025-07-18' as dt
from (
    select cargo_type,
           cargo_type_name,
           1 as recent_days,
           sum(order_count) as order_count,
           sum(order_amount) as order_amount
    from tms_dws.dws_trade_org_cargo_type_order_1d
    where dt = '2025-07-18'
    group by cargo_type, cargo_type_name
    union
    select cargo_type,
           cargo_type_name,
           recent_days,
           sum(order_count) as order_count,
           sum(order_amount) as order_amount
    from tms_dws.dws_trade_org_cargo_type_order_nd
    where dt = '2025-07-18'
    group by cargo_type, cargo_type_name, recent_days
);
"""
check_sql = "select cargo_type_name, recent_days, order_count, dt from tms_ads.ads_order_cargo_type_stats order by recent_days limit 5;"
create_and_load_ads_table(drop_sql, create_sql, insert_sql, check_sql)


# 7. 城市分析表
drop_sql = "drop table if exists tms_ads.ads_city_stats;"
create_sql = f"""
create external table tms_ads.ads_city_stats(
    city_id string COMMENT '城市ID（字符串类型）',
    city_name string COMMENT '城市名称',
    recent_days bigint COMMENT '最近天数,1:最近1天,7:最近7天,30:最近30天',
    order_count bigint COMMENT '下单数',
    order_amount decimal(16,2) COMMENT '下单金额',
    trans_finish_count bigint COMMENT '完成运输次数',
    trans_finish_distance decimal(16,2) COMMENT '完成运输里程',
    trans_finish_dur_sec bigint COMMENT '完成运输时长，单位：秒',
    avg_trans_finish_distance decimal(16,2) COMMENT '平均每次运输里程',
    avg_trans_finish_dur_sec bigint COMMENT '平均每次运输时长，单位：秒'
) comment '城市分析'
    partitioned by(dt string COMMENT '统计日期')
    stored as orc
    location 'hdfs://cdh01:8020/bigdata_warehouse/tms/tms_ads/ads_city_stats'
    tblproperties('orc.compress' = 'snappy');
"""
insert_sql = f"""
insert overwrite table tms_ads.ads_city_stats
    partition (dt)
select city_id,
       city_name,
       recent_days,
       order_count,
       order_amount,
       trans_finish_count,
       trans_finish_distance,
       trans_finish_dur_sec,
       avg_trans_finish_distance,
       avg_trans_finish_dur_sec,
       '2025-07-18' as dt
from (
    select nvl(city_order_1d.city_id, city_trans_1d.city_id) as city_id,
           nvl(city_order_1d.city_name, city_trans_1d.city_name) as city_name,
           nvl(city_order_1d.recent_days, city_trans_1d.recent_days) as recent_days,
           order_count,
           order_amount,
           trans_finish_count,
           trans_finish_distance,
           trans_finish_dur_sec,
           avg_trans_finish_distance,
           avg_trans_finish_dur_sec
    from (
             select city_id,
                    city_name,
                    1 as recent_days,
                    sum(order_count) as order_count,
                    sum(order_amount) as order_amount
             from tms_dws.dws_trade_org_cargo_type_order_1d
             where dt = '2025-07-18'
             group by city_id, city_name
         ) city_order_1d
         full outer join (
        select city_id,
               city_name,
               1 as recent_days,
               sum(trans_finish_count) as trans_finish_count,
               sum(trans_finish_distance) as trans_finish_distance,
               sum(trans_finish_dur_sec) as trans_finish_dur_sec,
               sum(trans_finish_distance) / sum(trans_finish_count) as avg_trans_finish_distance,
               sum(trans_finish_dur_sec) / sum(trans_finish_count) as avg_trans_finish_dur_sec
        from (
                 select if(org_level = 1, city_for_level1.id, city_for_level2.id) as city_id,
                        if(org_level = 1, city_for_level1.name, city_for_level2.name) as city_name,
                        trans_finish_count,
                        trans_finish_distance,
                        trans_finish_dur_sec
                 from (
                          select org_id,
                                 trans_finish_count,
                                 trans_finish_distance,
                                 trans_finish_dur_sec
                          from tms_dws.dws_trans_org_truck_model_type_trans_finish_1d
                          where dt = '2025-07-18'
                      ) trans_origin
                          left join (
                     select id, org_level, region_id
                     from tms_dim.dim_organ_full
                     where dt = '2025-07-18'
                 ) organ on org_id = organ.id
                          left join (
                     select id, name, parent_id
                     from tms_dim.dim_region_full
                     where dt = '2025-07-18'
                 ) city_for_level1 on region_id = city_for_level1.id
                          left join (
                     select id, name
                     from tms_dim.dim_region_full
                     where dt = '2025-07-18'
                 ) city_for_level2 on city_for_level1.parent_id = city_for_level2.id
             ) trans_1d
        group by city_id, city_name
    ) city_trans_1d on city_order_1d.city_id = city_trans_1d.city_id and city_order_1d.city_name = city_trans_1d.city_name
    union
    select nvl(city_order_nd.city_id, city_trans_nd.city_id) as city_id,
           nvl(city_order_nd.city_name, city_trans_nd.city_name) as city_name,
           nvl(city_order_nd.recent_days, city_trans_nd.recent_days) as recent_days,
           order_count,
           order_amount,
           trans_finish_count,
           trans_finish_distance,
           trans_finish_dur_sec,
           avg_trans_finish_distance,
           avg_trans_finish_dur_sec
    from (
             select city_id,
                    city_name,
                    recent_days,
                    sum(order_count) as order_count,
                    sum(order_amount) as order_amount
             from tms_dws.dws_trade_org_cargo_type_order_nd
             where dt = '2025-07-18'
             group by city_id, city_name, recent_days
         ) city_order_nd
         full outer join (
        select city_id,
               city_name,
               recent_days,
               sum(trans_finish_count) as trans_finish_count,
               sum(trans_finish_distance) as trans_finish_distance,
               sum(trans_finish_dur_sec) as trans_finish_dur_sec,
               sum(trans_finish_distance) / sum(trans_finish_count) as avg_trans_finish_distance,
               sum(trans_finish_dur_sec) / sum(trans_finish_count) as avg_trans_finish_dur_sec
        from tms_dws.dws_trans_shift_trans_finish_nd
        where dt = '2025-07-18'
        group by city_id, city_name, recent_days
    ) city_trans_nd on city_order_nd.city_id = city_trans_nd.city_id and city_order_nd.city_name = city_trans_nd.city_name and city_order_nd.recent_days = city_trans_nd.recent_days
);
"""
check_sql = "select city_name, recent_days, order_count, dt from tms_ads.ads_city_stats order by recent_days limit 5;"
create_and_load_ads_table(drop_sql, create_sql, insert_sql, check_sql)


# 7. 机构分析表
drop_sql = "drop table if exists tms_ads.ads_org_stats;"
create_sql = f"""
create external table tms_ads.ads_org_stats(
    org_id bigint COMMENT '机构ID',
    org_name string COMMENT '机构名称',
    recent_days tinyint COMMENT '最近天数,1:最近1天,7:最近7天,30:最近30天',
    order_count bigint COMMENT '下单数',
    order_amount decimal(16,2) COMMENT '下单金额',
    trans_finish_count bigint COMMENT '完成运输次数',
    trans_finish_distance decimal(16,2) COMMENT '完成运输里程',
    trans_finish_dur_sec bigint COMMENT '完成运输时长，单位：秒',
    avg_trans_finish_distance decimal(16,2) COMMENT '平均每次运输里程',
    avg_trans_finish_dur_sec bigint COMMENT '平均每次运输时长，单位：秒'
) comment '机构分析'
    partitioned by(dt string COMMENT '统计日期')
    stored as orc
    location 'hdfs://cdh01:8020/bigdata_warehouse/tms/tms_ads/ads_org_stats'
    tblproperties('orc.compress' = 'snappy');
"""
insert_sql = f"""
insert overwrite table tms_ads.ads_org_stats
    partition (dt)
select org_id,
       org_name,
       recent_days,
       order_count,
       order_amount,
       trans_finish_count,
       trans_finish_distance,
       trans_finish_dur_sec,
       avg_trans_finish_distance,
       avg_trans_finish_dur_sec,
       '2025-07-18' as dt
from (
    select nvl(org_order_1d.org_id, org_trans_1d.org_id) as org_id,
           nvl(org_order_1d.org_name, org_trans_1d.org_name) as org_name,
           nvl(org_order_1d.recent_days, org_trans_1d.recent_days) as recent_days,
           order_count,
           order_amount,
           trans_finish_count,
           trans_finish_distance,
           trans_finish_dur_sec,
           avg_trans_finish_distance,
           avg_trans_finish_dur_sec
    from (
             select org_id,
                    org_name,
                    1 as recent_days,
                    sum(order_count) as order_count,
                    sum(order_amount) as order_amount
             from tms_dws.dws_trade_org_cargo_type_order_1d
             where dt = '2025-07-18'
             group by org_id, org_name
         ) org_order_1d
         full outer join (
        select org_id,
               org_name,
               1 as recent_days,
               sum(trans_finish_count) as trans_finish_count,
               sum(trans_finish_distance) as trans_finish_distance,
               sum(trans_finish_dur_sec) as trans_finish_dur_sec,
               sum(trans_finish_distance) / sum(trans_finish_count) as avg_trans_finish_distance,
               sum(trans_finish_dur_sec) / sum(trans_finish_count) as avg_trans_finish_dur_sec
        from tms_dws.dws_trans_org_truck_model_type_trans_finish_1d
        where dt = '2025-07-18'
        group by org_id, org_name
    ) org_trans_1d on org_order_1d.org_id = org_trans_1d.org_id and org_order_1d.org_name = org_trans_1d.org_name
    union
    select org_order_nd.org_id,
           org_order_nd.org_name,
           org_order_nd.recent_days,
           order_count,
           order_amount,
           trans_finish_count,
           trans_finish_distance,
           trans_finish_dur_sec,
           avg_trans_finish_distance,
           avg_trans_finish_dur_sec
    from (
             select org_id,
                    org_name,
                    recent_days,
                    sum(order_count) as order_count,
                    sum(order_amount) as order_amount
             from tms_dws.dws_trade_org_cargo_type_order_nd
             where dt = '2025-07-18'
             group by org_id, org_name, recent_days
         ) org_order_nd
         join (
        select org_id,
               org_name,
               recent_days,
               sum(trans_finish_count) as trans_finish_count,
               sum(trans_finish_distance) as trans_finish_distance,
               sum(trans_finish_dur_sec) as trans_finish_dur_sec,
               sum(trans_finish_distance) / sum(trans_finish_count) as avg_trans_finish_distance,
               sum(trans_finish_dur_sec) / sum(trans_finish_count) as avg_trans_finish_dur_sec
        from tms_dws.dws_trans_shift_trans_finish_nd
        where dt = '2025-07-18'
        group by org_id, org_name, recent_days
    ) org_trans_nd on org_order_nd.org_id = org_trans_nd.org_id and org_order_nd.org_name = org_trans_nd.org_name and org_order_nd.recent_days = org_trans_nd.recent_days
);
"""
check_sql = "select org_name, recent_days, order_count, dt from tms_ads.ads_org_stats order by recent_days limit 5;"
create_and_load_ads_table(drop_sql, create_sql, insert_sql, check_sql)


# 9. 班次分析表
drop_sql = "drop table if exists tms_ads.ads_shift_stats;"
create_sql = f"""
create external table tms_ads.ads_shift_stats(
    shift_id bigint COMMENT '班次ID',
    recent_days tinyint COMMENT '最近天数,7:最近7天,30:最近30天',
    trans_finish_count bigint COMMENT '完成运输次数',
    trans_finish_distance decimal(16,2) COMMENT '完成运输里程',
    trans_finish_dur_sec bigint COMMENT '完成运输时长，单位：秒',
    trans_finish_order_count bigint COMMENT '运输完成运单数'
) comment '班次分析'
    partitioned by(dt string COMMENT '统计日期')
    stored as orc
    location 'hdfs://cdh01:8020/bigdata_warehouse/tms/tms_ads/ads_shift_stats'
    tblproperties('orc.compress' = 'snappy');
"""
insert_sql = f"""
insert overwrite table tms_ads.ads_shift_stats
    partition (dt)
select shift_id,
       recent_days,
       trans_finish_count,
       trans_finish_distance,
       trans_finish_dur_sec,
       trans_finish_order_count,
       '2025-07-18' as dt
from (
    select shift_id,
           recent_days,
           trans_finish_count,
           trans_finish_distance,
           trans_finish_dur_sec,
           trans_finish_order_count
    from tms_dws.dws_trans_shift_trans_finish_nd
    where dt = '2025-07-18'
);
"""
check_sql = "select shift_id, recent_days, trans_finish_count, dt from tms_ads.ads_shift_stats order by recent_days limit 5;"
create_and_load_ads_table(drop_sql, create_sql, insert_sql, check_sql)


# 10. 线路分析表
drop_sql = "drop table if exists tms_ads.ads_line_stats;"
create_sql = f"""
create external table tms_ads.ads_line_stats(
    line_id bigint COMMENT '线路ID',
    line_name string COMMENT '线路名称',
    recent_days tinyint COMMENT '最近天数,7:最近7天,30:最近30天',
    trans_finish_count bigint COMMENT '完成运输次数',
    trans_finish_distance decimal(16,2) COMMENT '完成运输里程',
    trans_finish_dur_sec bigint COMMENT '完成运输时长，单位：秒',
    trans_finish_order_count bigint COMMENT '运输完成运单数'
) comment '线路分析'
    partitioned by(dt string COMMENT '统计日期')
    stored as orc
    location 'hdfs://cdh01:8020/bigdata_warehouse/tms/tms_ads/ads_line_stats'
    tblproperties('orc.compress' = 'snappy');
"""
insert_sql = f"""
insert overwrite table tms_ads.ads_line_stats
    partition (dt)
select line_id,
       line_name,
       recent_days,
       sum(trans_finish_count) as trans_finish_count,
       sum(trans_finish_distance) as trans_finish_distance,
       sum(trans_finish_dur_sec) as trans_finish_dur_sec,
       sum(trans_finish_order_count) as trans_finish_order_count,
       '2025-07-18' as dt
from tms_dws.dws_trans_shift_trans_finish_nd
where dt = '2025-07-18'
group by line_id, line_name, recent_days;
"""
check_sql = "select line_name, recent_days, trans_finish_count, dt from tms_ads.ads_line_stats order by recent_days limit 5;"
create_and_load_ads_table(drop_sql, create_sql, insert_sql, check_sql)


# 11. 司机分析表
drop_sql = "drop table if exists tms_ads.ads_driver_stats;"
create_sql = f"""
create external table tms_ads.ads_driver_stats(
    driver_emp_id bigint comment '第一司机员工ID',
    driver_name string comment '第一司机姓名',
    recent_days tinyint COMMENT '最近天数,7:最近7天,30:最近30天',
    trans_finish_count bigint COMMENT '完成运输次数',
    trans_finish_distance decimal(16,2) COMMENT '完成运输里程',
    trans_finish_dur_sec bigint COMMENT '完成运输时长，单位：秒',
    avg_trans_finish_distance decimal(16,2) COMMENT '平均每次运输里程',
    avg_trans_finish_dur_sec bigint COMMENT '平均每次运输时长，单位：秒'
) comment '司机分析'
    partitioned by(dt string COMMENT '统计日期')
    stored as orc
    location 'hdfs://cdh01:8020/bigdata_warehouse/tms/tms_ads/ads_driver_stats'
    tblproperties('orc.compress' = 'snappy');
"""
insert_sql = f"""
insert overwrite table tms_ads.ads_driver_stats
    partition (dt)
select driver_id as driver_emp_id,
       driver_name,
       recent_days,
       sum(trans_finish_count) as trans_finish_count,
       sum(trans_finish_distance) as trans_finish_distance,
       sum(trans_finish_dur_sec) as trans_finish_dur_sec,
       sum(trans_finish_distance) / sum(trans_finish_count) as avg_trans_finish_distance,
       sum(trans_finish_dur_sec) / sum(trans_finish_count) as avg_trans_finish_dur_sec,
       '2025-07-18' as dt
from (
    select recent_days,
           driver1_emp_id as driver_id,
           driver1_name as driver_name,
           trans_finish_count,
           trans_finish_distance,
           trans_finish_dur_sec
    from tms_dws.dws_trans_shift_trans_finish_nd
    where dt = '2025-07-18'
      and driver2_emp_id is null
    union
    select recent_days,
           cast(driver_info[0] as bigint) as driver_id,
           driver_info[1] as driver_name,
           trans_finish_count,
           trans_finish_distance / 2 as trans_finish_distance,
           trans_finish_dur_sec / 2 as trans_finish_dur_sec
    from (
             select recent_days,
                    array(array(driver1_emp_id, driver1_name), array(driver2_emp_id, driver2_name)) as driver_arr,
                    trans_finish_count,
                    trans_finish_distance,
                    trans_finish_dur_sec
             from tms_dws.dws_trans_shift_trans_finish_nd
             where dt = '2025-07-18'
               and driver2_emp_id is not null
         ) t1
             lateral view explode(driver_arr) tmp as driver_info
) t2
group by driver_id, driver_name, recent_days;
"""
check_sql = "select driver_name, recent_days, trans_finish_count, dt from tms_ads.ads_driver_stats order by recent_days limit 5;"
create_and_load_ads_table(drop_sql, create_sql, insert_sql, check_sql)


# 12. 卡车分析表
drop_sql = "drop table if exists tms_ads.ads_truck_stats;"
create_sql = f"""
create external table tms_ads.ads_truck_stats(
    truck_model_type string COMMENT '卡车类别编码',
    truck_model_type_name string COMMENT '卡车类别名称',
    recent_days tinyint COMMENT '最近天数,1:最近1天,7:最近7天,30:最近30天',
    trans_finish_count bigint COMMENT '完成运输次数',
    trans_finish_distance decimal(16,2) COMMENT '完成运输里程',
    trans_finish_dur_sec bigint COMMENT '完成运输时长，单位：秒',
    avg_trans_finish_distance decimal(16,2) COMMENT '平均每次运输里程',
    avg_trans_finish_dur_sec bigint COMMENT '平均每次运输时长，单位：秒'
) comment '卡车分析'
    partitioned by(dt string COMMENT '统计日期')
    stored as orc
    location 'hdfs://cdh01:8020/bigdata_warehouse/tms/tms_ads/ads_truck_stats'
    tblproperties('orc.compress' = 'snappy');
"""
insert_sql = f"""
insert overwrite table tms_ads.ads_truck_stats
    partition (dt)
select truck_model_type,
       truck_model_type_name,
       recent_days,
       sum(trans_finish_count) as trans_finish_count,
       sum(trans_finish_distance) as trans_finish_distance,
       sum(trans_finish_dur_sec) as trans_finish_dur_sec,
       sum(trans_finish_distance) / sum(trans_finish_count) as avg_trans_finish_distance,
       sum(trans_finish_dur_sec) / sum(trans_finish_count) as avg_trans_finish_dur_sec,
       '2025-07-18' as dt
from tms_dws.dws_trans_shift_trans_finish_nd
where dt = '2025-07-18'
group by truck_model_type, truck_model_type_name, recent_days;
"""
check_sql = "select truck_model_type_name, recent_days, trans_finish_count, dt from tms_ads.ads_truck_stats order by recent_days limit 5;"
create_and_load_ads_table(drop_sql, create_sql, insert_sql, check_sql)


# 13. 快递综合统计表
drop_sql = "drop table if exists tms_ads.ads_express_stats;"
create_sql = f"""
create external table tms_ads.ads_express_stats(
    recent_days tinyint COMMENT '最近天数,1:最近1天,7:最近7天,30:最近30天',
    deliver_suc_count bigint COMMENT '派送成功次数（订单数）',
    sort_count bigint COMMENT '分拣次数'
) comment '快递综合统计'
    partitioned by(dt string COMMENT '统计日期')
    stored as orc
    location 'hdfs://cdh01:8020/bigdata_warehouse/tms/tms_ads/ads_express_stats'
    tblproperties('orc.compress' = 'snappy');
"""
insert_sql = f"""
insert overwrite table tms_ads.ads_express_stats
    partition (dt)
select recent_days,
       deliver_suc_count,
       sort_count,
       '2025-07-18' as dt
from (
    select nvl(deliver_1d.recent_days, sort_1d.recent_days) as recent_days,
           deliver_suc_count,
           sort_count
    from (
             select 1 as recent_days,
                    sum(order_count) as deliver_suc_count
             from tms_dws.dws_trans_org_deliver_suc_1d
             where dt = '2025-07-18'
         ) deliver_1d
         full outer join (
        select 1 as recent_days,
               sum(sort_count) as sort_count
        from tms_dws.dws_trans_org_sort_1d
        where dt = '2025-07-18'
    ) sort_1d on deliver_1d.recent_days = sort_1d.recent_days
    union
    select nvl(deliver_nd.recent_days, sort_nd.recent_days) as recent_days,
           deliver_suc_count,
           sort_count
    from (
             select recent_days,
                    sum(order_count) as deliver_suc_count
             from tms_dws.dws_trans_org_deliver_suc_nd
             where dt = '2025-07-18'
             group by recent_days
         ) deliver_nd
         full outer join (
        select recent_days,
               sum(sort_count) as sort_count
        from tms_dws.dws_trans_org_sort_nd
        where dt = '2025-07-18'
        group by recent_days
    ) sort_nd on deliver_nd.recent_days = sort_nd.recent_days
);
"""
check_sql = "select recent_days, deliver_suc_count, sort_count, dt from tms_ads.ads_express_stats order by recent_days limit 5;"
create_and_load_ads_table(drop_sql, create_sql, insert_sql, check_sql)


# 14. 各省份快递统计表
drop_sql = "drop table if exists tms_ads.ads_express_province_stats;"
create_sql = f"""
create external table tms_ads.ads_express_province_stats(
    province_id string COMMENT '省份ID（字符串类型）',
    province_name string COMMENT '省份名称',
    recent_days tinyint COMMENT '最近天数,1:最近1天,7:最近7天,30:最近30天',
    receive_order_count bigint COMMENT '揽收次数',
    receive_order_amount decimal(16,2) COMMENT '揽收金额',
    deliver_suc_count bigint COMMENT '派送成功次数',
    sort_count bigint COMMENT '分拣次数'
) comment '各省份快递统计'
    partitioned by(dt string COMMENT '统计日期')
    stored as orc
    location 'hdfs://cdh01:8020/bigdata_warehouse/tms/tms_ads/ads_express_province_stats'
    tblproperties('orc.compress' = 'snappy');
"""
insert_sql = f"""
insert overwrite table tms_ads.ads_express_province_stats
    partition (dt)
select province_id,
       province_name,
       recent_days,
       receive_order_count,
       receive_order_amount,
       deliver_suc_count,
       sort_count,
       '2025-07-18' as dt
from (
    select nvl(nvl(province_deliver_1d.province_id, province_sort_1d.province_id), province_receive_1d.province_id) as province_id,
           nvl(nvl(province_deliver_1d.province_name, province_sort_1d.province_name), province_receive_1d.province_name) as province_name,
           nvl(nvl(province_deliver_1d.recent_days, province_sort_1d.recent_days), province_receive_1d.recent_days) as recent_days,
           sum(province_receive_1d.receive_order_count) as receive_order_count,
           sum(province_receive_1d.receive_order_amount) as receive_order_amount,
           sum(province_deliver_1d.deliver_suc_count) as deliver_suc_count,
           sum(province_sort_1d.sort_count) as sort_count
    from (
             select cast(province_id as string) as province_id,
                    province_name,
                    1 as recent_days,
                    sum(order_count) as deliver_suc_count
             from tms_dws.dws_trans_org_deliver_suc_1d
             where dt = '2025-07-18'
             group by province_id, province_name
         ) province_deliver_1d
         full outer join (
        select cast(province_id as string) as province_id,
               province_name,
               1 as recent_days,
               sum(sort_count) as sort_count
        from tms_dws.dws_trans_org_sort_1d
        where dt = '2025-07-18'
        group by province_id, province_name
    ) province_sort_1d on province_deliver_1d.province_id = province_sort_1d.province_id and province_deliver_1d.province_name = province_sort_1d.province_name
         full outer join (
        select cast(province_id as string) as province_id,
               province_name,
               1 as recent_days,
               sum(order_count) as receive_order_count,
               sum(order_amount) as receive_order_amount
        from tms_dws.dws_trans_org_receive_1d
        where dt = '2025-07-18'
        group by province_id, province_name
    ) province_receive_1d on province_deliver_1d.province_id = province_receive_1d.province_id and province_deliver_1d.province_name = province_receive_1d.province_name
    group by nvl(nvl(province_deliver_1d.province_id, province_sort_1d.province_id), province_receive_1d.province_id),
             nvl(nvl(province_deliver_1d.province_name, province_sort_1d.province_name), province_receive_1d.province_name),
             nvl(nvl(province_deliver_1d.recent_days, province_sort_1d.recent_days), province_receive_1d.recent_days)
    union all
    select nvl(nvl(province_deliver_nd.province_id, province_sort_nd.province_id), province_receive_nd.province_id) as province_id,
           nvl(nvl(province_deliver_nd.province_name, province_sort_nd.province_name), province_receive_nd.province_name) as province_name,
           nvl(nvl(province_deliver_nd.recent_days, province_sort_nd.recent_days), province_receive_nd.recent_days) as recent_days,
           sum(province_receive_nd.receive_order_count) as receive_order_count,
           sum(province_receive_nd.receive_order_amount) as receive_order_amount,
           sum(province_deliver_nd.deliver_suc_count) as deliver_suc_count,
           sum(province_sort_nd.sort_count) as sort_count
    from (
             select cast(province_id as string) as province_id,
                    province_name,
                    recent_days,
                    sum(order_count) as deliver_suc_count
             from tms_dws.dws_trans_org_deliver_suc_nd
             where dt = '2025-07-18'
             group by recent_days, province_id, province_name
         ) province_deliver_nd
         full outer join (
        select cast(province_id as string) as province_id,
               province_name,
               recent_days,
               sum(sort_count) as sort_count
        from tms_dws.dws_trans_org_sort_nd
        where dt = '2025-07-18'
        group by recent_days, province_id, province_name
    ) province_sort_nd on province_deliver_nd.province_id = province_sort_nd.province_id and province_deliver_nd.province_name = province_sort_nd.province_name and province_deliver_nd.recent_days = province_sort_nd.recent_days
         full outer join (
        select cast(province_id as string) as province_id,
               province_name,
               recent_days,
               sum(order_count) as receive_order_count,
               sum(order_amount) as receive_order_amount
        from tms_dws.dws_trans_org_receive_nd
        where dt = '2025-07-18'
        group by recent_days, province_id, province_name
    ) province_receive_nd on province_deliver_nd.province_id = province_receive_nd.province_id and province_deliver_nd.province_name = province_receive_nd.province_name and province_deliver_nd.recent_days = province_receive_nd.recent_days
    group by nvl(nvl(province_deliver_nd.province_id, province_sort_nd.province_id), province_receive_nd.province_id),
             nvl(nvl(province_deliver_nd.province_name, province_sort_nd.province_name), province_receive_nd.province_name),
             nvl(nvl(province_deliver_nd.recent_days, province_sort_nd.recent_days), province_receive_nd.recent_days)
);
"""
check_sql = "select province_name, recent_days, receive_order_count, dt from tms_ads.ads_express_province_stats order by recent_days limit 5;"
create_and_load_ads_table(drop_sql, create_sql, insert_sql, check_sql)


# 15. 各城市快递统计表
drop_sql = "drop table if exists tms_ads.ads_express_city_stats;"
create_sql = f"""
create external table tms_ads.ads_express_city_stats(
    city_id string COMMENT '城市ID（字符串类型）',
    city_name string COMMENT '城市名称',
    recent_days tinyint COMMENT '最近天数,1:最近1天,7:最近7天,30:最近30天',
    receive_order_count bigint COMMENT '揽收次数',
    receive_order_amount decimal(16,2) COMMENT '揽收金额',
    deliver_suc_count bigint COMMENT '派送成功次数',
    sort_count bigint COMMENT '分拣次数'
) comment '各城市快递统计'
    partitioned by(dt string COMMENT '统计日期')
    stored as orc
    location 'hdfs://cdh01:8020/bigdata_warehouse/tms/tms_ads/ads_express_city_stats'
    tblproperties('orc.compress' = 'snappy');
"""
insert_sql = f"""
insert overwrite table tms_ads.ads_express_city_stats
    partition (dt)
select city_id,
       city_name,
       recent_days,
       receive_order_count,
       receive_order_amount,
       deliver_suc_count,
       sort_count,
       '2025-07-18' as dt
from (
    select nvl(nvl(city_deliver_1d.city_id, city_sort_1d.city_id), city_receive_1d.city_id) as city_id,
           nvl(nvl(city_deliver_1d.city_name, city_sort_1d.city_name), city_receive_1d.city_name) as city_name,
           nvl(nvl(city_deliver_1d.recent_days, city_sort_1d.recent_days), city_receive_1d.recent_days) as recent_days,
           sum(city_receive_1d.receive_order_count) as receive_order_count,
           sum(city_receive_1d.receive_order_amount) as receive_order_amount,
           sum(city_deliver_1d.deliver_suc_count) as deliver_suc_count,
           sum(city_sort_1d.sort_count) as sort_count
    from (
             select cast(city_id as string) as city_id,
                    city_name,
                    1 as recent_days,
                    sum(order_count) as deliver_suc_count
             from tms_dws.dws_trans_org_deliver_suc_1d
             where dt = '2025-07-18'
             group by city_id, city_name
         ) city_deliver_1d
         full outer join (
        select cast(city_id as string) as city_id,
               city_name,
               1 as recent_days,
               sum(sort_count) as sort_count
        from tms_dws.dws_trans_org_sort_1d
        where dt = '2025-07-18'
        group by city_id, city_name
    ) city_sort_1d on city_deliver_1d.city_id = city_sort_1d.city_id and city_deliver_1d.city_name = city_sort_1d.city_name
         full outer join (
        select cast(city_id as string) as city_id,
               city_name,
               1 as recent_days,
               sum(order_count) as receive_order_count,
               sum(order_amount) as receive_order_amount
        from tms_dws.dws_trans_org_receive_1d
        where dt = '2025-07-18'
        group by city_id, city_name
    ) city_receive_1d on city_deliver_1d.city_id = city_receive_1d.city_id and city_deliver_1d.city_name = city_receive_1d.city_name
    group by nvl(nvl(city_deliver_1d.city_id, city_sort_1d.city_id), city_receive_1d.city_id),
             nvl(nvl(city_deliver_1d.city_name, city_sort_1d.city_name), city_receive_1d.city_name),
             nvl(nvl(city_deliver_1d.recent_days, city_sort_1d.recent_days), city_receive_1d.recent_days)
    union all
    select nvl(nvl(city_deliver_nd.city_id, city_sort_nd.city_id), city_receive_nd.city_id) as city_id,
           nvl(nvl(city_deliver_nd.city_name, city_sort_nd.city_name), city_receive_nd.city_name) as city_name,
           nvl(nvl(city_deliver_nd.recent_days, city_sort_nd.recent_days), city_receive_nd.recent_days) as recent_days,
           sum(city_receive_nd.receive_order_count) as receive_order_count,
           sum(city_receive_nd.receive_order_amount) as receive_order_amount,
           sum(city_deliver_nd.deliver_suc_count) as deliver_suc_count,
           sum(city_sort_nd.sort_count) as sort_count
    from (
             select cast(city_id as string) as city_id,
                    city_name,
                    recent_days,
                    sum(order_count) as deliver_suc_count
             from tms_dws.dws_trans_org_deliver_suc_nd
             where dt = '2025-07-18'
             group by recent_days, city_id, city_name
         ) city_deliver_nd
         full outer join (
        select cast(city_id as string) as city_id,
               city_name,
               recent_days,
               sum(sort_count) as sort_count
        from tms_dws.dws_trans_org_sort_nd
        where dt = '2025-07-18'
        group by recent_days, city_id, city_name
    ) city_sort_nd on city_deliver_nd.city_id = city_sort_nd.city_id and city_deliver_nd.city_name = city_sort_nd.city_name and city_deliver_nd.recent_days = city_sort_nd.recent_days
         full outer join (
        select cast(city_id as string) as city_id,
               city_name,
               recent_days,
               sum(order_count) as receive_order_count,
               sum(order_amount) as receive_order_amount
        from tms_dws.dws_trans_org_receive_nd
        where dt = '2025-07-18'
        group by recent_days, city_id, city_name
    ) city_receive_nd on city_deliver_nd.city_id = city_receive_nd.city_id and city_deliver_nd.city_name = city_receive_nd.city_name and city_deliver_nd.recent_days = city_receive_nd.recent_days
    group by nvl(nvl(city_deliver_nd.city_id, city_sort_nd.city_id), city_receive_nd.city_id),
             nvl(nvl(city_deliver_nd.city_name, city_sort_nd.city_name), city_receive_nd.city_name),
             nvl(nvl(city_deliver_nd.recent_days, city_sort_nd.recent_days), city_receive_nd.recent_days)
);
"""
check_sql = "select city_name, recent_days, receive_order_count, dt from tms_ads.ads_express_city_stats order by recent_days limit 5;"
create_and_load_ads_table(drop_sql, create_sql, insert_sql, check_sql)


# 16. 各机构快递统计表
drop_sql = "drop table if exists tms_ads.ads_express_org_stats;"
create_sql = f"""
create external table tms_ads.ads_express_org_stats(
    org_id bigint COMMENT '机构ID',
    org_name string COMMENT '机构名称',
    recent_days tinyint COMMENT '最近天数,1:最近1天,7:最近7天,30:最近30天',
    receive_order_count bigint COMMENT '揽收次数',
    receive_order_amount decimal(16,2) COMMENT '揽收金额',
    deliver_suc_count bigint COMMENT '派送成功次数',
    sort_count bigint COMMENT '分拣次数'
) comment '各机构快递统计'
    partitioned by(dt string COMMENT '统计日期')
    stored as orc
    location 'hdfs://cdh01:8020/bigdata_warehouse/tms/tms_ads/ads_express_org_stats'
    tblproperties('orc.compress' = 'snappy');
"""
insert_sql = f"""
insert overwrite table tms_ads.ads_express_org_stats
    partition (dt)
select org_id,
       org_name,
       recent_days,
       receive_order_count,
       receive_order_amount,
       deliver_suc_count,
       sort_count,
       '2025-07-18' as dt
from (
    select nvl(nvl(org_deliver_1d.org_id, org_sort_1d.org_id), org_receive_1d.org_id) as org_id,
           nvl(nvl(org_deliver_1d.org_name, org_sort_1d.org_name), org_receive_1d.org_name) as org_name,
           nvl(nvl(org_deliver_1d.recent_days, org_sort_1d.recent_days), org_receive_1d.recent_days) as recent_days,
           receive_order_count,
           receive_order_amount,
           deliver_suc_count,
           sort_count
    from (
             select org_id,
                    org_name,
                    1 as recent_days,
                    sum(order_count) as deliver_suc_count
             from from tms_dws.dws_trans_org_deliver_suc_1d
             where dt = '2025-07-18'
             group by org_id, org_name
         ) org_deliver_1d
         full outer join (
        select org_id,
               org_name,
               1 as recent_days,
               sum(sort_count) as sort_count
        from tms_dws.dws_trans_org_sort_1d
        where dt = '2025-07-18'
        group by org_id, org_name
    ) org_sort_1d on org_deliver_1d.org_id = org_sort_1d.org_id and org_deliver_1d.org_name = org_sort_1d.org_name
         full outer join (
        select org_id,
               org_name,
               1 as recent_days,
               sum(order_count) as receive_order_count,
               sum(order_amount) as receive_order_amount
        from tms_dws.dws_trans_org_receive_1d
        where dt = '2025-07-18'
        group by org_id, org_name
    ) org_receive_1d on org_deliver_1d.org_id = org_receive_1d.org_id and org_deliver_1d.org_name = org_receive_1d.org_name
    union
    select nvl(nvl(org_deliver_nd.org_id, org_sort_nd.org_id), org_receive_nd.org_id) as org_id,
           nvl(nvl(org_deliver_nd.org_name, org_sort_nd.org_name), org_receive_nd.org_name) as org_name,
           nvl(nvl(org_deliver_nd.recent_days, org_sort_nd.recent_days), org_receive_nd.recent_days) as recent_days,
           receive_order_count,
           receive_order_amount,
           deliver_suc_count,
           sort_count
    from (
             select org_id,
                    org_name,
                    recent_days,
                    sum(order_count) as deliver_suc_count
             from tms_dws.dws_trans_org_deliver_suc_nd
             where dt = '2025-07-18'
             group by recent_days, org_id, org_name
         ) org_deliver_nd
         full outer join (
        select org_id,
               org_name,
               recent_days,
               sum(sort_count) as sort_count
        from tms_dws.dws_trans_org_sort_nd
        where dt = '2025-07-18'
        group by recent_days, org_id, org_name
    ) org_sort_nd on org_deliver_nd.org_id = org_sort_nd.org_id and org_deliver_nd.org_name = org_sort_nd.org_name and org_deliver_nd.recent_days = org_sort_nd.recent_days
         full outer join (
        select org_id,
               org_name,
               recent_days,
               sum(order_count) as receive_order_count,
               sum(order_amount) as receive_order_amount
        from tms_dws.dws_trans_org_receive_nd
        where dt = '2025-07-18'
        group by recent_days, org_id, org_name
    ) org_receive_nd on org_deliver_nd.org_id = org_receive_nd.org_id and org_deliver_nd.org_name = org_receive_nd.org_name and org_deliver_nd.recent_days = org_receive_nd.recent_days
);
"""
check_sql = "select org_name, recent_days, receive_order_count, dt from tms_ads.ads_express_org_stats order by recent_days limit 5;"
create_and_load_ads_table(drop_sql, create_sql, insert_sql, check_sql)


# 停止SparkSession
spark.stop()
print("所有ADS层表处理完成")