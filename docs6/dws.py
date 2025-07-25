from pyspark.sql import SparkSession
import os
import re
import shutil
import atexit
import time
from datetime import datetime


# 初始化SparkSession
spark = SparkSession.builder \
    .appName("TmsDwsIntegration") \
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


def create_and_load_dws_table(drop_sql, create_sql, insert_sql, check_sql):
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


# 1. 创建tms_dws数据库
spark.sql(f"""
create database IF NOT EXISTS tms_dws
    LOCATION 'hdfs://cdh01:8020/bigdata_warehouse/tms/tms_dws/';
""")
print("数据库 tms_dws 检查/创建成功")


# 2. 交易域机构货物类型粒度下单1日汇总表（已成功）
drop_sql = "drop table if exists tms_dws.dws_trade_org_cargo_type_order_1d;"
create_sql = f"""
create external table tms_dws.dws_trade_org_cargo_type_order_1d(
    org_id bigint comment '机构ID',
    org_name string comment '转运站名称',
    city_id string comment '城市ID（字符串类型，适配源数据）',
    city_name string comment '城市名称',
    cargo_type string comment '货物类型',
    cargo_type_name string comment '货物类型名称',
    order_count bigint comment '下单数',
    order_amount decimal(16,2) comment '下单金额'
) comment '交易域机构货物类型粒度下单 1 日汇总表'
    partitioned by(dt string comment '统计日期')
    stored as orc
    location 'hdfs://cdh01:8020/bigdata_warehouse/tms/tms_dws/dws_trade_org_cargo_type_order_1d'
    tblproperties('orc.compress' = 'snappy');
"""
insert_sql = f"""
insert overwrite table tms_dws.dws_trade_org_cargo_type_order_1d
    partition (dt)
select org_id,
       org_name,
       city_id,
       region.name city_name,
       cargo_type,
       cargo_type_name,
       order_count,
       order_amount,
       dt
from (select org_id,
             org_name,
             sender_city_id  city_id,
             cargo_type,
             cargo_type_name,
             count(order_id) order_count,
             sum(amount)     order_amount,
             dt
      from (select order_id,
                   cargo_type,
                   cargo_type_name,
                   sender_district_id,
                   sender_city_id,
                   max(amount) amount,
                   dt
            from (select order_id,
                         cargo_type,
                         cargo_type_name,
                         sender_district_id,
                         sender_city_id,
                         amount,
                         dt
                  from tms_dwd.dwd_trade_order_detail_inc
                  where dt = '{date}') detail
            group by order_id, cargo_type, cargo_type_name, sender_district_id, sender_city_id, dt) distinct_detail
               left join
           (select id org_id, org_name, region_id
            from tms_dim.dim_organ_full
            where dt = '{date}') org
           on distinct_detail.sender_city_id = org.region_id
      group by org_id, org_name, cargo_type, cargo_type_name, sender_city_id, dt) agg
         left join (
    select id, name
    from tms_dim.dim_region_full
    where dt = '{date}'
) region on agg.city_id = region.id;
"""
check_sql = "select org_id, org_name, city_id, order_count, dt from tms_dws.dws_trade_org_cargo_type_order_1d limit 5;"
create_and_load_dws_table(drop_sql, create_sql, insert_sql, check_sql)


# 3. 物流域转运站粒度揽收1日汇总表（已成功）
drop_sql = "drop table if exists tms_dws.dws_trans_org_receive_1d;"
create_sql = f"""
create external table tms_dws.dws_trans_org_receive_1d(
    org_id bigint comment '转运站ID',
    org_name string comment '转运站名称',
    city_id string comment '城市ID（字符串类型）',
    city_name string comment '城市名称',
    province_id string comment '省份ID（字符串类型）',
    province_name string comment '省份名称',
    order_count bigint comment '揽收次数',
    order_amount decimal(16, 2) comment '揽收金额'
) comment '物流域转运站粒度揽收 1 日汇总表'
    partitioned by (dt string comment '统计日期')
    stored as orc
    location 'hdfs://cdh01:8020/bigdata_warehouse/tms/tms_dws/dws_trans_org_receive_1d'
    tblproperties ('orc.compress'='snappy');
"""
insert_sql = f"""
insert overwrite table tms_dws.dws_trans_org_receive_1d
    partition (dt)
select org_id,
       org_name,
       city_id,
       city_name,
       province_id,
       province_name,
       count(order_id) as order_count,
       sum(amount) as order_amount,
       dt
from (
    select 
        d.order_id,
        d.amount,
        d.dt,
        d.sender_city_id as city_id,
        d.sender_province_id as province_id,
        o.org_id,
        o.org_name,
        c.name as city_name,
        p.name as province_name
    from (select * from tms_dwd.dwd_trans_receive_detail_inc where dt = '{date}') d
    left join (
        select id org_id, org_name, region_id 
        from tms_dim.dim_organ_full 
        where dt = '{date}'
    ) o on d.sender_city_id = o.region_id
    left join (
        select id, name 
        from tms_dim.dim_region_full 
        where dt = '{date}' 
          and parent_id in (select id from tms_dim.dim_region_full where dt = '{date}' and parent_id = 86)
    ) c on d.sender_city_id = c.id
    left join (
        select id, name 
        from tms_dim.dim_region_full 
        where dt = '{date}' 
          and parent_id = 86
    ) p on d.sender_province_id = p.id
) detail
group by org_id, org_name, city_id, city_name, province_id, province_name, dt;
"""
check_sql = "select org_id, city_name, order_count, dt from tms_dws.dws_trans_org_receive_1d limit 5;"
create_and_load_dws_table(drop_sql, create_sql, insert_sql, check_sql)


# 4. 物流域发单1日汇总表（已成功）
drop_sql = "drop table if exists tms_dws.dws_trans_dispatch_1d;"
create_sql = f"""
create external table tms_dws.dws_trans_dispatch_1d(
    order_count bigint comment '发单总数',
    order_amount decimal(16,2) comment '发单总金额'
) comment '物流域发单 1 日汇总表'
    partitioned by (dt string comment '统计日期')
    stored as orc
    location 'hdfs://cdh01:8020/bigdata_warehouse/tms/tms_dws/dws_trans_dispatch_1d'
    tblproperties('orc.compress'='snappy');
"""
insert_sql = f"""
insert overwrite table tms_dws.dws_trans_dispatch_1d
    partition (dt)
select count(order_id) as order_count,
       sum(amount) as order_amount,
       dt
from (select order_id,
             dt,
             max(amount) as amount
      from tms_dwd.dwd_trans_dispatch_detail_inc
      where dt = '{date}'
      group by order_id, dt) distinct_info
group by dt;
"""
check_sql = "select order_count, order_amount, dt from tms_dws.dws_trans_dispatch_1d limit 5;"
create_and_load_dws_table(drop_sql, create_sql, insert_sql, check_sql)


# 5. 物流域机构卡车类别粒度运输1日汇总表（已成功）
drop_sql = "drop table if exists tms_dws.dws_trans_org_truck_model_type_trans_finish_1d;"
create_sql = f"""
create external table tms_dws.dws_trans_org_truck_model_type_trans_finish_1d(
    org_id bigint comment '机构ID',
    org_name string comment '机构名称',
    truck_model_type string comment '卡车类别编码',
    truck_model_type_name string comment '卡车类别名称',
    trans_finish_count bigint comment '运输完成次数',
    trans_finish_distance decimal(16,2) comment '运输完成里程',
    trans_finish_dur_sec bigint comment '运输完成时长（秒）'
) comment '物流域机构卡车类别粒度运输最近 1 日汇总表'
    partitioned by (dt string comment '统计日期')
    stored as orc
    location 'hdfs://cdh01:8020/bigdata_warehouse/tms/tms_dws/dws_trans_org_truck_model_type_trans_finish_1d/'
    tblproperties('orc.compress'='snappy');
"""
insert_sql = f"""
insert overwrite table tms_dws.dws_trans_org_truck_model_type_trans_finish_1d
    partition (dt)
select org_id,
       org_name,
       truck_model_type,
       truck_model_type_name,
       count(trans_finish.id) as trans_finish_count,
       sum(actual_distance) as trans_finish_distance,
       sum(finish_dur_sec) as trans_finish_dur_sec,
       dt
from (select id,
             start_org_id   org_id,
             start_org_name org_name,
             truck_id,
             actual_distance,
             finish_dur_sec,
             dt
      from tms_dwd.dwd_trans_trans_finish_inc
      where dt = '{date}') trans_finish
         left join
     (select id,
             truck_model_type,
             truck_model_type_name
      from tms_dim.dim_truck_full
      where dt = '{date}') truck_info
     on trans_finish.truck_id = truck_info.id
group by org_id, org_name, truck_model_type, truck_model_type_name, dt;
"""
check_sql = "select org_id, truck_model_type_name, trans_finish_count, dt from tms_dws.dws_trans_org_truck_model_type_trans_finish_1d limit 5;"
create_and_load_dws_table(drop_sql, create_sql, insert_sql, check_sql)


# 6. 物流域转运站粒度派送成功1日汇总表（已成功）
drop_sql = "drop table if exists tms_dws.dws_trans_org_deliver_suc_1d;"
create_sql = f"""
create external table tms_dws.dws_trans_org_deliver_suc_1d(
    org_id bigint comment '转运站ID',
    org_name string comment '转运站名称',
    city_id string comment '城市ID（字符串类型）',
    city_name string comment '城市名称',
    province_id string comment '省份ID（字符串类型）',
    province_name string comment '省份名称',
    order_count bigint comment '派送成功次数（订单数）'
) comment '物流域转运站粒度派送成功 1 日汇总表'
    partitioned by (dt string comment '统计日期')
    stored as orc
    location 'hdfs://cdh01:8020/bigdata_warehouse/tms/tms_dws/dws_trans_org_deliver_suc_1d/'
    tblproperties('orc.compress'='snappy');
"""
insert_sql = f"""
insert overwrite table tms_dws.dws_trans_org_deliver_suc_1d
    partition (dt)
select org_id,
       org_name,
       city_id,
       city.name as city_name,
       province_id,
       province.name as province_name,
       count(order_id) as order_count,
       dt
from (select order_id,
             receiver_district_id,
             dt
      from tms_dwd.dwd_trans_deliver_suc_detail_inc
      where dt = '{date}'
      group by order_id, receiver_district_id, dt) detail
         left join
     (select id org_id, org_name, region_id district_id
      from tms_dim.dim_organ_full
      where dt = '{date}') organ
     on detail.receiver_district_id = organ.district_id
         left join
     (select id, parent_id as city_id
      from tms_dim.dim_region_full
      where dt = '{date}') district
     on organ.district_id = district.id
         left join
     (select id, name, parent_id as province_id
      from tms_dim.dim_region_full
      where dt = '{date}') city
     on district.city_id = city.id
         left join
     (select id, name
      from tms_dim.dim_region_full
      where dt = '{date}') province
     on city.province_id = province.id
group by org_id, org_name, city_id, city.name, province_id, province.name, dt;
"""
check_sql = "select org_id, city_name, order_count, dt from tms_dws.dws_trans_org_deliver_suc_1d limit 5;"
create_and_load_dws_table(drop_sql, create_sql, insert_sql, check_sql)


# 7. 物流域机构粒度分拣1日汇总表（已成功）
drop_sql = "drop table if exists tms_dws.dws_trans_org_sort_1d;"
create_sql = f"""
create external table tms_dws.dws_trans_org_sort_1d(
    org_id bigint comment '机构ID',
    org_name string comment '机构名称',
    city_id string comment '城市ID（字符串类型）',
    city_name string comment '城市名称',
    province_id string comment '省份ID（字符串类型）',
    province_name string comment '省份名称',
    sort_count bigint comment '分拣次数'
) comment '物流域机构粒度分拣 1 日汇总表'
    partitioned by (dt string comment '统计日期')
    stored as orc
    location 'hdfs://cdh01:8020/bigdata_warehouse/tms/tms_dws/dws_trans_org_sort_1d/'
    tblproperties('orc.compress'='snappy');
"""
insert_sql = f"""
insert overwrite table tms_dws.dws_trans_org_sort_1d
    partition (dt)
select org_id,
       org_name,
       if(org_level = 1, city_for_level1.id, province_for_level1.id) as city_id,
       if(org_level = 1, city_for_level1.name, province_for_level1.name) as city_name,
       if(org_level = 1, province_for_level1.id, province_for_level2.id) as province_id,
       if(org_level = 1, province_for_level1.name, province_for_level2.name) as province_name,
       sort_count,
       dt
from (select org_id,
             count(*) as sort_count,
             dt
      from tms_dwd.dwd_bound_sort_inc
      where dt = '{date}'
      group by org_id, dt) agg
         left join
     (select id, org_name, org_level, region_id
      from tms_dim.dim_organ_full
      where dt = '{date}') org
     on org_id = org.id
         left join
     (select id, name, parent_id
      from tms_dim.dim_region_full
      where dt = '{date}') city_for_level1
     on org.region_id = city_for_level1.id
         left join
     (select id, name, parent_id
      from tms_dim.dim_region_full
      where dt = '{date}') province_for_level1
     on city_for_level1.parent_id = province_for_level1.id
         left join
     (select id, name, parent_id
      from tms_dim.dim_region_full
      where dt = '{date}') province_for_level2
     on province_for_level1.parent_id = province_for_level2.id;
"""
check_sql = "select org_id, city_name, sort_count, dt from tms_dws.dws_trans_org_sort_1d limit 5;"
create_and_load_dws_table(drop_sql, create_sql, insert_sql, check_sql)


# 8. 交易域机构货物类型粒度下单n日汇总表（修复recent_days解析错误）
drop_sql = "drop table if exists tms_dws.dws_trade_org_cargo_type_order_nd;"
create_sql = f"""
create external table tms_dws.dws_trade_org_cargo_type_order_nd(
    org_id bigint comment '机构ID',
    org_name string comment '转运站名称',
    city_id string comment '城市ID（字符串类型）',
    city_name string comment '城市名称',
    cargo_type string comment '货物类型',
    cargo_type_name string comment '货物类型名称',
    recent_days tinyint comment '最近天数',
    order_count bigint comment '下单数',
    order_amount decimal(16,2) comment '下单金额'
) comment '交易域机构货物类型粒度下单 n 日汇总表'
    partitioned by(dt string comment '统计日期')
    stored as orc
    location 'hdfs://cdh01:8020/bigdata_warehouse/tms/tms_dws/dws_trade_org_cargo_type_order_nd'
    tblproperties('orc.compress' = 'snappy');
"""
insert_sql = f"""
insert overwrite table tms_dws.dws_trade_org_cargo_type_order_nd
    partition (dt = '{date}')
select org_id,
       org_name,
       city_id,
       city_name,
       cargo_type,
       cargo_type_name,
       recent_days,
       sum(order_count) as order_count,
       sum(order_amount) as order_amount
from (
    select t.*, tmp.recent_days
    from tms_dws.dws_trade_org_cargo_type_order_1d t
    lateral view explode(array(7, 30)) tmp as recent_days
    where t.dt >= date_sub('{date}', tmp.recent_days - 1)
      and t.dt <= '{date}'
) t
group by org_id, org_name, city_id, city_name, cargo_type, cargo_type_name, recent_days;
"""
check_sql = "select org_id, recent_days, order_count, dt from tms_dws.dws_trade_org_cargo_type_order_nd limit 5;"
create_and_load_dws_table(drop_sql, create_sql, insert_sql, check_sql)


# 9. 物流域转运站粒度揽收n日汇总表
drop_sql = "drop table if exists tms_dws.dws_trans_org_receive_nd;"
create_sql = f"""
create external table tms_dws.dws_trans_org_receive_nd(
    org_id bigint comment '转运站ID',
    org_name string comment '转运站名称',
    city_id string comment '城市ID（字符串类型）',
    city_name string comment '城市名称',
    province_id string comment '省份ID（字符串类型）',
    province_name string comment '省份名称',
    recent_days tinyint comment '最近天数',
    order_count bigint comment '揽收次数',
    order_amount decimal(16, 2) comment '揽收金额'
) comment '物流域转运站粒度揽收 n 日汇总表'
    partitioned by (dt string comment '统计日期')
    stored as orc
    location 'hdfs://cdh01:8020/bigdata_warehouse/tms/tms_dws/dws_trans_org_receive_nd/'
    tblproperties ('orc.compress'='snappy');
"""
insert_sql = f"""
insert overwrite table tms_dws.dws_trans_org_receive_nd
    partition (dt = '{date}')
select org_id,
       org_name,
       city_id,
       city_name,
       province_id,
       province_name,
       recent_days,
       sum(order_count) as order_count,
       sum(order_amount) as order_amount
from (
    select t.*, tmp.recent_days
    from tms_dws.dws_trans_org_receive_1d t
    lateral view explode(array(7, 30)) tmp as recent_days
    where t.dt >= date_sub('{date}', tmp.recent_days - 1)
      and t.dt <= '{date}'
) t
group by org_id, org_name, city_id, city_name, province_id, province_name, recent_days;
"""
check_sql = "select org_id, recent_days, order_count, dt from tms_dws.dws_trans_org_receive_nd limit 5;"
create_and_load_dws_table(drop_sql, create_sql, insert_sql, check_sql)


# 10. 物流域发单n日汇总表
drop_sql = "drop table if exists tms_dws.dws_trans_dispatch_nd;"
create_sql = f"""
create external table tms_dws.dws_trans_dispatch_nd(
    recent_days tinyint comment '最近天数',
    order_count bigint comment '发单总数',
    order_amount decimal(16,2) comment '发单总金额'
) comment '物流域发单 n 日汇总表'
    partitioned by (dt string comment '统计日期')
    stored as orc
    location 'hdfs://cdh01:8020/bigdata_warehouse/tms/tms_dws/dws_trans_dispatch_nd/'
    tblproperties('orc.compress'='snappy');
"""
insert_sql = f"""
insert overwrite table tms_dws.dws_trans_dispatch_nd
    partition (dt = '{date}')
select recent_days,
       sum(order_count) as order_count,
       sum(order_amount) as order_amount
from (
    select t.*, tmp.recent_days
    from tms_dws.dws_trans_dispatch_1d t
    lateral view explode(array(7, 30)) tmp as recent_days
    where t.dt >= date_sub('{date}', tmp.recent_days - 1)
      and t.dt <= '{date}'
) t
group by recent_days;
"""
check_sql = "select recent_days, order_count, dt from tms_dws.dws_trans_dispatch_nd limit 5;"
create_and_load_dws_table(drop_sql, create_sql, insert_sql, check_sql)


# 11. 物流域班次粒度转运完成n日汇总表（修复lateral view位置错误）
drop_sql = "drop table if exists tms_dws.dws_trans_shift_trans_finish_nd;"
create_sql = f"""
create external table tms_dws.dws_trans_shift_trans_finish_nd(
    shift_id bigint comment '班次ID',
    city_id string comment '城市ID',
    city_name string comment '城市名称',
    org_id bigint comment '机构ID',
    org_name string comment '机构名称',
    line_id bigint comment '线路ID',
    line_name string comment '线路名称',
    driver1_emp_id bigint comment '第一司机员工ID',
    driver1_name string comment '第一司机姓名',
    driver2_emp_id bigint comment '第二司机员工ID',
    driver2_name string comment '第二司机姓名',
    truck_model_type string comment '卡车类别编码',
    truck_model_type_name string comment '卡车类别名称',
    recent_days tinyint comment '最近天数',
    trans_finish_count bigint comment '转运完成次数',
    trans_finish_distance decimal(16,2) comment '转运完成里程',
    trans_finish_dur_sec bigint comment '转运完成时长（秒）',
    trans_finish_order_count bigint comment '转运完成运单数'
) comment '物流域班次粒度转运完成最近 n 日汇总表'
    partitioned by (dt string comment '统计日期')
    stored as orc
    location 'hdfs://cdh01:8020/bigdata_warehouse/tms/tms_dws/dws_trans_shift_trans_finish_nd/'
    tblproperties('orc.compress'='snappy');
"""
insert_sql = f"""
insert overwrite table tms_dws.dws_trans_shift_trans_finish_nd
    partition (dt = '{date}')
select shift_id,
       if(org_level = 1, first.region_id, city.id) as city_id,
       if(org_level = 1, first.region_name, city.name) as city_name,
       org_id,
       org_name,
       line_id,
       line_name,
       driver1_emp_id,
       driver1_name,
       driver2_emp_id,
       driver2_name,
       truck_model_type,
       truck_model_type_name,
       recent_days,
       trans_finish_count,
       trans_finish_distance,
       trans_finish_dur_sec,
       trans_finish_order_count
from (
    -- 先通过子查询生成recent_days，再用于过滤和分组
    select recent_days,
           shift_id,
           line_id,
           truck_id,
           start_org_id org_id,
           start_org_name org_name,
           driver1_emp_id,
           driver1_name,
           driver2_emp_id,
           driver2_name,
           count(id) as trans_finish_count,
           sum(actual_distance) as trans_finish_distance,
           sum(finish_dur_sec) as trans_finish_dur_sec,
           sum(order_num) as trans_finish_order_count
    from (
        select *, tmp.recent_days
        from tms_dwd.dwd_trans_trans_finish_inc
        lateral view explode(array(7, 30)) tmp as recent_days
        where dt <= '{date}'  -- 先限定上限，下限在外部过滤
    ) detail
    where dt >= date_sub('{date}', detail.recent_days - 1)
    group by recent_days, shift_id, line_id, start_org_id, start_org_name, driver1_emp_id, driver1_name, driver2_emp_id, driver2_name, truck_id
) aggregated
left join (
    select id, org_level, region_id, region_name
    from tms_dim.dim_organ_full
    where dt = '{date}'
) first on aggregated.org_id = first.id
left join (
    select id, parent_id
    from tms_dim.dim_region_full
    where dt = '{date}'
) parent on first.region_id = parent.id
left join (
    select id, name
    from tms_dim.dim_region_full
    where dt = '{date}'
) city on parent.parent_id = city.id
left join (
    select id, line_name
    from tms_dim.dim_shift_full
    where dt = '{date}'
) for_line_name on aggregated.shift_id = for_line_name.id
left join (
    select id, truck_model_type, truck_model_type_name
    from tms_dim.dim_truck_full
    where dt = '{date}'
) truck_info on aggregated.truck_id = truck_info.id;
"""
check_sql = "select shift_id, recent_days, trans_finish_count, dt from tms_dws.dws_trans_shift_trans_finish_nd limit 5;"
create_and_load_dws_table(drop_sql, create_sql, insert_sql, check_sql)


# 12. 物流域转运站粒度派送成功n日汇总表
drop_sql = "drop table if exists tms_dws.dws_trans_org_deliver_suc_nd;"
create_sql = f"""
create external table tms_dws.dws_trans_org_deliver_suc_nd(
    org_id bigint comment '转运站ID',
    org_name string comment '转运站名称',
    city_id string comment '城市ID',
    city_name string comment '城市名称',
    province_id string comment '省份ID',
    province_name string comment '省份名称',
    recent_days tinyint comment '最近天数',
    order_count bigint comment '派送成功次数（订单数）'
) comment '物流域转运站粒度派送成功 n 日汇总表'
    partitioned by (dt string comment '统计日期')
    stored as orc
    location 'hdfs://cdh01:8020/bigdata_warehouse/tms/tms_dws/dws_trans_org_deliver_suc_nd/'
    tblproperties('orc.compress'='snappy');
"""
insert_sql = f"""
insert overwrite table tms_dws.dws_trans_org_deliver_suc_nd
    partition (dt = '{date}')
select org_id,
       org_name,
       city_id,
       city_name,
       province_id,
       province_name,
       recent_days,
       sum(order_count) as order_count
from (
    select t.*, tmp.recent_days
    from tms_dws.dws_trans_org_deliver_suc_1d t
    lateral view explode(array(7, 30)) tmp as recent_days
    where t.dt >= date_sub('{date}', tmp.recent_days - 1)
      and t.dt <= '{date}'
) t
group by org_id, org_name, city_id, city_name, province_id, province_name, recent_days;
"""
check_sql = "select org_id, recent_days, order_count, dt from tms_dws.dws_trans_org_deliver_suc_nd limit 5;"
create_and_load_dws_table(drop_sql, create_sql, insert_sql, check_sql)


# 13. 物流域机构粒度分拣n日汇总表
drop_sql = "drop table if exists tms_dws.dws_trans_org_sort_nd;"
create_sql = f"""
create external table tms_dws.dws_trans_org_sort_nd(
    org_id bigint comment '机构ID',
    org_name string comment '机构名称',
    city_id string comment '城市ID',
    city_name string comment '城市名称',
    province_id string comment '省份ID',
    province_name string comment '省份名称',
    recent_days tinyint comment '最近天数',
    sort_count bigint comment '分拣次数'
) comment '物流域机构粒度分拣 n 日汇总表'
    partitioned by (dt string comment '统计日期')
    stored as orc
    location 'hdfs://cdh01:8020/bigdata_warehouse/tms/tms_dws/dws_trans_org_sort_nd/'
    tblproperties('orc.compress'='snappy');
"""
insert_sql = f"""
insert overwrite table tms_dws.dws_trans_org_sort_nd
    partition (dt = '{date}')
select org_id,
       org_name,
       city_id,
       city_name,
       province_id,
       province_name,
       recent_days,
       sum(sort_count) as sort_count
from (
    select t.*, tmp.recent_days
    from tms_dws.dws_trans_org_sort_1d t
    lateral view explode(array(7, 30)) tmp as recent_days
    where t.dt >= date_sub('{date}', tmp.recent_days - 1)
      and t.dt <= '{date}'
) t
group by org_id, org_name, city_id, city_name, province_id, province_name, recent_days;
"""
check_sql = "select org_id, recent_days, sort_count, dt from tms_dws.dws_trans_org_sort_nd limit 5;"
create_and_load_dws_table(drop_sql, create_sql, insert_sql, check_sql)


# 14. 物流域发单历史至今汇总表
drop_sql = "drop table if exists tms_dws.dws_trans_dispatch_td;"
create_sql = f"""
create external table tms_dws.dws_trans_dispatch_td(
    order_count bigint comment '发单数',
    order_amount decimal(16,2) comment '发单金额'
) comment '物流域发单历史至今汇总表'
    partitioned by (dt string comment '统计日期')
    stored as orc
    location 'hdfs://cdh01:8020/bigdata_warehouse/tms/tms_dws/dws_trans_dispatch_td'
    tblproperties('orc.compress'='snappy');
"""
insert_sql = f"""
insert overwrite table tms_dws.dws_trans_dispatch_td
    partition (dt = '{date}')
select sum(order_count) as order_count,
       sum(order_amount) as order_amount
from tms_dws.dws_trans_dispatch_1d
where dt <= '{date}';
"""
check_sql = "select order_count, order_amount, dt from tms_dws.dws_trans_dispatch_td limit 5;"
create_and_load_dws_table(drop_sql, create_sql, insert_sql, check_sql)


# 15. 物流域转运完成历史至今汇总表
drop_sql = "drop table if exists tms_dws.dws_trans_bound_finish_td;"
create_sql = f"""
create external table tms_dws.dws_trans_bound_finish_td(
    order_count bigint comment '发单数',
    order_amount decimal(16,2) comment '发单金额'
) comment '物流域转运完成历史至今汇总表'
    partitioned by (dt string comment '统计日期')
    stored as orc
    location 'hdfs://cdh01:8020/bigdata_warehouse/tms/tms_dws/dws_trans_bound_finish_td'
    tblproperties('orc.compress'='snappy');
"""
insert_sql = f"""
insert overwrite table tms_dws.dws_trans_bound_finish_td
    partition (dt = '{date}')
select count(order_id) as order_count,
       sum(amount) as order_amount
from (select order_id,
             max(amount) as amount
      from tms_dwd.dwd_trans_bound_finish_detail_inc
      where dt <= '{date}'
      group by order_id) distinct_info;
"""
check_sql = "select order_count, order_amount, dt from tms_dws.dws_trans_bound_finish_td limit 5;"
create_and_load_dws_table(drop_sql, create_sql, insert_sql, check_sql)


# 停止SparkSession
spark.stop()
print("所有DWS层表处理完成")