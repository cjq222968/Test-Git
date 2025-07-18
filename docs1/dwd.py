from pyspark.sql import SparkSession
import os
import re
import shutil
import atexit
import time
from datetime import datetime  # 新增：用于获取当天日期

# 初始化SparkSession，集成Hive（优化配置）
spark = SparkSession.builder \
    .appName("TmsDwdIntegration") \
    .master("local[*]") \
    .config("spark.hadoop.hive.metastore.uris", "thrift://cdh01:9083") \
    .config("spark.sql.warehouse.dir", "/home/user/hive/warehouse") \
    .config("spark.hadoop.hive.support.quoted.identifiers", "none") \
    .config("spark.hadoop.hive.exec.dynamic.partition.mode", "nonstrict") \
    .config("spark.cleaner.referenceTracking.cleanCheckpoints", "false") \
    .config("spark.local.dir", "${spark.temp.dir}") \
    .enableHiveSupport() \
    .getOrCreate()

# 获取SparkContext并设置日志级别（仅显示错误）
sc = spark.sparkContext
sc.setLogLevel("ERROR")

# 定义日期变量：获取当天日期（格式：yyyy-MM-dd）
date = datetime.now().strftime("%Y-%m-%d")  # 核心修改：动态获取当天日期

# 自定义临时目录（使用脚本所在目录，避免系统临时目录权限问题）
script_dir = os.path.dirname(os.path.abspath(__file__))
temp_dir = os.path.join(script_dir, "spark-temp-dwd")
# 确保临时目录存在且有正确权限
os.makedirs(temp_dir, exist_ok=True)
os.chmod(temp_dir, 0o755)  # 设置目录权限
spark.conf.set("spark.local.dir", temp_dir)  # 强制覆盖临时目录配置

def create_and_load_dwd_table(drop_sql, create_sql, insert_sql, check_sql):
    """封装DWD层表处理通用逻辑：删除→创建→插入→验证"""
    try:
        # 提取表名
        table_match = re.search(r'create external table (\w+\.\w+)', create_sql, re.IGNORECASE)
        if not table_match:
            raise ValueError("无法提取表名")
        full_table_name = table_match.group(1)
        table_name = full_table_name.split('.')[1]

        # 1. 删除表（若存在）
        spark.sql(drop_sql)

        # 2. 创建表
        spark.sql(create_sql)

        # 3. 执行插入（统一设置动态分区模式）
        spark.sql("set hive.exec.dynamic.partition.mode=nonstrict;")
        spark.sql(insert_sql)
        print(f"表 {table_name} 数据插入完成")

        # 4. 验证数据
        spark.sql(check_sql).show(5)  # 显示前5条验证
        print(f"表处理成功: {table_name}\n")

    except Exception as e:
        print(f"表 {table_name} 处理失败: {str(e)}")
        clean_temp_dir()
        spark.stop()
        exit(1)

def clean_temp_dir(force=False):
    """
    清理临时目录，增加重试机制和强制删除选项
    :param force: 是否强制删除（忽略文件锁定）
    """
    if not os.path.exists(temp_dir):
        print(f"临时目录 {temp_dir} 不存在，无需清理")
        return

    max_attempts = 3
    delay_seconds = 1
    for attempt in range(max_attempts):
        try:
            # 先尝试正常删除
            shutil.rmtree(temp_dir, ignore_errors=not force)
            # 验证是否删除成功
            if not os.path.exists(temp_dir):
                print(f"临时目录 {temp_dir} 已清理")
                return
            # 若仍存在且不是最后一次尝试，则重试
            if attempt < max_attempts - 1:
                time.sleep(delay_seconds)
                delay_seconds *= 2  # 指数退避重试
        except Exception as e:
            print(f"清理临时目录尝试 {attempt + 1} 失败: {str(e)}")
            if attempt < max_attempts - 1:
                time.sleep(delay_seconds)
                delay_seconds *= 2

    # 最后尝试强制删除（针对Windows文件锁定问题）
    if os.name == 'nt':  # 仅在Windows系统执行
        try:
            import ctypes
            # 使用Windows API强制删除目录
            ctypes.windll.kernel32.SetFileAttributesW(temp_dir, 0x80)  # 取消只读属性
            shutil.rmtree(temp_dir, ignore_errors=True)
            print(f"已强制清理临时目录 {temp_dir}")
        except Exception as e:
            print(f"最终清理临时目录失败: {str(e)}")

# 注册退出钩子，确保程序退出时清理临时目录
atexit.register(clean_temp_dir, force=True)

# 1. 创建dwd数据库（存在则跳过）
spark.sql(f"""
create database IF NOT EXISTS tms_dwd
    LOCATION 'hdfs://cdh01:8020/bigdata_warehouse/tms/tms_dwd/';
""")

# 2. 切换到tms_ods数据库（数据源所在库）
spark.sql("use tms_ods;")

# 3. 交易域订单明细事务事实表
drop_order_detail = "drop table if exists tms_dwd.dwd_trade_order_detail_inc;"
create_order_detail = f"""
create external table tms_dwd.dwd_trade_order_detail_inc(
    id bigint comment '运单明细ID',
    order_id string COMMENT '运单ID',
    cargo_type string COMMENT '货物类型ID',
    cargo_type_name string COMMENT '货物类型名称',
    volumn_length bigint COMMENT '长cm',
    volumn_width bigint COMMENT '宽cm',
    volumn_height bigint COMMENT '高cm',
    weight decimal(16,2) COMMENT '重量 kg',
    order_time string COMMENT '下单时间',
    order_no string COMMENT '运单号',
    status string COMMENT '运单状态',
    status_name string COMMENT '运单状态名称',
    collect_type string COMMENT '取件类型，1为网点自寄，2为上门取件',
    collect_type_name string COMMENT '取件类型名称',
    user_id bigint COMMENT '用户ID',
    receiver_complex_id bigint COMMENT '收件人小区id',
    receiver_province_id string COMMENT '收件人省份id',
    receiver_city_id string COMMENT '收件人城市id',
    receiver_district_id string COMMENT '收件人区县id',
    receiver_name string COMMENT '收件人姓名',
    sender_complex_id bigint COMMENT '发件人小区id',
    sender_province_id string COMMENT '发件人省份id',
    sender_city_id string COMMENT '发件人城市id',
    sender_district_id string COMMENT '发件人区县id',
    sender_name string COMMENT '发件人姓名',
    cargo_num bigint COMMENT '货物个数',
    amount decimal(16,2) COMMENT '金额',
    estimate_arrive_time string COMMENT '预计到达时间',
    distance decimal(16,2) COMMENT '距离，单位：公里'
) comment '交易域订单明细事务事实表'
    partitioned by (dt string comment '统计日期')
    stored as orc
    location 'hdfs://cdh01:8020/bigdata_warehouse/tms/tms_dwd/dwd_trade_order_detail_inc'
    tblproperties('orc.compress' = 'snappy');
"""
insert_order_detail = f"""
insert overwrite table tms_dwd.dwd_trade_order_detail_inc
    partition (dt)
select cargo.id,
       order_id,
       cargo_type,
       dic_for_cargo_type.name               cargo_type_name,
       volume_length,
       volume_width,
       volume_height,
       weight,
       order_time,
       order_no,
       status,
       dic_for_status.name                   status_name,
       collect_type,
       dic_for_collect_type.name             collect_type_name,
       user_id,
       receiver_complex_id,
       receiver_province_id,
       receiver_city_id,
       receiver_district_id,
       receiver_name,
       sender_complex_id,
       sender_province_id,
       sender_city_id,
       sender_district_id,
       sender_name,
       cargo_num,
       amount,
       date_format(from_utc_timestamp(
                           to_timestamp(cast(info.estimate_arrive_time as bigint) * 1000), 'UTC'),
                   'yyyy-MM-dd HH:mm:ss')    estimate_arrive_time,
       distance,
       '{date}' dt  -- 核心修改：固定为当天日期
from (select after.id,
             after.order_id,
             after.cargo_type,
             after.volume_length,
             after.volume_width,
             after.volume_height,
             after.weight,
             concat(substr(after.create_time, 1, 10), ' ', substr(after.create_time, 12, 8)) order_time
      from ods_order_cargo after
      where dt = '{date}'  -- 数据源过滤条件也使用当天日期
        and after.is_deleted = '0') cargo
         join
     (select after.id,
             after.order_no,
             after.status,
             after.collect_type,
             after.user_id,
             after.receiver_complex_id,
             after.receiver_province_id,
             after.receiver_city_id,
             after.receiver_district_id,
             concat(substr(after.receiver_name, 1, 1), '*') receiver_name,
             after.sender_complex_id,
             after.sender_province_id,
             after.sender_city_id,
             after.sender_district_id,
             concat(substr(after.sender_name, 1, 1), '*')   sender_name,
             after.cargo_num,
             after.amount,
             after.estimate_arrive_time,
             after.distance
      from ods_order_info after
      where dt = '{date}'  -- 数据源过滤条件也使用当天日期
        and after.is_deleted = '0') info
     on cargo.order_id = info.id
         left join
     (select id, name from ods_base_dic where dt = '{date}' and is_deleted = '0') dic_for_cargo_type
     on cargo.cargo_type = cast(dic_for_cargo_type.id as string)
         left join
     (select id, name from ods_base_dic where dt = '{date}' and is_deleted = '0') dic_for_status
     on info.status = cast(dic_for_status.id as string)
         left join
     (select id, name from ods_base_dic where dt = '{date}' and is_deleted = '0') dic_for_collect_type
     on info.collect_type = cast(dic_for_collect_type.id as string);
"""
check_order_detail = "select id, order_id, order_no, dt from tms_dwd.dwd_trade_order_detail_inc limit 5;"

create_and_load_dwd_table(drop_order_detail, create_order_detail, insert_order_detail, check_order_detail)


# 4. 交易域支付成功事务事实表
drop_pay_suc = "drop table if exists tms_dwd.dwd_trade_pay_suc_detail_inc;"
create_pay_suc = f"""
create external table tms_dwd.dwd_trade_pay_suc_detail_inc(
    id bigint comment '运单明细ID',
    order_id string COMMENT '运单ID',
    cargo_type string COMMENT '货物类型ID',
    cargo_type_name string COMMENT '货物类型名称',
    volumn_length bigint COMMENT '长cm',
    volumn_width bigint COMMENT '宽cm',
    volumn_height bigint COMMENT '高cm',
    weight decimal(16,2) COMMENT '重量 kg',
    payment_time string COMMENT '支付时间',
    order_no string COMMENT '运单号',
    status string COMMENT '运单状态',
    status_name string COMMENT '运单状态名称',
    collect_type string COMMENT '取件类型',
    collect_type_name string COMMENT '取件类型名称',
    user_id bigint COMMENT '用户ID',
    receiver_complex_id bigint COMMENT '收件人小区id',
    receiver_province_id string COMMENT '收件人省份id',
    receiver_city_id string COMMENT '收件人城市id',
    receiver_district_id string COMMENT '收件人区县id',
    receiver_name string COMMENT '收件人姓名',
    sender_complex_id bigint COMMENT '发件人小区id',
    sender_province_id string COMMENT '发件人省份id',
    sender_city_id string COMMENT '发件人城市id',
    sender_district_id string COMMENT '发件人区县id',
    sender_name string COMMENT '发件人姓名',
    payment_type string COMMENT '支付方式',
    payment_type_name string COMMENT '支付方式名称',
    cargo_num bigint COMMENT '货物个数',
    amount decimal(16,2) COMMENT '金额',
    estimate_arrive_time string COMMENT '预计到达时间',
    distance decimal(16,2) COMMENT '距离'
) comment '交易域支付成功事务事实表'
    partitioned by (dt string comment '统计日期')
    stored as orc
    location 'hdfs://cdh01:8020/bigdata_warehouse/tms/tms_dwd/dwd_trade_pay_suc_detail_inc'
    tblproperties('orc.compress' = 'snappy');
"""
insert_pay_suc = f"""
insert overwrite table tms_dwd.dwd_trade_pay_suc_detail_inc
    partition (dt)
select cargo.id,
       order_id,
       cargo_type,
       dic_for_cargo_type.name               cargo_type_name,
       volume_length,
       volume_width,
       volume_height,
       weight,
       payment_time,
       order_no,
       status,
       dic_for_status.name                   status_name,
       collect_type,
       dic_for_collect_type.name             collect_type_name,
       user_id,
       receiver_complex_id,
       receiver_province_id,
       receiver_city_id,
       receiver_district_id,
       receiver_name,
       sender_complex_id,
       sender_province_id,
       sender_city_id,
       sender_district_id,
       sender_name,
       payment_type,
       dic_for_payment_type.name             payment_type_name,
       cargo_num,
       amount,
       date_format(from_utc_timestamp(
                           to_timestamp(cast(info.estimate_arrive_time as bigint) * 1000), 'UTC'),
                   'yyyy-MM-dd HH:mm:ss')    estimate_arrive_time,
       distance,
       '{date}' dt  -- 核心修改：固定为当天日期
from (select after.id,
             after.order_id,
             after.cargo_type,
             after.volume_length,
             after.volume_width,
             after.volume_height,
             after.weight
      from ods_order_cargo after
      where dt = '{date}'  -- 数据源过滤条件也使用当天日期
        and after.is_deleted = '0') cargo
         join
     (select after.id,
             after.order_no,
             after.status,
             after.collect_type,
             after.user_id,
             after.receiver_complex_id,
             after.receiver_province_id,
             after.receiver_city_id,
             after.receiver_district_id,
             concat(substr(after.receiver_name, 1, 1), '*') receiver_name,
             after.sender_complex_id,
             after.sender_province_id,
             after.sender_city_id,
             after.sender_district_id,
             concat(substr(after.sender_name, 1, 1), '*')   sender_name,
             after.payment_type,
             after.cargo_num,
             after.amount,
             after.estimate_arrive_time,
             after.distance,
             concat(substr(after.update_time, 1, 10), ' ', substr(after.update_time, 12, 8)) payment_time
      from ods_order_info after
      where dt = '{date}'  -- 数据源过滤条件也使用当天日期
        and after.is_deleted = '0'
        and after.status not in ('60010', '60999')) info
     on cargo.order_id = info.id
         left join
     (select id, name from ods_base_dic where dt = '{date}' and is_deleted = '0') dic_for_cargo_type
     on cargo.cargo_type = cast(dic_for_cargo_type.id as string)
         left join
     (select id, name from ods_base_dic where dt = '{date}' and is_deleted = '0') dic_for_status
     on info.status = cast(dic_for_status.id as string)
         left join
     (select id, name from ods_base_dic where dt = '{date}' and is_deleted = '0') dic_for_collect_type
     on info.collect_type = cast(dic_for_collect_type.id as string)
         left join
     (select id, name from ods_base_dic where dt = '{date}' and is_deleted = '0') dic_for_payment_type
     on info.payment_type = cast(dic_for_payment_type.id as string);
"""
check_pay_suc = "select id, order_id, payment_time, dt from tms_dwd.dwd_trade_pay_suc_detail_inc limit 5;"

create_and_load_dwd_table(drop_pay_suc, create_pay_suc, insert_pay_suc, check_pay_suc)


# 5. 交易域取消运单事务事实表
drop_order_cancel = "drop table if exists tms_dwd.dwd_trade_order_cancel_detail_inc;"
create_order_cancel = f"""
create external table tms_dwd.dwd_trade_order_cancel_detail_inc(
    id bigint comment '运单明细ID',
    order_id string COMMENT '运单ID',
    cargo_type string COMMENT '货物类型ID',
    cargo_type_name string COMMENT '货物类型名称',
    volumn_length bigint COMMENT '长cm',
    volumn_width bigint COMMENT '宽cm',
    volumn_height bigint COMMENT '高cm',
    weight decimal(16,2) COMMENT '重量 kg',
    cancel_time string COMMENT '取消时间',
    order_no string COMMENT '运单号',
    status string COMMENT '运单状态',
    status_name string COMMENT '运单状态名称',
    collect_type string COMMENT '取件类型',
    collect_type_name string COMMENT '取件类型名称',
    user_id bigint COMMENT '用户ID',
    receiver_complex_id bigint COMMENT '收件人小区id',
    receiver_province_id string COMMENT '收件人省份id',
    receiver_city_id string COMMENT '收件人城市id',
    receiver_district_id string COMMENT '收件人区县id',
    receiver_name string COMMENT '收件人姓名',
    sender_complex_id bigint COMMENT '发件人小区id',
    sender_province_id string COMMENT '发件人省份id',
    sender_city_id string COMMENT '发件人城市id',
    sender_district_id string COMMENT '发件人区县id',
    sender_name string COMMENT '发件人姓名',
    cargo_num bigint COMMENT '货物个数',
    amount decimal(16,2) COMMENT '金额',
    estimate_arrive_time string COMMENT '预计到达时间',
    distance decimal(16,2) COMMENT '距离'
) comment '交易域取消运单事务事实表'
    partitioned by (dt string comment '统计日期')
    stored as orc
    location 'hdfs://cdh01:8020/bigdata_warehouse/tms/tms_dwd/dwd_trade_order_cancel_detail_inc'
    tblproperties('orc.compress' = 'snappy');
"""
insert_order_cancel = f"""
insert overwrite table tms_dwd.dwd_trade_order_cancel_detail_inc
    partition (dt)
select cargo.id,
       order_id,
       cargo_type,
       dic_for_cargo_type.name               cargo_type_name,
       volume_length,
       volume_width,
       volume_height,
       weight,
       cancel_time,
       order_no,
       status,
       dic_for_status.name                   status_name,
       collect_type,
       dic_for_collect_type.name             collect_type_name,
       user_id,
       receiver_complex_id,
       receiver_province_id,
       receiver_city_id,
       receiver_district_id,
       receiver_name,
       sender_complex_id,
       sender_province_id,
       sender_city_id,
       sender_district_id,
       sender_name,
       cargo_num,
       amount,
       date_format(from_utc_timestamp(
                           to_timestamp(cast(info.estimate_arrive_time as bigint) * 1000), 'UTC'),
                   'yyyy-MM-dd HH:mm:ss')    estimate_arrive_time,
       distance,
       '{date}' dt  -- 核心修改：固定为当天日期
from (select after.id,
             after.order_id,
             after.cargo_type,
             after.volume_length,
             after.volume_width,
             after.volume_height,
             after.weight
      from ods_order_cargo after
      where dt = '{date}'  -- 数据源过滤条件也使用当天日期
        and after.is_deleted = '0') cargo
         join
     (select after.id,
             after.order_no,
             after.status,
             after.collect_type,
             after.user_id,
             after.receiver_complex_id,
             after.receiver_province_id,
             after.receiver_city_id,
             after.receiver_district_id,
             concat(substr(after.receiver_name, 1, 1), '*') receiver_name,
             after.sender_complex_id,
             after.sender_province_id,
             after.sender_city_id,
             after.sender_district_id,
             concat(substr(after.sender_name, 1, 1), '*')   sender_name,
             after.cargo_num,
             after.amount,
             after.estimate_arrive_time,
             after.distance,
             concat(substr(after.update_time, 1, 10), ' ', substr(after.update_time, 12, 8)) cancel_time
      from ods_order_info after
      where dt = '{date}'  -- 数据源过滤条件也使用当天日期
        and after.is_deleted = '0'
        and after.status = '60020') info
     on cargo.order_id = info.id
         left join
     (select id, name from ods_base_dic where dt = '{date}' and is_deleted = '0') dic_for_cargo_type
     on cargo.cargo_type = cast(dic_for_cargo_type.id as string)
         left join
     (select id, name from ods_base_dic where dt = '{date}' and is_deleted = '0') dic_for_status
     on info.status = cast(dic_for_status.id as string)
         left join
     (select id, name from ods_base_dic where dt = '{date}' and is_deleted = '0') dic_for_collect_type
     on info.collect_type = cast(dic_for_collect_type.id as string);
"""
check_order_cancel = "select id, order_id, cancel_time, dt from tms_dwd.dwd_trade_order_cancel_detail_inc limit 5;"

create_and_load_dwd_table(drop_order_cancel, create_order_cancel, insert_order_cancel, check_order_cancel)


# 6. 物流域揽收事务事实表
drop_trans_receive = "drop table if exists tms_dwd.dwd_trans_receive_detail_inc;"
create_trans_receive = f"""
create external table tms_dwd.dwd_trans_receive_detail_inc(
    id bigint comment '运单明细ID',
    order_id string COMMENT '运单ID',
    cargo_type string COMMENT '货物类型ID',
    cargo_type_name string COMMENT '货物类型名称',
    volumn_length bigint COMMENT '长cm',
    volumn_width bigint COMMENT '宽cm',
    volumn_height bigint COMMENT '高cm',
    weight decimal(16,2) COMMENT '重量 kg',
    receive_time string COMMENT '揽收时间',
    order_no string COMMENT '运单号',
    status string COMMENT '运单状态',
    status_name string COMMENT '运单状态名称',
    collect_type string COMMENT '取件类型',
    collect_type_name string COMMENT '取件类型名称',
    user_id bigint COMMENT '用户ID',
    receiver_complex_id bigint COMMENT '收件人小区id',
    receiver_province_id string COMMENT '收件人省份id',
    receiver_city_id string COMMENT '收件人城市id',
    receiver_district_id string COMMENT '收件人区县id',
    receiver_name string COMMENT '收件人姓名',
    sender_complex_id bigint COMMENT '发件人小区id',
    sender_province_id string COMMENT '发件人省份id',
    sender_city_id string COMMENT '发件人城市id',
    sender_district_id string COMMENT '发件人区县id',
    sender_name string COMMENT '发件人姓名',
    payment_type string COMMENT '支付方式',
    payment_type_name string COMMENT '支付方式名称',
    cargo_num bigint COMMENT '货物个数',
    amount decimal(16,2) COMMENT '金额',
    estimate_arrive_time string COMMENT '预计到达时间',
    distance decimal(16,2) COMMENT '距离'
) comment '物流域揽收事务事实表'
    partitioned by (dt string comment '统计日期')
    stored as orc
    location 'hdfs://cdh01:8020/bigdata_warehouse/tms/tms_dwd/dwd_trans_receive_detail_inc'
    tblproperties('orc.compress' = 'snappy');
"""
insert_trans_receive = f"""
insert overwrite table tms_dwd.dwd_trans_receive_detail_inc
    partition (dt)
select cargo.id,
       order_id,
       cargo_type,
       dic_for_cargo_type.name               cargo_type_name,
       volume_length,
       volume_width,
       volume_height,
       weight,
       receive_time,
       order_no,
       status,
       dic_for_status.name                   status_name,
       collect_type,
       dic_for_collect_type.name             collect_type_name,
       user_id,
       receiver_complex_id,
       receiver_province_id,
       receiver_city_id,
       receiver_district_id,
       receiver_name,
       sender_complex_id,
       sender_province_id,
       sender_city_id,
       sender_district_id,
       sender_name,
       payment_type,
       dic_for_payment_type.name             payment_type_name,
       cargo_num,
       amount,
       date_format(from_utc_timestamp(
                           to_timestamp(cast(info.estimate_arrive_time as bigint) * 1000), 'UTC'),
                   'yyyy-MM-dd HH:mm:ss')    estimate_arrive_time,
       distance,
       '{date}' dt  -- 核心修改：固定为当天日期
from (select after.id,
             after.order_id,
             after.cargo_type,
             after.volume_length,
             after.volume_width,
             after.volume_height,
             after.weight
      from ods_order_cargo after
      where dt = '{date}'  -- 数据源过滤条件也使用当天日期
        and after.is_deleted = '0') cargo
         join
     (select after.id,
             after.order_no,
             after.status,
             after.collect_type,
             after.user_id,
             after.receiver_complex_id,
             after.receiver_province_id,
             after.receiver_city_id,
             after.receiver_district_id,
             concat(substr(after.receiver_name, 1, 1), '*') receiver_name,
             after.sender_complex_id,
             after.sender_province_id,
             after.sender_city_id,
             after.sender_district_id,
             concat(substr(after.sender_name, 1, 1), '*')   sender_name,
             after.payment_type,
             after.cargo_num,
             after.amount,
             after.estimate_arrive_time,
             after.distance,
             concat(substr(after.update_time, 1, 10), ' ', substr(after.update_time, 12, 8)) receive_time
      from ods_order_info after
      where dt = '{date}'  -- 数据源过滤条件也使用当天日期
        and after.is_deleted = '0'
        and after.status not in ('60010', '60020', '60999')) info
     on cargo.order_id = info.id
         left join
     (select id, name from ods_base_dic where dt = '{date}' and is_deleted = '0') dic_for_cargo_type
     on cargo.cargo_type = cast(dic_for_cargo_type.id as string)
         left join
     (select id, name from ods_base_dic where dt = '{date}' and is_deleted = '0') dic_for_status
     on info.status = cast(dic_for_status.id as string)
         left join
     (select id, name from ods_base_dic where dt = '{date}' and is_deleted = '0') dic_for_collect_type
     on info.collect_type = cast(dic_for_collect_type.id as string)
         left join
     (select id, name from ods_base_dic where dt = '{date}' and is_deleted = '0') dic_for_payment_type
     on info.payment_type = cast(dic_for_payment_type.id as string);
"""
check_trans_receive = "select id, order_id, receive_time, dt from tms_dwd.dwd_trans_receive_detail_inc limit 5;"

create_and_load_dwd_table(drop_trans_receive, create_trans_receive, insert_trans_receive, check_trans_receive)


# 7. 物流域发单事务事实表
drop_trans_dispatch = "drop table if exists tms_dwd.dwd_trans_dispatch_detail_inc;"
create_trans_dispatch = f"""
create external table tms_dwd.dwd_trans_dispatch_detail_inc(
    id bigint comment '运单明细ID',
    order_id string COMMENT '运单ID',
    cargo_type string COMMENT '货物类型ID',
    cargo_type_name string COMMENT '货物类型名称',
    volumn_length bigint COMMENT '长cm',
    volumn_width bigint COMMENT '宽cm',
    volumn_height bigint COMMENT '高cm',
    weight decimal(16,2) COMMENT '重量 kg',
    dispatch_time string COMMENT '发单时间',
    order_no string COMMENT '运单号',
    status string COMMENT '运单状态',
    status_name string COMMENT '运单状态名称',
    collect_type string COMMENT '取件类型',
    collect_type_name string COMMENT '取件类型名称',
    user_id bigint COMMENT '用户ID',
    receiver_complex_id bigint COMMENT '收件人小区id',
    receiver_province_id string COMMENT '收件人省份id',
    receiver_city_id string COMMENT '收件人城市id',
    receiver_district_id string COMMENT '收件人区县id',
    receiver_name string COMMENT '收件人姓名',
    sender_complex_id bigint COMMENT '发件人小区id',
    sender_province_id string COMMENT '发件人省份id',
    sender_city_id string COMMENT '发件人城市id',
    sender_district_id string COMMENT '发件人区县id',
    sender_name string COMMENT '发件人姓名',
    payment_type string COMMENT '支付方式',
    payment_type_name string COMMENT '支付方式名称',
    cargo_num bigint COMMENT '货物个数',
    amount decimal(16,2) COMMENT '金额',
    estimate_arrive_time string COMMENT '预计到达时间',
    distance decimal(16,2) COMMENT '距离'
) comment '物流域发单事务事实表'
    partitioned by (dt string comment '统计日期')
    stored as orc
    location 'hdfs://cdh01:8020/bigdata_warehouse/tms/tms_dwd/dwd_trans_dispatch_detail_inc'
    tblproperties('orc.compress' = 'snappy');
"""
insert_trans_dispatch = f"""
insert overwrite table tms_dwd.dwd_trans_dispatch_detail_inc
    partition (dt)
select cargo.id,
       order_id,
       cargo_type,
       dic_for_cargo_type.name               cargo_type_name,
       volume_length,
       volume_width,
       volume_height,
       weight,
       dispatch_time,
       order_no,
       status,
       dic_for_status.name                   status_name,
       collect_type,
       dic_for_collect_type.name             collect_type_name,
       user_id,
       receiver_complex_id,
       receiver_province_id,
       receiver_city_id,
       receiver_district_id,
       receiver_name,
       sender_complex_id,
       sender_province_id,
       sender_city_id,
       sender_district_id,
       sender_name,
       payment_type,
       dic_for_payment_type.name             payment_type_name,
       cargo_num,
       amount,
       date_format(from_utc_timestamp(
                           to_timestamp(cast(info.estimate_arrive_time as bigint) * 1000), 'UTC'),
                   'yyyy-MM-dd HH:mm:ss')    estimate_arrive_time,
       distance,
       '{date}' dt  -- 核心修改：固定为当天日期
from (select after.id,
             after.order_id,
             after.cargo_type,
             after.volume_length,
             after.volume_width,
             after.volume_height,
             after.weight
      from ods_order_cargo after
      where dt = '{date}'  -- 数据源过滤条件也使用当天日期
        and after.is_deleted = '0') cargo
         join
     (select after.id,
             after.order_no,
             after.status,
             after.collect_type,
             after.user_id,
             after.receiver_complex_id,
             after.receiver_province_id,
             after.receiver_city_id,
             after.receiver_district_id,
             concat(substr(after.receiver_name, 1, 1), '*') receiver_name,
             after.sender_complex_id,
             after.sender_province_id,
             after.sender_city_id,
             after.sender_district_id,
             concat(substr(after.sender_name, 1, 1), '*')   sender_name,
             after.payment_type,
             after.cargo_num,
             after.amount,
             after.estimate_arrive_time,
             after.distance,
             concat(substr(after.update_time, 1, 10), ' ', substr(after.update_time, 12, 8)) dispatch_time
      from ods_order_info after
      where dt = '{date}'  -- 数据源过滤条件也使用当天日期
        and after.is_deleted = '0'
        and after.status not in ('60010', '60020', '60030', '60040', '60999')) info
     on cargo.order_id = info.id
         left join
     (select id, name from ods_base_dic where dt = '{date}' and is_deleted = '0') dic_for_cargo_type
     on cargo.cargo_type = cast(dic_for_cargo_type.id as string)
         left join
     (select id, name from ods_base_dic where dt = '{date}' and is_deleted = '0') dic_for_status
     on info.status = cast(dic_for_status.id as string)
         left join
     (select id, name from ods_base_dic where dt = '{date}' and is_deleted = '0') dic_for_collect_type
     on info.collect_type = cast(dic_for_collect_type.id as string)
         left join
     (select id, name from ods_base_dic where dt = '{date}' and is_deleted = '0') dic_for_payment_type
     on info.payment_type = cast(dic_for_payment_type.id as string);
"""
check_trans_dispatch = "select id, order_id, dispatch_time, dt from tms_dwd.dwd_trans_dispatch_detail_inc limit 5;"

create_and_load_dwd_table(drop_trans_dispatch, create_trans_dispatch, insert_trans_dispatch, check_trans_dispatch)


# 8. 物流域转运完成事务事实表
drop_trans_bound = "drop table if exists tms_dwd.dwd_trans_bound_finish_detail_inc;"
create_trans_bound = f"""
create external table tms_dwd.dwd_trans_bound_finish_detail_inc(
    id bigint comment '运单明细ID',
    order_id string COMMENT '运单ID',
    cargo_type string COMMENT '货物类型ID',
    cargo_type_name string COMMENT '货物类型名称',
    volumn_length bigint COMMENT '长cm',
    volumn_width bigint COMMENT '宽cm',
    volumn_height bigint COMMENT '高cm',
    weight decimal(16,2) COMMENT '重量 kg',
    bound_finish_time string COMMENT '转运完成时间',
    order_no string COMMENT '运单号',
    status string COMMENT '运单状态',
    status_name string COMMENT '运单状态名称',
    collect_type string COMMENT '取件类型',
    collect_type_name string COMMENT '取件类型名称',
    user_id bigint COMMENT '用户ID',
    receiver_complex_id bigint COMMENT '收件人小区id',
    receiver_province_id string COMMENT '收件人省份id',
    receiver_city_id string COMMENT '收件人城市id',
    receiver_district_id string COMMENT '收件人区县id',
    receiver_name string COMMENT '收件人姓名',
    sender_complex_id bigint COMMENT '发件人小区id',
    sender_province_id string COMMENT '发件人省份id',
    sender_city_id string COMMENT '发件人城市id',
    sender_district_id string COMMENT '发件人区县id',
    sender_name string COMMENT '发件人姓名',
    payment_type string COMMENT '支付方式',
    payment_type_name string COMMENT '支付方式名称',
    cargo_num bigint COMMENT '货物个数',
    amount decimal(16,2) COMMENT '金额',
    estimate_arrive_time string COMMENT '预计到达时间',
    distance decimal(16,2) COMMENT '距离'
) comment '物流域转运完成事务事实表'
    partitioned by (dt string comment '统计日期')
    stored as orc
    location 'hdfs://cdh01:8020/bigdata_warehouse/tms/tms_dwd/dwd_trans_bound_finish_detail_inc'
    tblproperties('orc.compress' = 'snappy');
"""
insert_trans_bound = f"""
insert overwrite table tms_dwd.dwd_trans_bound_finish_detail_inc
    partition (dt)
select cargo.id,
       order_id,
       cargo_type,
       dic_for_cargo_type.name               cargo_type_name,
       volume_length,
       volume_width,
       volume_height,
       weight,
       bound_finish_time,
       order_no,
       status,
       dic_for_status.name                   status_name,
       collect_type,
       dic_for_collect_type.name             collect_type_name,
       user_id,
       receiver_complex_id,
       receiver_province_id,
       receiver_city_id,
       receiver_district_id,
       receiver_name,
       sender_complex_id,
       sender_province_id,
       sender_city_id,
       sender_district_id,
       sender_name,
       payment_type,
       dic_for_payment_type.name             payment_type_name,
       cargo_num,
       amount,
       date_format(from_utc_timestamp(
                           to_timestamp(cast(info.estimate_arrive_time as bigint) * 1000), 'UTC'),
                   'yyyy-MM-dd HH:mm:ss')    estimate_arrive_time,
       distance,
       '{date}' dt  -- 核心修改：固定为当天日期
from (select after.id,
             after.order_id,
             after.cargo_type,
             after.volume_length,
             after.volume_width,
             after.volume_height,
             after.weight
      from ods_order_cargo after
      where dt = '{date}'  -- 数据源过滤条件也使用当天日期
        and after.is_deleted = '0') cargo
         join
     (select after.id,
             after.order_no,
             after.status,
             after.collect_type,
             after.user_id,
             after.receiver_complex_id,
             after.receiver_province_id,
             after.receiver_city_id,
             after.receiver_district_id,
             concat(substr(after.receiver_name, 1, 1), '*') receiver_name,
             after.sender_complex_id,
             after.sender_province_id,
             after.sender_city_id,
             after.sender_district_id,
             concat(substr(after.sender_name, 1, 1), '*')   sender_name,
             after.payment_type,
             after.cargo_num,
             after.amount,
             after.estimate_arrive_time,
             after.distance,
             concat(substr(after.update_time, 1, 10), ' ', substr(after.update_time, 12, 8)) bound_finish_time
      from ods_order_info after
      where dt = '{date}'  -- 数据源过滤条件也使用当天日期
        and after.is_deleted = '0'
        and after.status not in ('60020', '60030', '60040', '60050')) info
     on cargo.order_id = info.id
         left join
     (select id, name from ods_base_dic where dt = '{date}' and is_deleted = '0') dic_for_cargo_type
     on cargo.cargo_type = cast(dic_for_cargo_type.id as string)
         left join
     (select id, name from ods_base_dic where dt = '{date}' and is_deleted = '0') dic_for_status
     on info.status = cast(dic_for_status.id as string)
         left join
     (select id, name from ods_base_dic where dt = '{date}' and is_deleted = '0') dic_for_collect_type
     on info.collect_type = cast(dic_for_collect_type.id as string)
         left join
     (select id, name from ods_base_dic where dt = '{date}' and is_deleted = '0') dic_for_payment_type
     on info.payment_type = cast(dic_for_payment_type.id as string);
"""
check_trans_bound = "select id, order_id, bound_finish_time, dt from tms_dwd.dwd_trans_bound_finish_detail_inc limit 5;"

create_and_load_dwd_table(drop_trans_bound, create_trans_bound, insert_trans_bound, check_trans_bound)


# 9. 物流域派送成功事务事实表
drop_trans_deliver = "drop table if exists tms_dwd.dwd_trans_deliver_suc_detail_inc;"
create_trans_deliver = f"""
create external table tms_dwd.dwd_trans_deliver_suc_detail_inc(
    id bigint comment '运单明细ID',
    order_id string COMMENT '运单ID',
    cargo_type string COMMENT '货物类型ID',
    cargo_type_name string COMMENT '货物类型名称',
    volumn_length bigint COMMENT '长cm',
    volumn_width bigint COMMENT '宽cm',
    volumn_height bigint COMMENT '高cm',
    weight decimal(16,2) COMMENT '重量 kg',
    deliver_suc_time string COMMENT '派送成功时间',
    order_no string COMMENT '运单号',
    status string COMMENT '运单状态',
    status_name string COMMENT '运单状态名称',
    collect_type string COMMENT '取件类型',
    collect_type_name string COMMENT '取件类型名称',
    user_id bigint COMMENT '用户ID',
    receiver_complex_id bigint COMMENT '收件人小区id',
    receiver_province_id string COMMENT '收件人省份id',
    receiver_city_id string COMMENT '收件人城市id',
    receiver_district_id string COMMENT '收件人区县id',
    receiver_name string COMMENT '收件人姓名',
    sender_complex_id bigint COMMENT '发件人小区id',
    sender_province_id string COMMENT '发件人省份id',
    sender_city_id string COMMENT '发件人城市id',
    sender_district_id string COMMENT '发件人区县id',
    sender_name string COMMENT '发件人姓名',
    payment_type string COMMENT '支付方式',
    payment_type_name string COMMENT '支付方式名称',
    cargo_num bigint COMMENT '货物个数',
    amount decimal(16,2) COMMENT '金额',
    estimate_arrive_time string COMMENT '预计到达时间',
    distance decimal(16,2) COMMENT '距离'
) comment '物流域派送成功事务事实表'
    partitioned by (dt string comment '统计日期')
    stored as orc
    location 'hdfs://cdh01:8020/bigdata_warehouse/tms/tms_dwd/dwd_trans_deliver_suc_detail_inc'
    tblproperties('orc.compress' = 'snappy');
"""
insert_trans_deliver = f"""
insert overwrite table tms_dwd.dwd_trans_deliver_suc_detail_inc
    partition (dt)
select cargo.id,
       order_id,
       cargo_type,
       dic_for_cargo_type.name               cargo_type_name,
       volume_length,
       volume_width,
       volume_height,
       weight,
       deliver_suc_time,
       order_no,
       status,
       dic_for_status.name                   status_name,
       collect_type,
       dic_for_collect_type.name             collect_type_name,
       user_id,
       receiver_complex_id,
       receiver_province_id,
       receiver_city_id,
       receiver_district_id,
       receiver_name,
       sender_complex_id,
       sender_province_id,
       sender_city_id,
       sender_district_id,
       sender_name,
       payment_type,
       dic_for_payment_type.name             payment_type_name,
       cargo_num,
       amount,
       date_format(from_utc_timestamp(
                           to_timestamp(cast(info.estimate_arrive_time as bigint) * 1000), 'UTC'),
                   'yyyy-MM-dd HH:mm:ss')    estimate_arrive_time,
       distance,
       '{date}' dt  -- 核心修改：固定为当天日期
from (select after.id,
             after.order_id,
             after.cargo_type,
             after.volume_length,
             after.volume_width,
             after.volume_height,
             after.weight
      from ods_order_cargo after
      where dt = '{date}'  -- 数据源过滤条件也使用当天日期
        and after.is_deleted = '0') cargo
         join
     (select after.id,
             after.order_no,
             after.status,
             after.collect_type,
             after.user_id,
             after.receiver_complex_id,
             after.receiver_province_id,
             after.receiver_city_id,
             after.receiver_district_id,
             concat(substr(after.receiver_name, 1, 1), '*') receiver_name,
             after.sender_complex_id,
             after.sender_province_id,
             after.sender_city_id,
             after.sender_district_id,
             concat(substr(after.sender_name, 1, 1), '*')   sender_name,
             after.payment_type,
             after.cargo_num,
             after.amount,
             after.estimate_arrive_time,
             after.distance,
             concat(substr(after.update_time, 1, 10), ' ', substr(after.update_time, 12, 8)) deliver_suc_time
      from ods_order_info after
      where dt = '{date}'  -- 数据源过滤条件也使用当天日期
        and after.is_deleted = '0'
        and after.status not in ('60020', '60030', '60040')) info
     on cargo.order_id = info.id
         left join
     (select id, name from ods_base_dic where dt = '{date}' and is_deleted = '0') dic_for_cargo_type
     on cargo.cargo_type = cast(dic_for_cargo_type.id as string)
         left join
     (select id, name from ods_base_dic where dt = '{date}' and is_deleted = '0') dic_for_status
     on info.status = cast(dic_for_status.id as string)
         left join
     (select id, name from ods_base_dic where dt = '{date}' and is_deleted = '0') dic_for_collect_type
     on info.collect_type = cast(dic_for_collect_type.id as string)
         left join
     (select id, name from ods_base_dic where dt = '{date}' and is_deleted = '0') dic_for_payment_type
     on info.payment_type = cast(dic_for_payment_type.id as string);
"""
check_trans_deliver = "select id, order_id, deliver_suc_time, dt from tms_dwd.dwd_trans_deliver_suc_detail_inc limit 5;"

create_and_load_dwd_table(drop_trans_deliver, create_trans_deliver, insert_trans_deliver, check_trans_deliver)


# 10. 物流域签收事务事实表
drop_trans_sign = "drop table if exists tms_dwd.dwd_trans_sign_detail_inc;"
create_trans_sign = f"""
create external table tms_dwd.dwd_trans_sign_detail_inc(
    id bigint comment '运单明细ID',
    order_id string COMMENT '运单ID',
    cargo_type string COMMENT '货物类型ID',
    cargo_type_name string COMMENT '货物类型名称',
    volumn_length bigint COMMENT '长cm',
    volumn_width bigint COMMENT '宽cm',
    volumn_height bigint COMMENT '高cm',
    weight decimal(16,2) COMMENT '重量 kg',
    sign_time string COMMENT '签收时间',
    order_no string COMMENT '运单号',
    status string COMMENT '运单状态',
    status_name string COMMENT '运单状态名称',
    collect_type string COMMENT '取件类型',
    collect_type_name string COMMENT '取件类型名称',
    user_id bigint COMMENT '用户ID',
    receiver_complex_id bigint COMMENT '收件人小区id',
    receiver_province_id string COMMENT '收件人省份id',
    receiver_city_id string COMMENT '收件人城市id',
    receiver_district_id string COMMENT '收件人区县id',
    receiver_name string COMMENT '收件人姓名',
    sender_complex_id bigint COMMENT '发件人小区id',
    sender_province_id string COMMENT '发件人省份id',
    sender_city_id string COMMENT '发件人城市id',
    sender_district_id string COMMENT '发件人区县id',
    sender_name string COMMENT '发件人姓名',
    payment_type string COMMENT '支付方式',
    payment_type_name string COMMENT '支付方式名称',
    cargo_num bigint COMMENT '货物个数',
    amount decimal(16,2) COMMENT '金额',
    estimate_arrive_time string COMMENT '预计到达时间',
    distance decimal(16,2) COMMENT '距离'
) comment '物流域签收事务事实表'
    partitioned by (dt string comment '统计日期')
    stored as orc
    location 'hdfs://cdh01:8020/bigdata_warehouse/tms/tms_dwd/dwd_trans_sign_detail_inc'
    tblproperties('orc.compress' = 'snappy');
"""
insert_trans_sign = f"""
insert overwrite table tms_dwd.dwd_trans_sign_detail_inc
    partition (dt)
select cargo.id,
       order_id,
       cargo_type,
       dic_for_cargo_type.name               cargo_type_name,
       volume_length,
       volume_width,
       volume_height,
       weight,
       sign_time,
       order_no,
       status,
       dic_for_status.name                   status_name,
       collect_type,
       dic_for_collect_type.name             collect_type_name,
       user_id,
       receiver_complex_id,
       receiver_province_id,
       receiver_city_id,
       receiver_district_id,
       receiver_name,
       sender_complex_id,
       sender_province_id,
       sender_city_id,
       sender_district_id,
       sender_name,
       payment_type,
       dic_for_payment_type.name             payment_type_name,
       cargo_num,
       amount,
       date_format(from_utc_timestamp(
                           to_timestamp(cast(info.estimate_arrive_time as bigint) * 1000), 'UTC'),
                   'yyyy-MM-dd HH:mm:ss')    estimate_arrive_time,
       distance,
       '{date}' dt  -- 核心修改：固定为当天日期
from (select after.id,
             after.order_id,
             after.cargo_type,
             after.volume_length,
             after.volume_width,
             after.volume_height,
             after.weight
      from ods_order_cargo after
      where dt = '{date}'  -- 数据源过滤条件也使用当天日期
        and after.is_deleted = '0') cargo
         join
     (select after.id,
             after.order_no,
             after.status,
             after.collect_type,
             after.user_id,
             after.receiver_complex_id,
             after.receiver_province_id,
             after.receiver_city_id,
             after.receiver_district_id,
             concat(substr(after.receiver_name, 1, 1), '*') receiver_name,
             after.sender_complex_id,
             after.sender_province_id,
             after.sender_city_id,
             after.sender_district_id,
             concat(substr(after.sender_name, 1, 1), '*')   sender_name,
             after.payment_type,
             after.cargo_num,
             after.amount,
             after.estimate_arrive_time,
             after.distance,
             concat(substr(after.update_time, 1, 10), ' ', substr(after.update_time, 12, 8)) sign_time
      from ods_order_info after
      where dt = '{date}'  -- 数据源过滤条件也使用当天日期
        and after.is_deleted = '0'
        and after.status not in ('60020', '60030', '60040', '60050')) info
     on cargo.order_id = info.id
         left join
     (select id, name from ods_base_dic where dt = '{date}' and is_deleted = '0') dic_for_cargo_type
     on cargo.cargo_type = cast(dic_for_cargo_type.id as string)
         left join
     (select id, name from ods_base_dic where dt = '{date}' and is_deleted = '0') dic_for_status
     on info.status = cast(dic_for_status.id as string)
         left join
     (select id, name from ods_base_dic where dt = '{date}' and is_deleted = '0') dic_for_collect_type
     on info.collect_type = cast(dic_for_collect_type.id as string)
         left join
     (select id, name from ods_base_dic where dt = '{date}' and is_deleted = '0') dic_for_payment_type
     on info.payment_type = cast(dic_for_payment_type.id as string);
"""
check_trans_sign = "select id, order_id, sign_time, dt from tms_dwd.dwd_trans_sign_detail_inc limit 5;"

create_and_load_dwd_table(drop_trans_sign, create_trans_sign, insert_trans_sign, check_trans_sign)


# 11. 交易域运单累积快照事实表
drop_trade_process = "drop table if exists tms_dwd.dwd_trade_order_process_inc;"
create_trade_process = f"""
create external table tms_dwd.dwd_trade_order_process_inc(
    id bigint comment '运单明细ID',
    order_id string COMMENT '运单ID',
    cargo_type string COMMENT '货物类型ID',
    cargo_type_name string COMMENT '货物类型名称',
    volumn_length bigint COMMENT '长cm',
    volumn_width bigint COMMENT '宽cm',
    volumn_height bigint COMMENT '高cm',
    weight decimal(16,2) COMMENT '重量 kg',
    order_time string COMMENT '下单时间',
    order_no string COMMENT '运单号',
    status string COMMENT '运单状态',
    status_name string COMMENT '运单状态名称',
    collect_type string COMMENT '取件类型',
    collect_type_name string COMMENT '取件类型名称',
    user_id bigint COMMENT '用户ID',
    receiver_complex_id bigint COMMENT '收件人小区id',
    receiver_province_id string COMMENT '收件人省份id',
    receiver_city_id string COMMENT '收件人城市id',
    receiver_district_id string COMMENT '收件人区县id',
    receiver_name string COMMENT '收件人姓名',
    sender_complex_id bigint COMMENT '发件人小区id',
    sender_province_id string COMMENT '发件人省份id',
    sender_city_id string COMMENT '发件人城市id',
    sender_district_id string COMMENT '发件人区县id',
    sender_name string COMMENT '发件人姓名',
    payment_type string COMMENT '支付方式',
    payment_type_name string COMMENT '支付方式名称',
    cargo_num bigint COMMENT '货物个数',
    amount decimal(16,2) COMMENT '金额',
    estimate_arrive_time string COMMENT '预计到达时间',
    distance decimal(16,2) COMMENT '距离',
    start_date string COMMENT '开始日期',
    end_date string COMMENT '结束日期'
) comment '交易域运单累积快照事实表'
    partitioned by (dt string comment '统计日期')
    stored as orc
    location 'hdfs://cdh01:8020/bigdata_warehouse/tms/tms_dwd/dwd_order_process'
    tblproperties('orc.compress' = 'snappy');
"""
insert_trade_process = f"""
insert overwrite table tms_dwd.dwd_trade_order_process_inc
    partition (dt)
select cargo.id,
       order_id,
       cargo_type,
       dic_for_cargo_type.name               cargo_type_name,
       volume_length,
       volume_width,
       volume_height,
       weight,
       order_time,
       order_no,
       status,
       dic_for_status.name                   status_name,
       collect_type,
       dic_for_collect_type.name             collect_type_name,
       user_id,
       receiver_complex_id,
       receiver_province_id,
       receiver_city_id,
       receiver_district_id,
       receiver_name,
       sender_complex_id,
       sender_province_id,
       sender_city_id,
       sender_district_id,
       sender_name,
       payment_type,
       dic_for_payment_type.name             payment_type_name,
       cargo_num,
       amount,
       date_format(from_utc_timestamp(
                           to_timestamp(cast(info.estimate_arrive_time as bigint) * 1000), 'UTC'),
                   'yyyy-MM-dd HH:mm:ss')    estimate_arrive_time,
       distance,
       date_format(order_time, 'yyyy-MM-dd') start_date,
       end_date,
       '{date}' dt  -- 核心修改：固定为当天日期
from (select after.id,
             after.order_id,
             after.cargo_type,
             after.volume_length,
             after.volume_width,
             after.volume_height,
             after.weight,
             concat(substr(after.create_time, 1, 10), ' ', substr(after.create_time, 12, 8)) order_time
      from ods_order_cargo after
      where dt = '{date}'  -- 数据源过滤条件也使用当天日期
        and after.is_deleted = '0') cargo
         join
     (select after.id,
             after.order_no,
             after.status,
             after.collect_type,
             after.user_id,
             after.receiver_complex_id,
             after.receiver_province_id,
             after.receiver_city_id,
             after.receiver_district_id,
             concat(substr(after.receiver_name, 1, 1), '*') receiver_name,
             after.sender_complex_id,
             after.sender_province_id,
             after.sender_city_id,
             after.sender_district_id,
             concat(substr(after.sender_name, 1, 1), '*')   sender_name,
             after.payment_type,
             after.cargo_num,
             after.amount,
             after.estimate_arrive_time,
             after.distance,
             case when after.status in ('60020', '60030') 
                  then substr(after.update_time, 1, 10) 
                  else '9999-12-31' end end_date
      from ods_order_info after
      where dt = '{date}'  -- 数据源过滤条件也使用当天日期
        and after.is_deleted = '0') info
     on cargo.order_id = info.id
         left join
     (select id, name from ods_base_dic where dt = '{date}' and is_deleted = '0') dic_for_cargo_type
     on cargo.cargo_type = cast(dic_for_cargo_type.id as string)
         left join
     (select id, name from ods_base_dic where dt = '{date}' and is_deleted = '0') dic_for_status
     on info.status = cast(dic_for_status.id as string)
         left join
     (select id, name from ods_base_dic where dt = '{date}' and is_deleted = '0') dic_for_collect_type
     on info.collect_type = cast(dic_for_collect_type.id as string)
         left join
     (select id, name from ods_base_dic where dt = '{date}' and is_deleted = '0') dic_for_payment_type
     on info.payment_type = cast(dic_for_payment_type.id as string);
"""
check_trade_process = "select id, order_id, start_date, end_date, dt from tms_dwd.dwd_trade_order_process_inc limit 5;"

create_and_load_dwd_table(drop_trade_process, create_trade_process, insert_trade_process, check_trade_process)


# 12. 物流域运输事务事实表
drop_trans_trans = "drop table if exists tms_dwd.dwd_trans_trans_finish_inc;"
create_trans_trans = f"""
create external table tms_dwd.dwd_trans_trans_finish_inc(
    id bigint comment '运输任务ID',
    shift_id bigint COMMENT '车次ID',
    line_id bigint COMMENT '路线ID',
    start_org_id bigint COMMENT '起始机构ID',
    start_org_name string COMMENT '起始机构名称',
    end_org_id bigint COMMENT '目的机构ID',
    end_org_name string COMMENT '目的机构名称',
    order_num bigint COMMENT '运单个数',
    driver1_emp_id bigint COMMENT '司机1ID',
    driver1_name string COMMENT '司机1名称',
    driver2_emp_id bigint COMMENT '司机2ID',
    driver2_name string COMMENT '司机2名称',
    truck_id bigint COMMENT '卡车ID',
    truck_no string COMMENT '卡车号牌',
    actual_start_time string COMMENT '实际启动时间',
    actual_end_time string COMMENT '实际到达时间',
    actual_distance decimal(16,2) COMMENT '实际行驶距离',
    finish_dur_sec bigint COMMENT '运输完成历经时长：秒'
) comment '物流域运输事务事实表'
    partitioned by (dt string comment '统计日期')
    stored as orc
    location 'hdfs://cdh01:8020/bigdata_warehouse/tms/tms_dwd/dwd_trans_trans_finish_inc'
    tblproperties('orc.compress' = 'snappy');
"""
insert_trans_trans = f"""
insert overwrite table tms_dwd.dwd_trans_trans_finish_inc
    partition (dt = '{date}')  -- 核心修改：固定为当天日期
select info.id,
       shift_id,
       line_id,
       start_org_id,
       start_org_name,
       end_org_id,
       end_org_name,
       order_num,
       driver1_emp_id,
       driver1_name,
       driver2_emp_id,
       driver2_name,
       truck_id,
       truck_no,
       date_format(from_utc_timestamp(
                           to_timestamp(cast(info.actual_start_time as bigint) * 1000), 'UTC'),
                   'yyyy-MM-dd HH:mm:ss')    actual_start_time,
       date_format(from_utc_timestamp(
                           to_timestamp(cast(info.actual_end_time as bigint) * 1000), 'UTC'),
                   'yyyy-MM-dd HH:mm:ss')    actual_end_time,
       actual_distance,
       finish_dur_sec
from (select after.id,
             after.shift_id,
             after.line_id,
             after.start_org_id,
             after.start_org_name,
             after.end_org_id,
             after.end_org_name,
             after.order_num,
             after.driver1_emp_id,
             concat(substr(after.driver1_name, 1, 1), '*') driver1_name,
             after.driver2_emp_id,
             concat(substr(after.driver2_name, 1, 1), '*') driver2_name,
             after.truck_id,
             md5(after.truck_no) truck_no,
             after.actual_start_time,
             after.actual_end_time,
             after.actual_distance,
             (cast(after.actual_end_time as bigint) - cast(after.actual_start_time as bigint)) / 1000 finish_dur_sec
      from ods_transport_task after
      where dt = '{date}'  -- 数据源过滤条件也使用当天日期
        and after.actual_end_time is not null
        and after.is_deleted = '0') info
         left join
     (select id from tms_dim.dim_shift_full where dt = '{date}') dim_tb
     on info.shift_id = dim_tb.id;
"""
check_trans_trans = "select id, shift_id, actual_start_time, actual_end_time, dt from tms_dwd.dwd_trans_trans_finish_inc limit 5;"

create_and_load_dwd_table(drop_trans_trans, create_trans_trans, insert_trans_trans, check_trans_trans)


# 13. 中转域入库事务事实表
drop_bound_inbound = "drop table if exists tms_dwd.dwd_bound_inbound_inc;"
create_bound_inbound = f"""
create external table tms_dwd.dwd_bound_inbound_inc(
    id bigint COMMENT '中转记录ID',
    order_id bigint COMMENT '运单ID',
    org_id bigint COMMENT '机构ID',
    inbound_time string COMMENT '入库时间',
    inbound_emp_id bigint COMMENT '入库人员'
) comment '中转域入库事务事实表'
    partitioned by (dt string comment '统计日期')
    stored as orc
    location 'hdfs://cdh01:8020/bigdata_warehouse/tms/tms_dwd/dwd_bound_inbound_inc'
    tblproperties('orc.compress' = 'snappy');
"""
insert_bound_inbound = f"""
insert overwrite table tms_dwd.dwd_bound_inbound_inc
    partition (dt)
select after.id,
       after.order_id,
       after.org_id,
       date_format(from_utc_timestamp(
                           to_timestamp(cast(after.inbound_time as bigint) * 1000), 'UTC'),
                   'yyyy-MM-dd HH:mm:ss') inbound_time,
       after.inbound_emp_id,
       '{date}' dt  -- 核心修改：固定为当天日期
from ods_order_org_bound after
where dt = '{date}'  -- 数据源过滤条件也使用当天日期
  and after.is_deleted = '0';
"""
check_bound_inbound = "select id, order_id, inbound_time, dt from tms_dwd.dwd_bound_inbound_inc limit 5;"

create_and_load_dwd_table(drop_bound_inbound, create_bound_inbound, insert_bound_inbound, check_bound_inbound)


# 14. 中转域分拣事务事实表
drop_bound_sort = "drop table if exists tms_dwd.dwd_bound_sort_inc;"
create_bound_sort = f"""
create external table tms_dwd.dwd_bound_sort_inc(
    id bigint COMMENT '中转记录ID',
    order_id bigint COMMENT '订单ID',
    org_id bigint COMMENT '机构ID',
    sort_time string COMMENT '分拣时间',
    sorter_emp_id bigint COMMENT '分拣人员'
) comment '中转域分拣事务事实表'
    partitioned by (dt string comment '统计日期')
    stored as orc
    location 'hdfs://cdh01:8020/bigdata_warehouse/tms/tms_dwd/dwd_bound_sort_inc'
    tblproperties('orc.compress' = 'snappy');
"""
insert_bound_sort = f"""
insert overwrite table tms_dwd.dwd_bound_sort_inc
    partition (dt)
select after.id,
       after.order_id,
       after.org_id,
       date_format(from_utc_timestamp(
                           to_timestamp(cast(after.sort_time as bigint) * 1000), 'UTC'),
                   'yyyy-MM-dd HH:mm:ss') sort_time,
       after.sorter_emp_id,
       '{date}' dt  -- 核心修改：固定为当天日期
from ods_order_org_bound after
where dt = '{date}'  -- 数据源过滤条件也使用当天日期
  and after.sort_time is not null
  and after.is_deleted = '0';
"""
check_bound_sort = "select id, order_id, sort_time, dt from tms_dwd.dwd_bound_sort_inc limit 5;"

create_and_load_dwd_table(drop_bound_sort, create_bound_sort, insert_bound_sort, check_bound_sort)


# 15. 中转域出库事务事实表
drop_bound_outbound = "drop table if exists tms_dwd.dwd_bound_outbound_inc;"
create_bound_outbound = f"""
create external table tms_dwd.dwd_bound_outbound_inc(
    id bigint COMMENT '中转记录ID',
    order_id bigint COMMENT '订单ID',
    org_id bigint COMMENT '机构ID',
    outbound_time string COMMENT '出库时间',
    outbound_emp_id bigint COMMENT '出库人员'
) comment '中转域出库事务事实表'
    partitioned by (dt string comment '统计日期')
    stored as orc
    location 'hdfs://cdh01:8020/bigdata_warehouse/tms/tms_dwd/dwd_bound_outbound_inc'
    tblproperties('orc.compress' = 'snappy');
"""
insert_bound_outbound = f"""
insert overwrite table tms_dwd.dwd_bound_outbound_inc
    partition (dt)
select after.id,
       after.order_id,
       after.org_id,
       date_format(from_utc_timestamp(
                           to_timestamp(cast(after.outbound_time as