from pyspark.sql import SparkSession
import os
import re
import traceback

# 1. 自定义临时目录配置（优先使用脚本所在目录）
script_dir = os.path.dirname(os.path.abspath(__file__))
temp_dir = os.path.join(script_dir, "spark-temp")
os.makedirs(temp_dir, exist_ok=True)
os.chmod(temp_dir, 0o755)  # 确保权限

# 2. 初始化SparkSession（强制指定临时目录，由Spark自身管理清理）
spark = SparkSession.builder \
    .appName("TmsDimIntegration") \
    .master("local[*]") \
    .config("hive.metastore.uris", "thrift://cdh01:9083") \
    .config("spark.sql.warehouse.dir", "/home/user/hive/warehouse") \
    .config("spark.hadoop.hive.support.quoted.identifiers", "none") \
    .config("spark.hadoop.hive.exec.dynamic.partition.mode", "nonstrict") \
    .config("spark.hadoop.hive.auto.convert.join", "true") \
    .config("spark.cleaner.referenceTracking.cleanCheckpoints", "false") \
    .config("spark.local.dir", temp_dir)  \
.config("spark.shuffle.service.enabled", "false") \
    .enableHiveSupport() \
    .getOrCreate()

# 3. 设置日志级别
sc = spark.sparkContext
sc.setLogLevel("ERROR")

# 4. 定义日期变量
date = "2025-07-18"

def create_and_load_table(drop_sql, create_sql, insert_sql, check_sql):
    """封装表创建和数据加载逻辑"""
    try:
        # 提取表名
        table_match = re.search(r'create external table (\w+\.\w+)', create_sql, re.IGNORECASE)
        if not table_match:
            raise ValueError("无法提取表名")
        full_table_name = table_match.group(1)
        table_name = full_table_name.split('.')[1]

        # 执行SQL操作
        spark.sql(drop_sql)
        spark.sql(create_sql)

        # 检查数据是否已存在
        count_sql = f"select count(1) from {full_table_name} where dt = '{date}'"
        data_count = spark.sql(count_sql).collect()[0][0]

        if data_count > 0:
            print(f"表 {table_name} 已有数据，跳过插入")
        else:
            spark.sql(insert_sql)
            print(f"表 {table_name} 数据插入完成")

        # 验证数据
        spark.sql(check_sql).show(10)
        print(f"表处理成功: {table_name}\n")

    except Exception as e:
        print(f"表 {table_name} 处理失败: {str(e)}")
        traceback.print_exc()
        spark.stop()
        exit(1)

# 5. 业务逻辑（与之前一致）
# 显示数据库
spark.sql("show databases;").show()

# 创建数据库tms_dim
spark.sql(f"""
create database IF NOT EXISTS tms_dim
    LOCATION 'hdfs://cdh01:8020/bigdata_warehouse/tms/tms_dim/';
""")

# 切换到tms_ods数据库
spark.sql("use tms_ods;")

# 处理dim_complex_full表
drop_complex = "drop table if exists tms_dim.dim_complex_full;"
create_complex = f"""
create external table tms_dim.dim_complex_full(
  id bigint comment '小区ID',
  complex_name string comment '小区名称',
  courier_emp_ids array<string> comment '负责快递员IDS',
  province_id bigint comment '省份ID',
  province_name string comment '省份名称',
  city_id bigint comment '城市ID',
  city_name string comment '城市名称',
  district_id bigint comment '区（县）ID',
  district_name string comment '区（县）名称'
) comment '小区维度表'
    partitioned by (dt string comment '统计日期')
    stored as orc
    location 'hdfs://cdh01:8020/bigdata_warehouse/tms/tms_dim/dim_complex_full'
    tblproperties('orc.compress'='snappy');
"""
insert_complex = f"""
insert overwrite table tms_dim.dim_complex_full
    partition (dt = '{date}')
select complex_info.id   id,
       complex_name,
       courier_emp_ids,
       province_id,
       dic_for_prov.name province_name,
       city_id,
       dic_for_city.name city_name,
       district_id,
       district_name
from (select id,
             complex_name,
             province_id,
             city_id,
             district_id,
             district_name
      from ods_base_complex
      where dt = '{date}'
        and is_deleted = '0') complex_info
         join
     (select id,
             name
      from ods_base_region_info
      where dt = '{date}'
        and is_deleted = '0') dic_for_prov
     on complex_info.province_id = dic_for_prov.id
         join
     (select id,
             name
      from ods_base_region_info
      where dt = '{date}'
        and is_deleted = '0') dic_for_city
     on complex_info.city_id = dic_for_city.id
         left join
     (select
          collect_set(cast(courier_emp_id as string)) courier_emp_ids,
          complex_id
      from ods_express_courier_complex 
      where dt = '{date}' 
        and is_deleted = '0'
      group by complex_id
     ) complex_courier
     on complex_info.id = complex_courier.complex_id;
"""
check_complex = "select * from tms_dim.dim_complex_full limit 10;"
create_and_load_table(drop_complex, create_complex, insert_complex, check_complex)


# 5. 处理dim_organ_full表
drop_organ = "drop table if exists tms_dim.dim_organ_full;"
create_organ = f"""
create external table tms_dim.dim_organ_full(
    id bigint COMMENT '机构ID',
    org_name string COMMENT '机构名称',
    org_level bigint COMMENT '机构等级',
    region_id bigint COMMENT '地区ID',
    region_name string COMMENT '地区名称',
    region_code string COMMENT '地区编码',
    org_parent_id bigint COMMENT '父级机构ID',
    org_parent_name string COMMENT '父级机构名称'
) comment '机构维度表'
    partitioned by (dt string comment '统计日期')
    stored as orc
    location 'hdfs://cdh01:8020/bigdata_warehouse/tms/tms_dim/dim_organ_full'
    tblproperties('orc.compress'='snappy');
"""
insert_organ = f"""
insert overwrite table tms_dim.dim_organ_full
    partition (dt = '{date}')
select organ_info.id,
       organ_info.org_name,
       org_level,
       region_id,
       region_info.name        region_name,
       region_info.dict_code   region_code,
       org_parent_id,
       org_for_parent.org_name org_parent_name
from (select id,
             org_name,
             org_level,
             region_id,
             org_parent_id
      from ods_base_organ
      where dt = '{date}'
        and is_deleted = '0') organ_info
         left join (
    select id,
           name,
           dict_code
    from ods_base_region_info
    where dt = '{date}'
      and is_deleted = '0'
) region_info
                   on organ_info.region_id = region_info.id
         left join (
    select id,
           org_name
    from ods_base_organ
    where dt = '{date}'
      and is_deleted = '0'
) org_for_parent on organ_info.org_parent_id = org_for_parent.id;
"""
check_organ = "select * from tms_dim.dim_organ_full limit 10;"
create_and_load_table(drop_organ, create_organ, insert_organ, check_organ)


# 6. 处理dim_region_full表
drop_region = "drop table if exists tms_dim.dim_region_full;"
create_region = f"""
create external table tms_dim.dim_region_full(
     id bigint COMMENT '地区ID',
     parent_id bigint COMMENT '上级地区ID',
     name string COMMENT '地区名称',
     dict_code string COMMENT '编码',
     short_name string COMMENT '简称'
) comment '地区维度表'
    partitioned by (dt string comment '统计日期')
    stored as orc
    location 'hdfs://cdh01:8020/bigdata_warehouse/tms/tms_dim/dim_region_full'
    tblproperties('orc.compress'='snappy');
"""
insert_region = f"""
insert overwrite table tms_dim.dim_region_full
    partition (dt = '{date}')
select id,
       parent_id,
       name,
       dict_code,
       short_name
from ods_base_region_info
where dt = '{date}'
  and is_deleted = '0';
"""
check_region = "select * from tms_dim.dim_region_full limit 10;"
create_and_load_table(drop_region, create_region, insert_region, check_region)


# 7. 处理dim_express_courier_full表
drop_courier = "drop table if exists tms_dim.dim_express_courier_full;"
create_courier = f"""
create external table tms_dim.dim_express_courier_full(
    id bigint COMMENT '快递员ID',
    emp_id bigint COMMENT '员工ID',
    org_id bigint COMMENT '所属机构ID',
    org_name string COMMENT '机构名称',
    working_phone string COMMENT '工作电话',
    express_type string COMMENT '快递员类型',
    express_type_name string COMMENT '类型名称'
) comment '快递员维度表'
    partitioned by (dt string comment '统计日期')
    stored as orc
    location 'hdfs://cdh01:8020/bigdata_warehouse/tms/tms_dim/dim_express_courier_full'
    tblproperties('orc.compress'='snappy');
"""
insert_courier = f"""
insert overwrite table tms_dim.dim_express_courier_full
    partition (dt = '{date}')
select express_cor_info.id,
       emp_id,
       org_id,
       org_name,
       working_phone,
       express_type,
       dic_info.name express_type_name
from (select id,
             emp_id,
             org_id,
             md5(working_phone) working_phone,
             express_type
      from ods_express_courier
      where dt = '{date}'
        and is_deleted = '0') express_cor_info
         join (
    select id,
           org_name
    from ods_base_organ
    where dt = '{date}'
      and is_deleted = '0'
) organ_info
              on express_cor_info.org_id = organ_info.id
         join (
    select id,
           name
    from ods_base_dic
    where dt = '{date}'
      and is_deleted = '0'
) dic_info
              on express_type = dic_info.id;
"""
check_courier = "select * from tms_dim.dim_express_courier_full limit 10;"
create_and_load_table(drop_courier, create_courier, insert_courier, check_courier)


# 8. 处理dim_shift_full表
drop_shift = "drop table if exists tms_dim.dim_shift_full;"
create_shift = f"""
create external table tms_dim.dim_shift_full(
    id bigint COMMENT '班次ID',
    line_id bigint COMMENT '线路ID',
    line_name string COMMENT '线路名称',
    line_no string COMMENT '线路编号',
    line_level string COMMENT '线路级别',
    org_id bigint COMMENT '所属机构',
    transport_line_type_id string COMMENT '线路类型ID',
    transport_line_type_name string COMMENT '类型名称',
    start_org_id bigint COMMENT '起始机构ID',
    start_org_name string COMMENT '起始机构名称',
    end_org_id bigint COMMENT '目标机构ID',
    end_org_name string COMMENT '目标机构名称',
    pair_line_id bigint COMMENT '配对线路ID',
    distance decimal(10,2) COMMENT '直线距离',
    cost decimal(10,2) COMMENT '公路里程',
    estimated_time bigint COMMENT '预计时间（分钟）',
    start_time string COMMENT '班次开始时间',
    driver1_emp_id bigint COMMENT '第一司机',
    driver2_emp_id bigint COMMENT '第二司机',
    truck_id bigint COMMENT '卡车ID',
    pair_shift_id bigint COMMENT '配对班次'
) comment '班次维度表'
    partitioned by (dt string comment '统计周期')
    stored as orc
    location 'hdfs://cdh01:8020/bigdata_warehouse/tms/tms_dim/dim_shift_full'
    tblproperties('orc.compress'='snappy');
"""
insert_shift = f"""
insert overwrite table tms_dim.dim_shift_full
    partition (dt = '{date}')
select shift_info.id,
       line_id,
       line_info.name line_name,
       line_no,
       line_level,
       org_id,
       transport_line_type_id,
       dic_info.name  transport_line_type_name,
       start_org_id,
       start_org_name,
       end_org_id,
       end_org_name,
       pair_line_id,
       distance,
       cost,
       estimated_time,
       start_time,
       driver1_emp_id,
       driver2_emp_id,
       truck_id,
       pair_shift_id
from (select id,
             line_id,
             start_time,
             driver1_emp_id,
             driver2_emp_id,
             truck_id,
             pair_shift_id
      from ods_line_base_shift
      where dt = '{date}'
        and is_deleted = '0') shift_info
         join
     (select id,
             name,
             line_no,
             line_level,
             org_id,
             transport_line_type_id,
             start_org_id,
             start_org_name,
             end_org_id,
             end_org_name,
             pair_line_id,
             distance,
             cost,
             estimated_time
      from ods_line_base_info
      where dt = '{date}'
        and is_deleted = '0') line_info
     on shift_info.line_id = line_info.id
         join (
    select id,
           name
    from ods_base_dic
    where dt = '{date}'
      and is_deleted = '0'
) dic_info on line_info.transport_line_type_id = dic_info.id;
"""
check_shift = "select * from tms_dim.dim_shift_full limit 10;"
create_and_load_table(drop_shift, create_shift, insert_shift, check_shift)


# 9. 处理dim_truck_driver_full表
drop_truck_driver = "drop table if exists tms_dim.dim_truck_driver_full;"
create_truck_driver = f"""
create external table tms_dim.dim_truck_driver_full(
    id bigint COMMENT '司机信息ID',
    emp_id bigint COMMENT '员工ID',
    org_id bigint COMMENT '所属机构ID',
    org_name string COMMENT '机构名称',
    team_id bigint COMMENT '所属车队ID',
    tream_name string COMMENT '车队名称',
    license_type string COMMENT '准驾车型',
    init_license_date string COMMENT '初次领证日期',
    expire_date string COMMENT '有效截止日期',
    license_no string COMMENT '驾驶证号',
    is_enabled tinyint COMMENT '状态'
) comment '司机维度表'
    partitioned by (dt string comment '统计日期')
    stored as orc
    location 'hdfs://cdh01:8020/bigdata_warehouse/tms/tms_dim/dim_truck_driver_full'
    tblproperties('orc.compress'='snappy');
"""
insert_truck_driver = f"""
insert overwrite table tms_dim.dim_truck_driver_full
    partition (dt = '{date}')
select driver_info.id,
       emp_id,
       org_id,
       organ_info.org_name,
       team_id,
       team_info.name team_name,
       license_type,
       init_license_date,
       expire_date,
       license_no,
       is_enabled
from (select id,
             emp_id,
             org_id,
             team_id,
             license_type,
             init_license_date,
             expire_date,
             license_no,
             is_enabled
      from ods_truck_driver
      where dt = '{date}'
        and is_deleted = '0') driver_info
         join (
    select id,
           org_name
    from ods_base_organ
    where dt = '{date}'
      and is_deleted = '0'
) organ_info
              on driver_info.org_id = organ_info.id
         join (
    select id,
           name
    from ods_truck_team
    where dt = '{date}'
      and is_deleted = '0'
) team_info
              on driver_info.team_id = team_info.id;
"""
check_truck_driver = "select * from tms_dim.dim_truck_driver_full limit 10;"
create_and_load_table(drop_truck_driver, create_truck_driver, insert_truck_driver, check_truck_driver)


# 10. 处理dim_truck_full表
drop_truck = "drop table if exists tms_dim.dim_truck_full;"
create_truck = f"""
create external table tms_dim.dim_truck_full(
    id bigint COMMENT '卡车ID',
    team_id bigint COMMENT '所属车队ID',
    team_name string COMMENT '车队名称',
    team_no string COMMENT '车队编号',
    org_id bigint COMMENT '所属机构',
    org_name string COMMENT '机构名称',
    manager_emp_id bigint COMMENT '负责人',
    truck_no string COMMENT '车牌号码',
    truck_model_id string COMMENT '型号',
    truck_model_name string COMMENT '型号名称',
    truck_model_type string COMMENT '型号类型',
    truck_model_type_name string COMMENT '类型名称',
    truck_model_no string COMMENT '型号编码',
    truck_brand string COMMENT '品牌',
    truck_brand_name string COMMENT '品牌名称',
    truck_weight decimal(16,2) COMMENT '整车重量',
    load_weight decimal(16,2) COMMENT '额定载重',
    total_weight decimal(16,2) COMMENT '总质量',
    eev string COMMENT '排放标准',
    boxcar_len decimal(16,2) COMMENT '货箱长',
    boxcar_wd decimal(16,2) COMMENT '货箱宽',
    boxcar_hg decimal(16,2) COMMENT '货箱高',
    max_speed bigint COMMENT '最高时速',
    oil_vol bigint COMMENT '油箱容积',
    device_gps_id string COMMENT 'GPS设备ID',
    engine_no string COMMENT '发动机编码',
    license_registration_date string COMMENT '注册时间',
    license_last_check_date string COMMENT '最后年检日期',
    license_expire_date string COMMENT '失效日期',
    is_enabled tinyint COMMENT '状态'
) comment '卡车维度表'
    partitioned by (dt string comment '统计日期')
    stored as orc
    location 'hdfs://cdh01:8020/bigdata_warehouse/tms/tms_dim/dim_truck_full'
    tblproperties('orc.compress'='snappy');
"""
insert_truck = f"""
insert overwrite table tms_dim.dim_truck_full
    partition (dt = '{date}')
select truck_info.id,
       team_id,
       team_info.name     team_name,
       team_no,
       org_id,
       org_name,
       manager_emp_id,
       truck_no,
       truck_model_id,
       model_name         truck_model_name,
       model_type         truck_model_type,
       dic_for_type.name  truck_model_type_name,
       model_no           truck_model_no,
       brand              truck_brand,
       dic_for_brand.name truck_brand_name,
       truck_weight,
       load_weight,
       total_weight,
       eev,
       boxcar_len,
       boxcar_wd,
       boxcar_hg,
       max_speed,
       oil_vol,
       device_gps_id,
       engine_no,
       license_registration_date,
       license_last_check_date,
       license_expire_date,
       is_enabled
from (select id,
             team_id,
             md5(truck_no) truck_no,
             truck_model_id,
             device_gps_id,
             engine_no,
             license_registration_date,
             license_last_check_date,
             license_expire_date,
             is_enabled
      from ods_truck_info
      where dt = '{date}'
        and is_deleted = '0') truck_info
         join
     (select id,
             name,
             team_no,
             org_id,
             manager_emp_id
      from ods_truck_team
      where dt = '{date}'
        and is_deleted = '0') team_info
     on truck_info.team_id = team_info.id
         join
     (select id,
             model_name,
             model_type,
             model_no,
             brand,
             truck_weight,
             load_weight,
             total_weight,
             eev,
             boxcar_len,
             boxcar_wd,
             boxcar_hg,
             max_speed,
             oil_vol
      from ods_truck_model
      where dt = '{date}'
        and is_deleted = '0') model_info
     on truck_info.truck_model_id = model_info.id
         join
     (select id,
             org_name
      from ods_base_organ
      where dt = '{date}'
        and is_deleted = '0'
     ) organ_info
     on org_id = organ_info.id
         join
     (select id,
             name
      from ods_base_dic
      where dt = '{date}'
        and is_deleted = '0') dic_for_type
     on model_info.model_type = dic_for_type.id
         join
     (select id,
             name
      from ods_base_dic
      where dt = '{date}'
        and is_deleted = '0') dic_for_brand
     on model_info.brand = dic_for_brand.id;
"""
check_truck = "select * from tms_dim.dim_truck_full limit 10;"
create_and_load_table(drop_truck, create_truck, insert_truck, check_truck)


# 11. 处理dim_user_zip表
drop_user = "drop table if exists tms_dim.dim_user_zip;"
create_user = f"""
create external table tms_dim.dim_user_zip(
    id bigint COMMENT '用户地址信息ID',
    login_name string COMMENT '用户名称',
    nick_name string COMMENT '用户昵称',
    passwd string COMMENT '密码',
    real_name string COMMENT '姓名',
    phone_num string COMMENT '手机号',
    email string COMMENT '邮箱',
    user_level string COMMENT '用户级别',
    birthday string COMMENT '生日',
    gender string COMMENT '性别',
    start_date string COMMENT '起始日期',
    end_date string COMMENT '结束日期'
) comment '用户拉链表'
    partitioned by (dt string comment '统计日期')
    stored as orc
    location 'hdfs://cdh01:8020/bigdata_warehouse/tms/tms_dim/dim_user_zip'
    tblproperties('orc.compress'='snappy');
"""
insert_user = f"""
insert overwrite table tms_dim.dim_user_zip
    partition (dt = '{date}')
select after.id,
       after.login_name,
       after.nick_name,
       md5(after.passwd)                                    as passwd,
       md5(after.real_name)                                as realname,
       md5(if(after.phone_num regexp '^(13[0-9]|14[01456879]|15[0-35-9]|16[2567]|17[0-8]|18[0-9]|19[0-35-9])\\d{8}$',
              after.phone_num, null))  as phone_num,
       md5(if(after.email regexp '^[a-zA-Z0-9_-]+@[a-zA-Z0-9_-]+(\\.[a-zA-Z0-9_-]+)+$', after.email, null)) as email,
       after.user_level,
       date_add('1970-01-01', cast(after.birthday as int))  as birthday,
       after.gender,
       date_format(from_utc_timestamp(
                           to_timestamp(cast(after.create_time as bigint) * 1000), 'UTC'),
                   'yyyy-MM-dd')                                                                            start_date,
       '9999-12-31'                                                                                         end_date
from ods_user_info after
where dt = '{date}'
  and after.is_deleted = '0';
"""
check_user = "select * from tms_dim.dim_user_zip limit 10;"
create_and_load_table(drop_user, create_user, insert_user, check_user)


# 12. 处理dim_user_address_zip表
drop_user_addr = "drop table if exists tms_dim.dim_user_address_zip;"
create_user_addr = f"""
create external table tms_dim.dim_user_address_zip(
    id bigint COMMENT '地址ID',
    user_id bigint COMMENT '用户ID',
    phone string COMMENT '电话号',
    province_id bigint COMMENT '省份ID',
    city_id bigint COMMENT '城市ID',
    district_id bigint COMMENT '区县ID',
    complex_id bigint COMMENT '小区ID',
    address string COMMENT '详细地址',
    is_default tinyint COMMENT '是否默认',
    start_date string COMMENT '起始日期',
    end_date string COMMENT '结束日期'
) comment '用户地址拉链表'
    partitioned by (dt string comment '统计日期')
    stored as orc
    location 'hdfs://cdh01:8020/bigdata_warehouse/tms/tms_dim/dim_user_address_zip'
    tblproperties('orc.compress'='snappy');
"""
insert_user_addr = f"""
insert overwrite table tms_dim.dim_user_address_zip
    partition (dt = '{date}')
select after.id,
       after.user_id,
       md5(if(after.phone regexp
              '^(13[0-9]|14[01456879]|15[0-35-9]|16[2567]|17[0-8]|18[0-9]|19[0-35-9])\\d{8}$',
              after.phone, null))as               phone,
       after.province_id,
       after.city_id,
       after.district_id,
       after.complex_id,
       after.address,
       after.is_default,
       concat(substr(after.create_time, 1, 10), ' ',
              substr(after.create_time, 12, 8)) start_date,
       '9999-12-31'                             end_date
from ods_user_address after
where dt = '{date}'
  and after.is_deleted = '0';
"""
check_user_addr = "select * from tms_dim.dim_user_address_zip limit 10;"
create_and_load_table(drop_user_addr, create_user_addr, insert_user_addr, check_user_addr)


# 停止SparkSession（临时目录由Spark自动清理）
spark.stop()