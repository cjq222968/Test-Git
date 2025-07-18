import os
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, FloatType
import random

# 配置环境变量
os.environ["SPARK_HOME"] = "D:\IDEA\spark-3.4.1"
os.environ["JAVA_HOME"] = "D:\IDEA\jdk17.0.1"
os.environ["PYSPARK_PYTHON"] = "D:\PyCharm\ANAConda\envs\conda\python.exe"

# 初始化 SparkSession（添加中文列名支持）
spark = SparkSession.builder \
    .appName("IDEA_PySpark_Test") \
    .master("local[*]") \
    .config("spark.ui.port", "4050") \
    .config("spark.sql.parser.quotedRegexColumnNames", "true") \
    .getOrCreate()

print("="*50)
print(f"Spark 版本: {spark.version}")
print("="*50)

# 定义数据结构
schema = StructType([
    StructField("user_id", IntegerType(), nullable=False),
    StructField("name", StringType(), nullable=False),
    StructField("age", IntegerType(), nullable=True),
    StructField("gender", StringType(), nullable=True),
    StructField("salary", FloatType(), nullable=True)
])

# 生成模拟数据
def generate_data(num_rows):
    data = []
    genders = ["男", "女", "其他"]
    for i in range(num_rows):
        user_id = i + 1
        name = f"用户_{i:03d}"
        age = random.randint(18, 60)
        gender = random.choice(genders)
        salary = round(random.uniform(3000, 20000), 2)
        data.append((user_id, name, age, gender, salary))
    return data

# 创建 DataFrame
mock_data = generate_data(1000)
df = spark.createDataFrame(mock_data, schema)

# 执行基本操作
print("\n【1. 查看数据结构】")
df.printSchema()

print("\n【2. 查看数据行数】")
print(f"总行数: {df.count()}")

print("\n【3. 查看数据前 5 行】")
df.show(5, truncate=False)

print("\n【4. 统计不同性别数量】")
gender_count = df.groupBy("gender").count()
gender_count.show()

print("\n【5. 计算平均工资】")
# 使用反引号包裹中文列名
avg_salary = df.selectExpr("avg(salary) as `平均工资`").first()["平均工资"]
print(f"平均工资: {avg_salary:.2f} 元")

print("\n【6. 筛选年龄大于 40 且工资大于 15000 的用户】")
filtered_df = df.filter("age > 40 AND salary > 15000")
filtered_df.show(5, truncate=False)

# 停止 SparkSession
spark.stop()
print("\n测试完成！")