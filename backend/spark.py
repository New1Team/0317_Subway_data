import os
from sqlalchemy import create_engine, inspect
from pyspark.sql import SparkSession
import pandas as pd

engine_mariadb = create_engine('mysql+pymysql://root:1234@192.168.0.201:3306/seoul_metro')

print('시작1')
# 생성된 테이블 확인
def table_check():
    inspector = inspect(engine_mariadb)
    tables = inspector.get_table_names()
    print(tables)

table_check()

# print('시작2')
# # data의 파일 읽기
current_path = os.path.dirname(os.path.abspath(__file__))
data_path = os.path.join(current_path, "data")
all_files = os.listdir(data_path)
for file in all_files:
    file_path = os.path.join(data_path, file)
    print("파일: ", file_path)
    df = pd.read_csv(file_path, encoding="cp949", header=0, thousands=',', quotechar='"', skipinitialspace=True)
    df.columns = df.columns.str.strip()
    cols = ['고유역번호(외부역코드)', '위도', '경도']
    df2 = df[cols].copy()
    df2.columns = ['역번호','위도', '경도']
    print(df2)

    # 판다스 DF -> 스파크 DF 변환
    # Sdf = spark.createDataFrame(df2)


    # 테이블 생성 및 적재
    # spark = SparkSession.builder.appName("sooah").master("spark://192.168.0.201:7077").config("spark.driver.extraJavaOptions", "-Djdk.io.permissions.UseCanonCaches=false") \
    # .config("spark.executor.extraJavaOptions", "-Djdk.io.permissions.UseCanonCaches=false") \
    # .getOrCreate()

    # db_url = "jdbc:mysql://192.168.0.201:3306/seoul_metro"
    # db_properties = {
    # "user": "root",
    # "password": "1234",
    # "driver": "com.mysql.cj.jdbc.Driver"
    # }
    # Sdf.writhe.jdbc(
    #     url=db_url,
    #     table="coordinate",
    #     mode="overwrite",
    #     # 처음 테이블 생성 때는 overwrite, 추가할 땐 아래 append문
    #     properties=db_properties
    # )
    # # df.to_sql('coordinate', con=engine_mariadb, if_exists='append', index=False)

    # df = spark.read.csv(file_path, header=True, inferSchema=True)
    # print(df.count())
# df.printSchema()
# df.show()
# print('끝')