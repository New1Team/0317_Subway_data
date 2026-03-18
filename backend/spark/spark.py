from sqlalchemy import create_engine, inspect
from pyspark.sql import SparkSession

engine_mariadb = create_engine('mysql+pymysql://root:1234@192.168.0.201:3306/seoul_metro')
spark = SparkSession.builder.appName("DB").master("spark://master:7077").getOrCreate()

# 생성된 테이블 확인
def table_check():
    inspector = inspect(engine_mariadb)
    tables = inspector.get_table_names()
    print(tables)

# 