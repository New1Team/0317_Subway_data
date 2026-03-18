from pyspark.sql import SparkSession, Row
from sqlalchemy import create_engine, inspect
from fastapi import FastAPI
import pandas as pd
from settings import settings
import os

app = FastAPI()

spark = None
engine_mariadb = create_engine(settings.mariadb_host)

@app.on_event("startup")
def startup_event():
  global spark
  try:
    jar_path = settings.jar_path
    spark = SparkSession.builder \
      .appName("뚜아") \
      .master(settings.spark_url) \
      .config("spark.jars", jar_path) \
      .config("spark.driver.host", settings.host_ip) \
      .config("spark.driver.bindAddress", "0.0.0.0") \
      .config("spark.driver.port", "10000") \
      .config("spark.blockManager.port", "10001") \
      .config("spark.cores.max", "2") \
      .config("spark.sql.sources.jdbc.driver.kind", "mariadb") \
      .config("spark.sql.dialect", "mysql") \
      .getOrCreate()
    print("성공!")
  except Exception as e:
    print(f"Failed to create Spark session: {e}")
  
@app.on_event("shutdown")
def shutdown_event():
  if spark:
    spark.stop()

@app.get("/")
def read_root():
  if not spark:
    return {"status": False, "error": "Spark session not initialized"}
  try:
    df = pd.read_csv(settings.file_dir, encoding="cp949", header=0, thousands=',', quotechar='"', skipinitialspace=True)
    spDf = spark.createDataFrame(df)    
    result = spDf.limit(50).toPandas().to_dict(orient="records")
    return {"status": True, "data": result}
  except Exception as e:
    return {"status": False, "error": str(e)}


@app.get('/sooah')
def read():
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
      df2.columns = ['station_id', 'latitude', 'longitude']
      print("df2:  ",df2)
      sDf = spark.createDataFrame(df2)
      
      # 테이블 생성 및 적재
      db_properties = {
      "user": "root",
      "password": "1234",
      "driver": "org.mariadb.jdbc.Driver",
      "sessionInitStatement": "SET SQL_MODE='ANSI_QUOTES'"
      }
      try:
        sDf.write.jdbc(
            url=f"{settings.db_url}?useUnicode=true&characterEncoding=UTF-8&sessionVariables=sql_mode='ANSI_QUOTES'",
            table="coordinate",
            mode="overwrite",
            # 처음 테이블 생성 때는 overwrite, 추가할 땐 아래 append문
            properties=db_properties
        )
        print(f"✅ {file} 적재 완료!")
      except Exception as e:
        print(f"😥 적재실패: {e}")

      check_df = spark.read.jdbc(settings.db_url, table="coordinate", properties=db_properties)
      print("개수 ", check_df.count())
      check_df.show()
