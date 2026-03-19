from pyspark.sql import SparkSession, Row
from sqlalchemy import create_engine, inspect
from fastapi import FastAPI
import pandas as pd
from settings import settings
import os
from pydantic import BaseModel
from typing import List, Dict

@app.get("/api/stations")
def get_stations():
    try:
        # SQLAlechemy engine을 사용하여 MariaDB에서 데이터를 읽어옵니다.
        # 테이블명이 'coordinate'라고 가정했습니다.
        query = "SELECT * FROM coordinate"
        df = pd.read_sql(query, engine_mariadb)
        
        # DataFrame을 리액트가 읽기 쉬운 JSON(배열) 형태로 변환하여 반환
        return df.to_dict(orient="records")
    except Exception as e:
        return {"error": str(e)}
@app.get("/api/get_map_data")
def get_map_data():
    try:
        # engine_mariadb는 이미 상단에 선언되어 있으므로 그대로 사용
        query = "SELECT * FROM coordinate"
        df = pd.read_sql(query, engine_mariadb)
        
        # 데이터를 리스트 형태로 변환
        return df.to_dict(orient="records")
    except Exception as e:
        return {"error": str(e)}

spark = None
engine_mariadb = create_engine(settings.mariadb_host)
# Base Model

# 파일별 컬럼 매핑 모델
class ColsMapping(BaseModel):
  # 원본 컬럼명
  source_col:str
  # 변경할 컬럼명
  target_col:str
# 파일명, 매핑리스트 모델
class FileList(BaseModel):
  file: Dict[str, List[ColsMapping]]

# API 함수
@app.on_event("startup")
def startup_event():
  global spark
  try:
    jar_path = settings.jar_path
    spark = SparkSession.builder \
      .appName("hayong") \
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
  return {"status": True}

# 파일마다 컬럼 정할 수 있게 만들었습니다.
# 사용방법은 md로 넣어둘게요.
@app.post('/file_upload')
def read(fileCon: FileList):
  current_path = os.path.dirname(os.path.abspath(__file__))
  data_path = os.path.join(current_path, "data")
  # all_files = os.listdir(data_path)
  for file_name, mappings in fileCon.file.items():
    file_path = os.path.join(data_path, file_name)
    if not os.path.exists(file_path):
      continue
    print(f"✅ 처리 중인 파일: {file_path}")

  df = pd.read_csv(file_path, encoding="utf8", header=0, thousands=',', quotechar='"', skipinitialspace=True)
  df.columns = df.columns.str.strip()
  cols = [m.source_col for m in mappings]
  newCols = {m.source_col: m.target_col for m in mappings}
  df2 = df[cols].copy()
  df2 = df2.rename(columns=newCols)
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
        table="holiday_check",
        mode="overwrite",
        # 처음 테이블 생성 때는 overwrite, 추가할 땐 아래 append문
        properties=db_properties
    )
    print(f"✅ {file_name} 적재 완료!")
  except Exception as e:
    print(f"😥 적재실패: {e}")

  check_df = spark.read.jdbc(settings.db_url, table="coordinate", properties=db_properties)
  print("개수 ", check_df.count())
  check_df.show()
  return {'message': '적재 성공✨'}

@app.get("/api/metro_seoul/coordinate")
async def get_coordinate():
    # 여기에 DB(MariaDB)에서 데이터를 가져오는 로직이 들어가야 합니다.
    return [
        {"station_nm": "서울역", "lat": 37.5547, "lng": 126.9706},
        {"station_nm": "시청역", "lat": 37.5657, "lng": 126.9769}
    ]