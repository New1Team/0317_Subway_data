from pyspark.sql import SparkSession, Row
from sqlalchemy import create_engine, inspect
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
import pandas as pd
from settings import settings
import os
from pydantic import BaseModel
from typing import List, Dict
from data import router as data_router



app = FastAPI()

app.add_middleware(
  CORSMiddleware,
  allow_origins=["http://localhost:5173"],
  allow_credentials=True,
  allow_methods=["*"],
  allow_headers=["*"],
)
app.include_router(data_router)

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

  df = pd.read_csv(file_path, encoding="cp949", header=0, thousands=',', quotechar='"', skipinitialspace=True)
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
        table="coordinate",
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

