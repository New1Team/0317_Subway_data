from pyspark.sql import SparkSession, Row
from fastapi import FastAPI
import pandas as pd
from settings import settings

app = FastAPI()

spark = None

@app.on_event("startup")
def startup_event():
  global spark
  try:
    spark = SparkSession.builder \
      .appName("Sooah") \
      .master(settings.spark_url) \
      .config("spark.driver.host", settings.host_ip) \
      .config("spark.driver.bindAddress", "0.0.0.0") \
      .config("spark.driver.port", "10000") \
      .config("spark.blockManager.port", "10001") \
      .config("spark.cores.max", "2") \
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
