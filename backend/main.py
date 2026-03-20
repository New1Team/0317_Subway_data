from pyspark.sql import SparkSession, Row
from sqlalchemy import create_engine, inspect
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
import pandas as pd
from settings import settings
import os
from routers import data, spark



app = FastAPI()

app.add_middleware(
  CORSMiddleware,
  allow_origins=["http://localhost:5173"],
  allow_credentials=True,
  allow_methods=["*"],
  allow_headers=["*"],
)

app.include_router(data.router)
app.include_router(spark.router)

@app.get("/")
def read_root():
  return {"status": True}


