from db import findAll
from fastapi import FastAPI
from fastapi import APIRouter

router = APIRouter()



@router.get('/kpi')
def get_data(year: int = 2021):
  sql='''SELECT s.`역명`, c.`위도`, c.`경도` FROM station_timeband_summary AS s 
        JOIN coordinate AS c
        ON (s.역번호 = c.`역번호`);
        WHERE year = {year};'''
  data = findAll(sql)
  return {'data': data}