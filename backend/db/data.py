from db import findAll
from fastapi import FastAPI
from fastapi import APIRouter

router = APIRouter()



@router.get('/data')
def get_data():
  sql='''SELECT s.`역명`, c.`위도`, c.`경도` FROM station_timeband_summary AS s 
        JOIN coordinate AS c
        ON (s.역번호 = c.`역번호`);'''
  data = findAll(sql)
  return {'data': data}