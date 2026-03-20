from db import findAll
from fastapi import APIRouter

router = APIRouter(
    prefix="/data",
    tags=["data"]
)

@router.get('')
def get_data(year: int):
  sql=f'''SELECT s.`역명`, c.`위도`, c.`경도` FROM station_timeband_summary AS s 
        JOIN coordinate AS c
        ON (s.역번호 = c.`역번호`)
        WHERE s.src_year = {year};'''
  data = findAll(sql)
  return {'data': data}

# @app.get("/api/metro_seoul/coordinate")
# async def get_coordinate():
#     # 여기에 DB(MariaDB)에서 데이터를 가져오는 로직이 들어가야 합니다.
#     return [
#         {"station_nm": "서울역", "lat": 37.5547, "lng": 126.9706},
#         {"station_nm": "시청역", "lat": 37.5657, "lng": 126.9769}
#     ]

# @app.get("/api/stations")
# def get_stations():
#     try:
#         # SQLAlechemy engine을 사용하여 MariaDB에서 데이터를 읽어옵니다.
#         # 테이블명이 'coordinate'라고 가정했습니다.
#         query = "SELECT * FROM coordinate"
#         df = pd.read_sql(query, engine_mariadb)
        
#         # DataFrame을 리액트가 읽기 쉬운 JSON(배열) 형태로 변환하여 반환
#         return df.to_dict(orient="records")
#     except Exception as e:
#         return {"error": str(e)}

# @app.get("/api/get_map_data")
# def get_map_data():
#     try:
#         # engine_mariadb는 이미 상단에 선언되어 있으므로 그대로 사용
#         query = "SELECT * FROM coordinate"
#         df = pd.read_sql(query, engine_mariadb)
        
#         # 데이터를 리스트 형태로 변환
#         return df.to_dict(orient="records")
#     except Exception as e:
#         return {"error": str(e)}