# file_upload 함수 사용시 넣을 값
```
{
  "file": {
    "파일명.확장자명": [
      {"source_col": "실제파일 컬럼명", "target_col": "바꿀 컬럼명"}
    ]
  }
}
```
- 파일 개수만큼 `file` 만들기
- 뽑아올 컬럼만큼 `source_col`, `target_col` 만들기

# 예시
```
{
  "file": {
    "subway_data.csv": [
      {"source_col": "고유역번호(외부역코드)", "target_col": "station_id"},
      {"source_col": "위도", "target_col": "latitude"},
      {"source_col": "경도", "target_col": "longitude"}
    ],
    "bus_stop.csv": [
      {"source_col": "ARS_ID", "target_col": "station_id"},
      {"source_col": "Y좌표", "target_col": "latitude"},
      {"source_col": "X좌표", "target_col": "longitude"},
      {"source_col": "정류소명", "target_col": "name"} 
    ]
  }
}
```