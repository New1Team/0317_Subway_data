import React, { useState, useEffect } from 'react';
import axios from 'axios';
import '../assets/Dashboard.css';
import { Map, MapMarker } from 'react-kakao-maps-sdk';

const Card = () => {
  // --- 상태 관리 ---
  const [selectedYear, setSelectedYear] = useState(2021);
  const [markers, setMarkers] = useState([]); 
  const [map, setMap] = useState(null);
  const [info, setInfo] = useState(null); // 마커 클릭 시 정보 저장
  const [loading, setLoading] = useState(true);
  const [currCategory, setCurrCategory] = useState(""); // 현재 선택된 카테고리 (BK9, CS2 등)
  const [kpiData, setKpiData] = useState({
    commute: { station: "-", count: 0 },
    weekday: { station: "-", count: 0 },
    weekend: { station: "-", count: 0 }
  });

  // --- 헬퍼 함수: 카테고리별 마커 이미지 위치 계산 ---
  const getCategoryOrder = (category) => {
    const orders = { "BK9": 0, "MT1": 1, "PM9": 2, "OL7": 3, "CE7": 4, "CS2": 5 };
    return orders[category] || 0;
  };

  // --- 데이터 호출: 연도가 바뀔 때마다 실행 ---
  useEffect(() => {
    const fetchData = async () => {
      setLoading(true);
      try {
        const [mapRes, kpiRes] = await Promise.all([
          axios.get(`http://localhost:8000/api/subway/stats?year=${selectedYear}`),
          axios.get(`http://localhost:8000/api/subway/kpi?year=${selectedYear}`)
        ]);

        // 데이터 가공 및 상태 저장
        const formatData = mapRes.data.map((item, index) => ({
          id: item.id || `subway-${index}`,
          lat: item.lat || item.위도,
          lng: item.lng || item.경도,
          title: item.station_nm || item.역명,
          ...item
        }));

        setMarkers(formatData);
        setKpiData(kpiRes.data);
      } catch (error) {
        console.error("데이터 로드 실패:", error);
      } finally {
        setLoading(false);
      }
    };

    fetchData();
  }, [selectedYear]);

  return (
    <div className="dashboard-wrapper">
      {/* 1. 연도 선택 드롭다운 */}
      <div className="center-section">
        <select 
          className="dropdown" 
          value={selectedYear} 
          onChange={(e) => setSelectedYear(Number(e.target.value))}
        >
          {Array.from({ length: 2021 - 2008 + 1 }, (_, i) => 2008 + i).map((year) => (
            <option key={year} value={year}>{year}년</option>
          ))}
        </select>
      </div>

      {/* 2. 핵심 지표 (KPI) 카드 섹션 */}
      <div className="kpi-grid-3">
        <div className="box kpi-card">
          <span className='kpi-title'>출근시간 최다 승하차</span>
          <div className='kpi-value'>{kpiData.commute.station}</div>
          <span className='kpi-sub'>{kpiData.commute.count.toLocaleString()}명</span>
        </div>  
        <div className="box kpi-card">
          <span className='kpi-title'>퇴근시간 최다 승하차</span>
          <div className='kpi-value'>{kpiData.weekday.station}</div>
          <span className='kpi-sub'>{kpiData.weekday.count.toLocaleString()}명</span>
        </div>
        <div className="box kpi-card">
          <span className='kpi-title'>주말 최다 승하차</span>
          <div className='kpi-value'>{kpiData.weekend.station}</div>
          <span className='kpi-sub'>{kpiData.weekend.count.toLocaleString()}명</span>
        </div>
      </div>

      {/* 3. 지도 시각화 섹션 */}
      <div className="box map-section">
        <Map 
          center={{ lat: 37.5665, lng: 126.9780 }} 
          style={{ width: '100%', height: '450px' }} 
          level={7}
          onCreate={setMap}
        >
          {!loading && markers.map((marker) => (
            <MapMarker  
              key={`marker-${marker.id}`}
              position={{ lat: Number(marker.lat), lng: Number(marker.lng) }}
              onClick={() => setInfo(marker)}
              title={marker.title}
              image={{
                src: "https://t1.daumcdn.net/localimg/localimages/07/mapapidoc/places_category.png",
                size: { width: 27, height: 28 },
                options: {
                  spriteSize: { width: 72, height: 208 },
                  // currCategory 값에 따라 아이콘 위치가 바뀜
                  spriteOrigin: { x: 46, y: getCategoryOrder(currCategory) * 36 },
                  offset: { x: 11, y: 28 },
                },
              }}
            />
          ))}
        </Map>
        {loading && <div className="loading-overlay">데이터 분석 중...</div>}
      </div>

      {/* 4. 데이터 하단 설명 바 */}
      <div className="box description-bar">
        정보: 주거지 | 업무지구 | 여가지역 분석 (Spark ETL 기반 데이터)
      </div>

      {/* 5. 비즈니스 광고 전략 가이드 */}
      <div className="box ad-plan-container">
        <div className="plan-section">
          <h4>(평일) 직장인 타겟 광고</h4>
          <ul>
            <li>출근시간-하차 집중역 <span className="highlight">TOP 3</span></li>
            <li>퇴근시간-승차 집중역 <span className="highlight">TOP 3</span></li>
          </ul>
        </div>
        <div className="plan-section">
          <h4>(주말) 여가/쇼핑 타겟 광고</h4>
          <ul>
            <li>유동인구 최고점 <span className="highlight">상위 3개 노선</span></li>
          </ul>
        </div>
      </div>
    </div>
  );
}

export default Card;