import { useEffect, useState } from "react";
import { Map, MapMarker } from "react-kakao-maps-sdk";
import { api } from '@utils/network.js'

const Maps = ({year}) => {
  const [map, setMap] = useState(null); // 지도 객체 저장
  const [info, setInfo] = useState(null); // 클릭한 마커의 상세 정보
  const [markers, setMarkers] = useState([]); // 검색된 마커 리스트
  const [currCategory, setCurrCategory] = useState(""); // 현재 선택된 카테고리


  // 스프라이트 이미지 순서 계산기 (카카오 예제 기준)
  const getCategoryOrder = (category) => {
    const orders = { "BK9": 0, "MT1": 1, "PM9": 2, "OL7": 3, "CE7": 4, "CS2": 5 };
    return orders[category] || 0;
  }

  // 카테고리 변경 시 실행
  useEffect(() => {
    if (!map|| !currCategory) {
      setMarkers([])
      return
    }

    // 카테고리로 장소 검색 (현재 지도 바운드 내에서)
    // 여기 data = db에서 불러오기
    api.get('/data',{params:{year, category: currCategory}})
      .then(res => {
        const mapData = res.data.data
        const formatData = mapData.map((item, index) => ({
          id: `subway-${index}`,
          lat: item.위도,
          lng: item.경도,
          title: item.역명,
          ...item
        })
        )
        setMarkers(formatData)
      })

  }, [map, currCategory, year])

  // 카테고리 버튼 클릭 함수
  const handleCategoryClick = (id) => {
    setInfo(null);
    if (currCategory === id) {
      setCurrCategory(""); // 이미 선택된 거면 해제
    } else {
      setCurrCategory(id);
    }
  };

  return (
    <div className='kakaoMap'>
      {/* 지도 영역 */}
      <Map
        center={{ lat: 37.5547, lng: 126.9708 }} // 지금은 센터 서울역
        style={{ width: "100%", height: "100%" }}
        level={5}
        onCreate={setMap}
        onIdle={(map) => {
          if (currCategory) setMap(map);
        }}
      >
        {/* 검색된 마커들을 뿌려줌 */}
        {markers.map((marker) => (
          <MapMarker
            key={`marker-${marker.id}`}
            // 이게 지금 마커 위치 찍어주는 코드. 이걸 바꿔야함
            position={{ lat: marker.lat, lng: marker.lng }}
            onClick={() => setInfo(marker)} // 마커 클릭 시 정보 저장
            image={{
              src: "https://t1.daumcdn.net/localimg/localimages/07/mapapidoc/places_category.png",
              //마커 바꾸려면 이거⬆ 바꿔야함
              size: { width: 27, height: 28 }, //이건 마커 사이즈따라 바꾸면 됨
              options: {
                // 이미지 한 페이지에 다들어있어서 그거 나누는 코드,, 이걸 어케 바꾸지...
                spriteSize: { width: 72, height: 208 },
                spriteOrigin: { x: 46, y: getCategoryOrder(currCategory) * 36 },
                // 이미지 중 핀 위치
                offset: { x: 11, y: 28 },
              },
            }}
          />
        ))}

        {/* 마커 클릭 시 나타나는 장소 info 세팅 */}
        {/* 이건 없어도 됨 */}
        {info && (
          <MapMarker position={{ lat: info.lat, lng: info.lng }}>
            <div className="placeinfo_wrap" style={{ backgroundColor: "#FE9EC7", padding: "10px", color: '#FFFBF1', borderRadius: "5px", border: "1px solid #ccc" }}>
              <div className="placeinfo">
                <a className="title" href={info.place_url} target="_blank" rel="noreferrer" style={{ fontWeight: "bold", display: "block" }}>
                  {info.place_name}
                </a>
                {info.road_address_name ? (
                  <span title={info.road_address_name}>{info.road_address_name}</span>
                ) : (
                  <span>{info.address_name}</span>
                )}
                <span className="tel" style={{ color: "green", display: "block" }}>{info.phone}</span>
              </div>
            </div>
          </MapMarker>
        )}
      </Map>

      {/* 카테고리 선택 버튼들 */}
      {/* 꾸미는 건 ui에서 */}
      <div style={{ position: "absolute", top: "10px", left: "10px", zIndex: 10, display: "flex", gap: "5px" }}>
        {["MT1", "CS2", "PS3", "SC4", "PK6", "OL7", "SW8", "BK9", "CT1", "AG2", "AD5", "FD6", "CE7", "HP8", "PM9"].map((cat) => (
          <button
            key={cat}
            onClick={() => handleCategoryClick(cat)}
            style={{
              padding: "5px 10px",
              backgroundColor: currCategory === cat ? "blue" : "white",
              color: currCategory === cat ? "white" : "black",
              cursor: "pointer",
              border: "1px solid #ccc"
            }}
          >
            {cat}
          </button>
        ))}
      </div>
    </div>
  )
}

export default Maps