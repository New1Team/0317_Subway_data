import { useNavigate } from "react-router"


const NotFound = () => {
  const nav = useNavigate()
  return (
    <div className="text-center">
      <h1>404</h1>
      <p>잘못된 접근입니다.</p>
      <button type="button" onClick={()=>nav('/')}>메인으로 돌아가기</button>
    </div>
  )
}

export default NotFound