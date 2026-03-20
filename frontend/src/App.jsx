import { Routes, Route } from "react-router"
import NotFound from '@pages/NotFound.jsx'
import Maps from '@pages/Maps.jsx'
import Card from '@pages/Card.jsx'


const paths = [
  { path: "/map", element: <Maps/> },
  { path: "*", element: <NotFound /> },
  {path: "/", element: <Card />},
]


function App() {

  return (
    <>

      <Routes>
        {paths?.map((v, i) => <Route key={i} path={v.path} element={v.element} />)}
      </Routes>
    </>

  )
}


export default App