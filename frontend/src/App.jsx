import { Routes, Route } from "react-router"
import NotFound from '@pages/NotFound.jsx'
import Maps from '@pages/Maps.jsx'


const paths = [
  { path: "/", element: <Maps/> },
  { path: "*", element: <NotFound /> },
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