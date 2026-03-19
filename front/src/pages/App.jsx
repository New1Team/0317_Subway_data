import { BrowserRouter, Routes, Route } from "react-router";
import 'leaflet/dist/leaflet.css';
import NotFound  from "./NotFound.jsx"; 
import Card from "./Card.jsx";


const App = () => {
  const paths = [
    {path: "*", element: <NotFound />},
    {path: "map", element: <Map />},
    {path: "card", element: <Card />},
  ]
  return (
    <>
      <BrowserRouter>
        <Routes>
          { paths?.map((v, i) => <Route key={i} path={v.path} element={v.element} />) }
        </Routes>
      </BrowserRouter>
    </>
  )
}

export default App