import React from 'react';

function Home({ setView }) {
  const API_URL = process.env.REACT_APP_API_URL

  return (
    <>
      <h1>Proyecto Contaminantes</h1>
      <p>Bienvenido. Haz clic para agregar información.</p>
      <button onClick={() => setView('zones')}>Gestionar Zonas</button>
      <button onClick={() => setView('stations')}>Gestionar Estaciones</button>
      <button onClick={() => setView('pollutants')}>Gestionar Contaminantes</button>
      <button onClick={() => setView('measurements')}>Agregar Medición</button>
      <button onClick={() => setView('capabilities')}>Agregar Capability</button>
    </>
  );
}

export default Home;