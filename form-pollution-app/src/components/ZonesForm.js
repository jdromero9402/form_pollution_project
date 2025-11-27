import React, { useState } from 'react';

const API_URL = process.env.REACT_APP_API_URL

function ZonesForm({ setView }) {
  const [formData, setFormData] = useState({
    zone_id: '',
    city_code: '',
    city_name: '',
    zone_code: '',
    zone_name: '',
    population: '',
    centroid_lat: '',
    centroid_lon: ''
  });
  const [message, setMessage] = useState('');
  const [messageType, setMessageType] = useState('');

  const handleChange = (e) => {
    const { name, value } = e.target;
    setFormData({ ...formData, [name]: value });
  };

  const handleSubmit = async (e) => {
    e.preventDefault();
    const formDataObj = new FormData();
    Object.keys(formData).forEach(key => {
        formDataObj.append(key, formData[key]);
    });
    try {
      const response = await fetch(`http://${API_URL}/api/v1/zones/`, {
        method: 'POST',
        body: formDataObj
      });
      const result = await response.json();
      
      if (response.status === 201 && response.ok) {
        setMessage(result.message || 'Zona enviada exitosamente.');
        setMessageType('success');
        setFormData({
          zone_id: '',
          city_code: '',
          city_name: '',
          zone_code: '',
          zone_name: '',
          population: '',
          centroid_lat: '',
          centroid_lon: ''
        });
      } else {
        setMessage(result.message || 'Error al enviar la zona.');
        setMessageType('error');
      }
    } catch (error) {
      setMessage('Error en el servidor. Inténtalo de nuevo.');
      setMessageType('error');
    }
    setTimeout(() => setMessage(''), 5000);
  };

  return (
    <>
      <h2>Formulario de Zonas</h2>
      <form onSubmit={handleSubmit}>
        <label>ID único de la zona:</label>
        <input type="number" name="zone_id" value={formData.zone_id} onChange={handleChange} required min="1" placeholder="Ej: 123" />

        <label>Código de la ciudad:</label>
        <input type="text" name="city_code" value={formData.city_code} onChange={handleChange} maxLength="10" required placeholder="Ej: MAD" />

        <label>Nombre de la ciudad:</label>
        <input type="text" name="city_name" value={formData.city_name} onChange={handleChange} maxLength="100" required placeholder="Ej: Madrid" />

        <label>Código de la zona:</label>
        <input type="text" name="zone_code" value={formData.zone_code} onChange={handleChange} maxLength="50" placeholder="Ej: Z001" />

        <label>Nombre de la zona:</label>
        <input type="text" name="zone_name" value={formData.zone_name} onChange={handleChange} maxLength="100" placeholder="Ej: Centro" />

        <label>Población:</label>
        <input type="number" name="population" value={formData.population} onChange={handleChange} min="0" placeholder="Ej: 50000" />

        <label>Latitud del centroide:</label>
        <input type="number" name="centroid_lat" value={formData.centroid_lat} onChange={handleChange} step="any" placeholder="Ej: 40.4168" />

        <label>Longitud del centroide:</label>
        <input type="number" name="centroid_lon" value={formData.centroid_lon} onChange={handleChange} step="any" placeholder="Ej: -3.7038" />

        <div className="buttons-container">
          <button type="submit">Enviar Zona</button>
          <button type="button" onClick={() => setView('home')}>Volver a Inicio</button>
        </div>
      </form>
      {message && (
        <div className={messageType === 'success' ? 'success-message' : 'error-message'}>
          {message}
        </div>
      )}
    </>
  );
}

export default ZonesForm;