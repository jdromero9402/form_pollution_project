import React, { useState, useEffect } from 'react';
import SearchSelect from './SearchSelect';

const API_URL = process.env.REACT_APP_API_URL

function StationsForm({ setView }) {
  const [zones, setZones] = useState([]);
  const [zoneQuery, setZoneQuery] = useState('');
  const [selectedZoneId, setSelectedZoneId] = useState(null);
  const [formData, setFormData] = useState({
    station_id: '',
    station_code: '',
    station_name: '',
    lat: '',
    lon: '',
    station_type: '',
    authority: '',
    years_min: '',
    years_max: ''
  });
  const [message, setMessage] = useState('');
  const [messageType, setMessageType] = useState('');

  useEffect(() => {
    const loadZones = async () => {
      try {
        const response = await fetch(`http://${API_URL}/api/v1/zones`);
        if (!response.ok) throw new Error('Error al cargar zonas');
        setZones(await response.json());
      } catch (error) {
        console.error('Error cargando zonas:', error);
      }
    };
    loadZones();
  }, []);

  const handleChange = (e) => {
    const { name, value } = e.target;
    setFormData({ ...formData, [name]: value });
  };

  const handleSubmit = async (e) => {
    e.preventDefault();
    const data = { ...formData, zone_id: selectedZoneId };
    const formDataObj = new FormData();
    Object.keys(data).forEach(key => {
        formDataObj.append(key, data[key]);
    });
    try {
      const response = await fetch(`http://${API_URL}/api/v1/stations/`, {
        method: 'POST',
        body: formDataObj
      });
      const result = await response.json();
      if (response.status === 201 && response.ok) {
        setMessage(result.message || 'Estación enviada exitosamente.');
        setMessageType('success');
        setFormData({
          station_id: '',
          station_code: '',
          station_name: '',
          lat: '',
          lon: '',
          station_type: '',
          authority: '',
          years_min: '',
          years_max: ''
        });
        setZoneQuery('');
        setSelectedZoneId(null);
      } else {
        setMessage(result.message || 'Error al enviar la estación.');
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
      <h2>Formulario de Estaciones</h2>
      <form onSubmit={handleSubmit}>
        <label>ID único de la estación (int):</label>
        <input type="number" name="station_id" value={formData.station_id} onChange={handleChange} required min="1" placeholder="Ej: 456" />

        <label>Código de la estación (str, max 50):</label>
        <input type="text" name="station_code" value={formData.station_code} onChange={handleChange} maxLength="50" required placeholder="Ej: STN001" />

        <label>Nombre de la estación (str, max 200):</label>
        <input type="text" name="station_name" value={formData.station_name} onChange={handleChange} maxLength="200" required placeholder="Ej: Estación Central" />

        <label>Zona (busca y selecciona):</label>
        <SearchSelect
          data={zones}
          query={zoneQuery}
          onQueryChange={setZoneQuery}
          onSelect={(zone) => { setSelectedZoneId(zone.zone_id); setZoneQuery(zone.zone_name); }}
          placeholder="Escribe para buscar zonas..."
          displayKey="zone_name"
          valueKey="zone_id"
        />
        <input type="hidden" name="zone_id" value={selectedZoneId || ''} />

        <label>Latitud (opcional, Decimal):</label>
        <input type="number" name="lat" value={formData.lat} onChange={handleChange} step="any" placeholder="Ej: 40.4168" />

        <label>Longitud (opcional, Decimal):</label>
        <input type="number" name="lon" value={formData.lon} onChange={handleChange} step="any" placeholder="Ej: -3.7038" />

        <label>Tipo de estación (opcional, str, max 100):</label>
        <input type="text" name="station_type" value={formData.station_type} onChange={handleChange} maxLength="100" placeholder="Ej: Meteorológica" />

        <label>Autoridad responsable (opcional, str, max 100):</label>
        <input type="text" name="authority" value={formData.authority} onChange={handleChange} maxLength="100" placeholder="Ej: Gobierno Local" />

        <label>Año mínimo de operación (opcional, int):</label>
        <input type="number" name="years_min" value={formData.years_min} onChange={handleChange} min="1900" max="2100" placeholder="Ej: 2000" />

        <label>Año máximo de operación (opcional, int):</label>
        <input type="number" name="years_max" value={formData.years_max} onChange={handleChange} min="1900" max="2100" placeholder="Ej: 2023" />

        <div className="buttons-container">
          <button type="submit">Enviar Estación</button>
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

export default StationsForm;