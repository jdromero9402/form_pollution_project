import React, { useState, useEffect } from 'react';
import SearchSelect from './SearchSelect';

const API_URL = process.env.REACT_APP_API_URL

function StationsForm({ setView }) {
  const [zones, setZones] = useState([]);
  const [zoneQuery, setZoneQuery] = useState('');
  const [selectedZoneId, setSelectedZoneId] = useState(null);
  const [showMap, setShowMap] = useState(false);
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

  useEffect(() => {
    if (!showMap) return;

    // Cargar Leaflet CSS
    const link = document.createElement('link');
    link.rel = 'stylesheet';
    link.href = 'https://unpkg.com/leaflet@1.9.4/dist/leaflet.css';
    document.head.appendChild(link);

    // Cargar Leaflet JS
    const script = document.createElement('script');
    script.src = 'https://unpkg.com/leaflet@1.9.4/dist/leaflet.js';
    script.onload = initMap;
    document.body.appendChild(script);

    return () => {
      document.head.removeChild(link);
      document.body.removeChild(script);
    };
  }, [showMap]);

  const initMap = () => {
    if (!window.L) return;

    const map = window.L.map('map-selector').setView([4.6097, -74.0817], 6); // Colombia centro

    window.L.tileLayer('https://{s}.tile.openstreetmap.org/{z}/{x}/{y}.png', {
      attribution: '© OpenStreetMap contributors'
    }).addTo(map);

    let marker = null;

    map.on('click', (e) => {
      const { lat, lng } = e.latlng;
      
      if (marker) {
        map.removeLayer(marker);
      }
      
      marker = window.L.marker([lat, lng]).addTo(map);
      
      setFormData(prev => ({
        ...prev,
        lat: lat.toFixed(6),
        lon: lng.toFixed(6)
      }));
    });

    // Si ya hay coordenadas, mostrar marker
    if (formData.lat && formData.lon) {
      marker = window.L.marker([parseFloat(formData.lat), parseFloat(formData.lon)]).addTo(map);
      map.setView([parseFloat(formData.lat), parseFloat(formData.lon)], 10);
    }
  };

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

        <label>Código de la estación:</label>
        <input type="text" name="station_code" value={formData.station_code} onChange={handleChange} maxLength="50" required placeholder="Ej: STN001" />

        <label>Nombre de la estación:</label>
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

        <label>Ubicación Estación</label>
          <button 
            type="button" 
            onClick={() => setShowMap(!showMap)}
          >
            {showMap ? '🗺️ Cerrar Mapa' : '📍 Seleccionar en Mapa'}
          </button>
        <label>Latitud:</label>
        <div style={{ display: 'flex', gap: '10px', alignItems: 'center' }}>
          <input 
            type="number" 
            name="lat" 
            value={formData.lat} 
            onChange={handleChange} 
            step="any" 
            placeholder="Ej: 40.4168"
            style={{ flex: 1 }}
          />
        </div>

        <label>Longitud:</label>
        <input type="number" name="lon" value={formData.lon} onChange={handleChange} step="any" placeholder="Ej: -3.7038" />

        {showMap && (
          <div style={{ margin: '20px 0' }}>
            <div 
              id="map-selector" 
              style={{ 
                height: '400px', 
                width: '100%', 
                border: '2px solid #ccc',
                borderRadius: '8px'
              }}
            ></div>
            <p style={{ marginTop: '10px', fontSize: '14px', color: '#666' }}>
              Haz clic en el mapa para seleccionar las coordenadas
            </p>
          </div>
        )}

        <label>Tipo de estación:</label>
        <input type="text" name="station_type" value={formData.station_type} onChange={handleChange} maxLength="100" placeholder="Ej: Meteorológica" />

        <label>Autoridad responsable:</label>
        <input type="text" name="authority" value={formData.authority} onChange={handleChange} maxLength="100" placeholder="Ej: Gobierno Local" />

        <label>Año inicio de operación:</label>
        <input type="number" name="years_min" value={formData.years_min} onChange={handleChange} min="1900" max="2100" placeholder="Ej: 2000" />

        <label>Último año de operación:</label>
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