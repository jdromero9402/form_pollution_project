import React, { useState, useEffect } from 'react';
import SearchSelect from './SearchSelect';

const API_URL = process.env.REACT_APP_API_URL

function CapabilitiesForm({ setView }) {
  const [stations, setStations] = useState([]);
  const [pollutants, setPollutants] = useState([]);
  const [stationQuery, setStationQuery] = useState('');
  const [pollutantQuery, setPollutantQuery] = useState('');
  const [selectedStationId, setSelectedStationId] = useState(null);
  const [selectedPollutantId, setSelectedPollutantId] = useState(null);
  const [formData, setFormData] = useState({ unit: '' });
  const [message, setMessage] = useState('');
  const [messageType, setMessageType] = useState('');

  useEffect(() => {
    const loadData = async () => {
      try {
        const [stationsRes, pollutantsRes] = await Promise.all([
          fetch(`http://${API_URL}/api/v1/stations`),
          fetch(`http://${API_URL}/api/v1/pollutants`)
        ]);
        if (!stationsRes.ok || !pollutantsRes.ok) throw new Error('Error al cargar datos');
        setStations(await stationsRes.json());
        setPollutants(await pollutantsRes.json());
      } catch (error) {
        console.error('Error cargando datos:', error);
      }
    };
    loadData();
  }, []);

  const handleChange = (e) => {
    const { name, value } = e.target;
    setFormData({ ...formData, [name]: value });
  };

  const handleSubmit = async (e) => {
    e.preventDefault();
    const data = { ...formData, station_id: selectedStationId, pollutant_id: selectedPollutantId };
    const formDataObj = new FormData();
    Object.keys(data).forEach(key => {
        formDataObj.append(key, data[key]);
    });
    try {
      const response = await fetch(`http://${API_URL}/api/v1/capabilities/`, {
        method: 'POST',
        body: formDataObj
      });
      const result = await response.json();
      if (response.status === 201 && response.ok) {
        setMessage(result.message || 'Capability enviada exitosamente.');
        setMessageType('success');
        setFormData({ unit: '' });
        setStationQuery('');
        setPollutantQuery('');
        setSelectedStationId(null);
        setSelectedPollutantId(null);
      } else {
        setMessage(result.message || 'Error al enviar la capability.');
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
      <h2>Formulario de Capabilities</h2>
      <form onSubmit={handleSubmit}>
        <label>Estación (busca y selecciona):</label>
        <SearchSelect
          data={stations}
          query={stationQuery}
          onQueryChange={setStationQuery}
          onSelect={(station) => { setSelectedStationId(station.station_id); setStationQuery(station.station_name); }}
          placeholder="Escribe para buscar estaciones..."
          displayKey="station_name"
          valueKey="station_id"
        />
        <input type="hidden" name="station_id" value={selectedStationId || ''} />

        <label>Contaminante (busca y selecciona):</label>
        <SearchSelect
          data={pollutants}
          query={pollutantQuery}
          onQueryChange={setPollutantQuery}
          onSelect={(pollutant) => { setSelectedPollutantId(pollutant.pollutant_id); setPollutantQuery(pollutant.name); }}
          placeholder="Escribe para buscar contaminantes..."
          displayKey="name"
          valueKey="pollutant_id"
        />
        <input type="hidden" name="pollutant_id" value={selectedPollutantId || ''} />

        <label>Unidad de medición (opcional, str, max 20):</label>
        <input type="text" name="unit" value={formData.unit} onChange={handleChange} maxLength="20" placeholder="Ej: µg/m³" />

        <div className="buttons-container">
          <button type="submit">Enviar Capability</button>
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

export default CapabilitiesForm;