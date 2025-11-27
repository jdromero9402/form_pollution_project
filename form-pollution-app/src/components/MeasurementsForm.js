import React, { useState, useEffect } from 'react';
import SearchSelect from './SearchSelect';

const API_URL = process.env.REACT_APP_API_URL

function MeasurementsForm({ setView }) {
  const [stations, setStations] = useState([]);
  const [pollutants, setPollutants] = useState([]);
  const [stationQuery, setStationQuery] = useState('');
  const [pollutantQuery, setPollutantQuery] = useState('');
  const [selectedStationId, setSelectedStationId] = useState(null);
  const [selectedPollutantId, setSelectedPollutantId] = useState(null);
  const [formData, setFormData] = useState({ ts_utc: '', value: '', unit: '', source: 'synthetic_projected', is_valid: true });
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

  const handleSubmit = async (e) => {
    e.preventDefault();
    const data = { ...formData, station_id: selectedStationId, pollutant_id: selectedPollutantId };
    const formDataObj = new FormData();
    Object.keys(data).forEach(key => {
      formDataObj.append(key, data[key]);
    });
    try {
      const response = await fetch(`http://${API_URL}/api/v1/measurements/`, {
        method: 'POST',
        body: formDataObj
      });
      const result = await response.json();
      if (response.status === 201 && response.ok) {
        setMessage(result.message || 'Medición enviada exitosamente.');
        setMessageType('success');
        // Reset form
        setFormData({ ts_utc: '', value: '', unit: '', source: '', is_valid: true });
        setStationQuery('');
        setPollutantQuery('');
        setSelectedStationId(null);
        setSelectedPollutantId(null);
      } else {
        setMessage(result.message || 'Error al enviar la medición.');
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
      <h1>Formulario de Mediciones</h1>
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

        <label>Fecha Medición:</label>
        <input type="datetime-local" value={formData.ts_utc} onChange={(e) => setFormData({ ...formData, ts_utc: e.target.value })} required />

        <label>Valor medido (Decimal):</label>
        <input type="number" step="any" value={formData.value} onChange={(e) => setFormData({ ...formData, value: e.target.value })} />


        <label>Unidad :</label>
        <select value={formData.unit} onChange={(e) => setFormData({ ...formData, unit: e.target.value })} required>
          <option value="">Seleccione unidad</option>
          <option value="mg/m³">mg/m³</option>
          <option value="µg/m³">µg/m³</option>
        </select>

        <label>Fuente de datos:</label>
        <input type="text" maxLength="50" value={formData.source} readOnly />
        {/* <input type="text" maxLength="50" value="synt hetic_projected" onChange={(e) => setFormData({ ...formData, source: e.target.value })} /> */}

        <label>¿Es válida la medición?</label>
        <input type="checkbox" checked={formData.is_valid} onChange={(e) => setFormData({ ...formData, is_valid: e.target.checked })} />

        <div className="buttons-container">
          <button type="submit">Enviar Medición</button>
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

export default MeasurementsForm;