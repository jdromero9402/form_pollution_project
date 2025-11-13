import React, { useState } from 'react';

const API_URL = process.env.REACT_APP_API_URL

function PollutantsForm({ setView }) {
  const [formData, setFormData] = useState({
    pollutant_id: '',
    name: '',
    default_unit: '',
    who_daily_limit: '',
    who_annual_limit: ''
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
      const response = await fetch(`http://${API_URL}/api/v1/pollutants/`, {
        method: 'POST',
        body: formDataObj
      });
      const result = await response.json();
      console.log(">>result",result);
      
      if (response.status === 201 && response.ok) {
      
        setMessage(result.message || 'Contaminante enviado exitosamente.');
        setMessageType('success');
        setFormData({
          pollutant_id: '',
          name: '',
          default_unit: '',
          who_daily_limit: '',
          who_annual_limit: ''
        });
      } else {
        setMessage(result.message || 'Error al enviar el contaminante.');
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
      <h2>Formulario de Contaminantes</h2>
      <form onSubmit={handleSubmit}>
        <label>ID del contaminante (str):</label>
        <input type="text" name="pollutant_id" value={formData.pollutant_id} onChange={handleChange} required placeholder="Ej: NO2" />

        <label>Nombre (str):</label>
        <input type="text" name="name" value={formData.name} onChange={handleChange} required placeholder="Ej: Dióxido de Nitrógeno" />

        <label>Unidad por defecto (opcional, str):</label>
        <input type="text" name="default_unit" value={formData.default_unit} onChange={handleChange} placeholder="Ej: µg/m³" />

        <label>Límite diario WHO (opcional, Decimal):</label>
        <input type="number" name="who_daily_limit" value={formData.who_daily_limit} onChange={handleChange} step="any" placeholder="Ej: 40.0" />

        <label>Límite anual WHO (opcional, Decimal):</label>
        <input type="number" name="who_annual_limit" value={formData.who_annual_limit} onChange={handleChange} step="any" placeholder="Ej: 20.0" />

        <div className="buttons-container">
          <button type="submit">Enviar Contaminante</button>
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

export default PollutantsForm;