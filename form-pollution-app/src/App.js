import React, { useState } from 'react';
import Home from './components/Home';
import MeasurementsForm from './components/MeasurementsForm';
import ZonesForm from './components/ZonesForm';
import StationsForm from './components/StationsForm';
import PollutantsForm from './components/PollutantsForm';
import CapabilitiesForm from './components/CapabilitiesForm';
import './styles.css';

function App() {
  const [currentView, setCurrentView] = useState('home');

  const renderView = () => {
    switch (currentView) {
      case 'measurements': return <MeasurementsForm setView={setCurrentView} />;
      case 'zones': return <ZonesForm setView={setCurrentView} />;
      case 'stations': return <StationsForm setView={setCurrentView} />;
      case 'pollutants': return <PollutantsForm setView={setCurrentView} />;
      case 'capabilities': return <CapabilitiesForm setView={setCurrentView} />;
      default: return <Home setView={setCurrentView} />;
    }
  };

  return (
    <div id="app">
      <div id="content">
        {renderView()}
      </div>
    </div>
  );
}

export default App;