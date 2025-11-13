import React, { useState, useEffect } from 'react';

function SearchSelect({ data, query, onQueryChange, onSelect, placeholder, displayKey, valueKey }) {
  const [filteredData, setFilteredData] = useState([]);
  const [showSuggestions, setShowSuggestions] = useState(false);

  useEffect(() => {
    if (query.length > 0) {
      const filtered = data.filter(item =>
        item[displayKey].toLowerCase().includes(query.toLowerCase())
      );
      setFilteredData(filtered);
      setShowSuggestions(filtered.length > 0);
    } else {
      setFilteredData([]);
      setShowSuggestions(false);
    }
  }, [query, data, displayKey]);

  const handleSelect = (item) => {
    onSelect(item);
    setShowSuggestions(false);
  };

  return (
    <div style={{ position: 'relative' }}>
      <input
        type="text"
        value={query}
        onChange={(e) => onQueryChange(e.target.value)}
        onFocus={() => setShowSuggestions(filteredData.length > 0)}
        onBlur={() => setTimeout(() => setShowSuggestions(false), 200)}
        placeholder={placeholder}
        required
      />
      {showSuggestions && (
        <ul className="suggestions-list">
          {filteredData.map(item => (
            <li key={item[valueKey]} onClick={() => handleSelect(item)}>
              {item[displayKey]}
            </li>
          ))}
        </ul>
      )}
    </div>
  );
}

export default SearchSelect;