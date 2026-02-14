import React from 'react';
import ReactDOM from 'react-dom/client';
import App from './App';
import { MessageTrackingProvider } from './contexts/MessageTrackingContext';
import './styles/index.css';

ReactDOM.createRoot(document.getElementById('root')!).render(
  <React.StrictMode>
    <MessageTrackingProvider>
      <App />
    </MessageTrackingProvider>
  </React.StrictMode>
);
