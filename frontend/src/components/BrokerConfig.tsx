import { useState, useEffect } from 'react';

interface BrokerConfigProps {
  onBrokerChange: () => void;
}

export function BrokerConfig({ onBrokerChange }: BrokerConfigProps) {
  const [brokers, setBrokers] = useState('');
  const [input, setInput] = useState('');
  const [saving, setSaving] = useState(false);
  const [error, setError] = useState<string | null>(null);

  useEffect(() => {
    fetch('/api/broker')
      .then((r) => r.json())
      .then((data) => {
        setBrokers(data.brokers);
        setInput(data.brokers);
      })
      .catch(() => {});
  }, []);

  async function handleSave() {
    if (!input.trim() || input.trim() === brokers) return;
    setSaving(true);
    setError(null);
    try {
      const res = await fetch('/api/broker', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ brokers: input.trim() }),
      });
      if (!res.ok) {
        const data = await res.json();
        throw new Error(data.error || `HTTP ${res.status}`);
      }
      const data = await res.json();
      setBrokers(data.brokers);
      setInput(data.brokers);
      onBrokerChange();
    } catch (e) {
      setError(e instanceof Error ? e.message : 'Failed to update broker');
    } finally {
      setSaving(false);
    }
  }

  function handleKeyDown(e: React.KeyboardEvent) {
    if (e.key === 'Enter') handleSave();
  }

  const changed = input.trim() !== brokers;

  return (
    <div className="broker-config">
      <label className="broker-label">Broker:</label>
      <input
        className="broker-input"
        type="text"
        value={input}
        onChange={(e) => setInput(e.target.value)}
        onKeyDown={handleKeyDown}
        placeholder="localhost:9092"
      />
      {changed && (
        <button
          className="bp5-button bp5-intent-primary bp5-small"
          onClick={handleSave}
          disabled={saving}
        >
          {saving ? 'Connecting...' : 'Connect'}
        </button>
      )}
      {error && <span className="broker-error">{error}</span>}
    </div>
  );
}
