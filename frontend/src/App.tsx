import { useState } from 'react';
import { useTopics } from './hooks/useTopics';
import { MosaicLayout } from './components/MosaicLayout';
import { TopicSelector } from './components/TopicSelector';
import { BrokerConfig } from './components/BrokerConfig';

export default function App() {
  const { topics, loading, error, refetch } = useTopics();
  const [selectedTopics, setSelectedTopics] = useState<string[] | null>(null);
  const [searchQuery, setSearchQuery] = useState('');

  function handleBack() {
    setSelectedTopics(null);
  }

  function handleBrokerChange() {
    setSelectedTopics(null);
    refetch();
  }

  return (
    <div className="app">
      <header className="app-header">
        <h1>Kafka Dashboard</h1>
        <div className="header-actions">
          <BrokerConfig onBrokerChange={handleBrokerChange} />
          {selectedTopics && (
            <>
              <input
                className="global-search"
                type="text"
                placeholder="Search all topics..."
                value={searchQuery}
                onChange={(e) => setSearchQuery(e.target.value)}
              />
              <button className="bp5-button bp5-minimal bp5-small" onClick={handleBack}>
                Change Topics
              </button>
            </>
          )}
          <span className="topic-count">{topics.length} topics</span>
          <button className="bp5-button bp5-minimal bp5-icon-refresh" onClick={refetch}>
            Refresh
          </button>
        </div>
      </header>
      <main className="app-main">
        {loading && <div className="loading">Connecting to Kafka...</div>}
        {error && (
          <div className="error">
            <p>Failed to connect: {error}</p>
            <button className="bp5-button bp5-intent-primary" onClick={refetch}>
              Retry
            </button>
          </div>
        )}
        {!loading && !error && topics.length === 0 && (
          <div className="empty">No topics found. Create some Kafka topics and refresh.</div>
        )}
        {!loading && !error && topics.length > 0 && !selectedTopics && (
          <TopicSelector topics={topics} onConfirm={setSelectedTopics} />
        )}
        {!loading && !error && selectedTopics && selectedTopics.length > 0 && (
          <MosaicLayout topics={selectedTopics} searchQuery={searchQuery || undefined} />
        )}
      </main>
    </div>
  );
}
