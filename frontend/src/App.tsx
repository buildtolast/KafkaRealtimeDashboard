import { useState, useMemo } from 'react';
import { useTopics } from './hooks/useTopics';
import { LayoutManager } from './components/LayoutManager';
import { TopicSelector } from './components/TopicSelector';
import { BrokerConfig } from './components/BrokerConfig';
import { SearchResultsPanel } from './components/SearchResultsPanel';

export default function App() {
  const { topics, loading, error, refetch } = useTopics();
  const [selectedTopics, setSelectedTopics] = useState<string[] | null>(null);
  const [searchQuery, setSearchQuery] = useState('');
  const [globalDateFilter, setGlobalDateFilter] = useState('');
  const [searchPanelOpen, setSearchPanelOpen] = useState(false);
  const [searchPanelQuery, setSearchPanelQuery] = useState('');

  function handleBack() {
    setSelectedTopics(null);
  }

  function handleBrokerChange() {
    setSelectedTopics(null);
    refetch();
  }

  function handleSearchKeyDown(e: React.KeyboardEvent) {
    if (e.key === 'Enter' && searchQuery.trim()) {
      setSearchPanelQuery(searchQuery.trim());
      setSearchPanelOpen(true);
    }
  }

  const globalDateFilterMs = useMemo(() => {
    if (!globalDateFilter) return undefined;
    const d = new Date(globalDateFilter);
    return isNaN(d.getTime()) ? undefined : d.getTime();
  }, [globalDateFilter]);

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
                placeholder="Search all topics… (Enter for results)"
                value={searchQuery}
                onChange={(e) => setSearchQuery(e.target.value)}
                onKeyDown={handleSearchKeyDown}
              />
              <div className="global-date-filter">
                <span className="date-filter-label">From:</span>
                <input
                  type="datetime-local"
                  step="1"
                  value={globalDateFilter}
                  onChange={(e) => setGlobalDateFilter(e.target.value)}
                  title="Global date filter (applies to all topics)"
                />
                {globalDateFilter && (
                  <button
                    className="bp5-button bp5-minimal bp5-small"
                    onClick={() => setGlobalDateFilter('')}
                    title="Clear global date filter"
                  >
                    ×
                  </button>
                )}
              </div>
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
          <LayoutManager
            topics={selectedTopics}
            searchQuery={searchQuery || undefined}
            globalDateFilterMs={globalDateFilterMs}
          />
        )}
      </main>
      {searchPanelOpen && selectedTopics && (
        <SearchResultsPanel
          query={searchPanelQuery}
          topics={selectedTopics}
          onClose={() => setSearchPanelOpen(false)}
        />
      )}
    </div>
  );
}
