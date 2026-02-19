import { useMemo } from 'react';
import { useMessageTracking } from '../contexts/MessageTrackingContext';
import { KafkaMessage } from '../types';

interface SearchResultsPanelProps {
  query: string;
  topics: string[];
  onClose: () => void;
}

function highlightInSearch(text: string, query: string): React.ReactNode {
  const idx = text.toLowerCase().indexOf(query.toLowerCase());
  if (idx === -1) return text;
  return (
    <>
      {text.slice(0, idx)}
      <mark className="search-highlight">{text.slice(idx, idx + query.length)}</mark>
      {text.slice(idx + query.length)}
    </>
  );
}

export function SearchResultsPanel({ query, topics, onClose }: SearchResultsPanelProps) {
  const tracking = useMessageTracking();

  const results = useMemo(() => {
    if (!tracking) return new Map<string, KafkaMessage[]>();
    const allMessages = tracking.getAllMessages();
    const filtered = new Map<string, KafkaMessage[]>();
    const q = query.toLowerCase();

    for (const topic of topics) {
      const msgs = allMessages.get(topic) ?? [];
      const matching = msgs.filter(
        (m) =>
          (m.payload && m.payload.toLowerCase().includes(q)) ||
          (m.key && m.key.toLowerCase().includes(q))
      );
      if (matching.length > 0) {
        filtered.set(topic, matching);
      }
    }
    return filtered;
  }, [tracking, query, topics]);

  const totalResults = Array.from(results.values()).reduce((sum, arr) => sum + arr.length, 0);

  return (
    <div className="search-panel-overlay" onClick={onClose}>
      <div className="search-panel" onClick={(e) => e.stopPropagation()}>
        <div className="search-panel-header">
          <h3>Search: &quot;{query}&quot;</h3>
          <span className="search-panel-count">{totalResults} matches</span>
          <button className="bp5-button bp5-minimal bp5-small" onClick={onClose}>
            Close
          </button>
        </div>
        <div className="search-panel-body">
          {results.size === 0 && (
            <div className="search-panel-empty">No results found.</div>
          )}
          {Array.from(results.entries()).map(([topic, msgs]) => (
            <div key={topic} className="search-topic-group">
              <div className="search-topic-header">
                {topic} ({msgs.length})
              </div>
              {msgs.map((msg) => (
                <div key={`${msg.partition}-${msg.offset}`} className="search-result-row">
                  <span className="search-result-meta">
                    P{msg.partition} #{msg.offset}
                    {msg.timestamp && ` \u2014 ${new Date(msg.timestamp).toLocaleTimeString()}`}
                  </span>
                  {msg.key && (
                    <span className="search-result-key">
                      Key: {highlightInSearch(msg.key, query)}
                    </span>
                  )}
                  <pre className="search-result-payload">
                    {highlightInSearch(msg.payload ?? '(empty)', query)}
                  </pre>
                </div>
              ))}
            </div>
          ))}
        </div>
      </div>
    </div>
  );
}
