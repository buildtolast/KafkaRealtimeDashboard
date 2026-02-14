import { useState, useMemo } from 'react';
import { useKafkaStream } from '../hooks/useKafkaStream';
import { MessageList } from './MessageList';
import { ConnectionStatus } from './ConnectionStatus';
import { KafkaMessage } from '../types';

interface TopicWindowProps {
  topic: string;
  searchQuery?: string;
  globalDateFilterMs?: number;
}

function exportToCsv(messages: KafkaMessage[], topic: string) {
  const header = 'topic,partition,offset,key,value,timestamp\n';
  const rows = messages.map((m) => {
    const key = (m.key ?? '').replace(/"/g, '""');
    const payload = (m.payload ?? '').replace(/"/g, '""');
    const ts = m.timestamp ? new Date(m.timestamp).toISOString() : '';
    return `"${m.topic}",${m.partition},${m.offset},"${key}","${payload}","${ts}"`;
  }).join('\n');
  const blob = new Blob([header + rows], { type: 'text/csv' });
  const url = URL.createObjectURL(blob);
  const a = document.createElement('a');
  a.href = url;
  a.download = `${topic}-messages-${Date.now()}.csv`;
  a.click();
  URL.revokeObjectURL(url);
}

export function TopicWindow({ topic, searchQuery, globalDateFilterMs }: TopicWindowProps) {
  const { messages, connected, paused, clear, togglePause, seekToTimestamp } = useKafkaStream(topic);
  const [seekInput, setSeekInput] = useState('');
  const [localDateFilter, setLocalDateFilter] = useState('');

  function handleSeek() {
    if (!seekInput.trim()) return;
    const date = new Date(seekInput);
    if (isNaN(date.getTime())) return;
    seekToTimestamp(date.getTime());
  }

  function handleSeekKey(e: React.KeyboardEvent) {
    if (e.key === 'Enter') handleSeek();
  }

  // Compute effective date filter: local overrides global
  const effectiveDateFilterMs = useMemo(() => {
    if (localDateFilter) {
      const d = new Date(localDateFilter);
      return isNaN(d.getTime()) ? undefined : d.getTime();
    }
    return globalDateFilterMs;
  }, [localDateFilter, globalDateFilterMs]);

  const filteredMessages = useMemo(() => {
    let result = messages;

    // Apply date filter
    if (effectiveDateFilterMs) {
      result = result.filter((m) => m.timestamp && m.timestamp >= effectiveDateFilterMs);
    }

    // Apply search filter
    if (searchQuery) {
      const q = searchQuery.toLowerCase();
      result = result.filter(
        (m) =>
          (m.payload && m.payload.toLowerCase().includes(q)) ||
          (m.key && m.key.toLowerCase().includes(q))
      );
    }

    return result;
  }, [messages, searchQuery, effectiveDateFilterMs]);

  return (
    <div className="topic-window">
      <div className="topic-toolbar">
        <span className="topic-window-name">{topic}</span>
        <ConnectionStatus connected={connected} />
        <button
          className={`bp5-button bp5-minimal bp5-small ${paused ? 'bp5-intent-warning' : ''}`}
          onClick={togglePause}
          title={paused ? 'Resume live messages' : 'Pause live messages'}
        >
          {paused ? 'Play' : 'Pause'}
        </button>
        <span className="message-count">{filteredMessages.length} messages</span>
        <button className="bp5-button bp5-minimal bp5-small" onClick={clear}>
          Clear
        </button>
        <button
          className="bp5-button bp5-minimal bp5-small"
          onClick={() => exportToCsv(filteredMessages, topic)}
          title="Export to CSV"
        >
          Export CSV
        </button>
      </div>
      <div className="topic-seek-bar">
        <input
          className="seek-input"
          type="datetime-local"
          step="1"
          value={seekInput}
          onChange={(e) => setSeekInput(e.target.value)}
          onKeyDown={handleSeekKey}
          title="Seek to timestamp"
        />
        <button className="bp5-button bp5-minimal bp5-small" onClick={handleSeek}>
          Seek
        </button>
        <div className="local-date-filter">
          <span className="date-filter-label">Filter from:</span>
          <input
            type="datetime-local"
            step="1"
            value={localDateFilter}
            onChange={(e) => setLocalDateFilter(e.target.value)}
            title="Local date filter (overrides global)"
          />
          {localDateFilter && (
            <button
              className="bp5-button bp5-minimal bp5-small"
              onClick={() => setLocalDateFilter('')}
              title="Clear local filter"
            >
              ×
            </button>
          )}
        </div>
      </div>
      <MessageList messages={filteredMessages} searchQuery={searchQuery} />
    </div>
  );
}
