import { useMemo } from 'react';
import { Virtuoso } from 'react-virtuoso';
import { KafkaMessage } from '../types';

// Bright, high-contrast colors readable on black background
const MESSAGE_COLORS = [
  '#00ff87', // green
  '#ff6b6b', // coral red
  '#69dbff', // sky blue
  '#ffd93d', // yellow
  '#c084fc', // purple
  '#ff9f43', // orange
  '#0abde3', // cyan
  '#ff6b9d', // pink
  '#a3e635', // lime
  '#38bdf8', // light blue
  '#fb923c', // amber
  '#f472b6', // hot pink
  '#34d399', // emerald
  '#e879f9', // fuchsia
  '#facc15', // gold
  '#22d3ee', // teal
  '#f87171', // red
  '#a78bfa', // violet
  '#4ade80', // bright green
  '#fbbf24', // warm yellow
];

function getColor(offset: number): string {
  return MESSAGE_COLORS[Math.abs(offset) % MESSAGE_COLORS.length];
}

function highlightText(text: string, query: string | undefined): React.ReactNode {
  if (!query || !query.trim()) return text;
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

interface MessageListProps {
  messages: KafkaMessage[];
  searchQuery?: string;
}

export function MessageList({ messages, searchQuery }: MessageListProps) {
  if (messages.length === 0) {
    return <div className="empty-messages">Waiting for messages...</div>;
  }

  return (
    <Virtuoso
      data={messages}
      className="message-list"
      followOutput="smooth"
      itemContent={(_index, msg) => (
        <MessageRow msg={msg} searchQuery={searchQuery} />
      )}
    />
  );
}

function MessageRow({ msg, searchQuery }: { msg: KafkaMessage; searchQuery?: string }) {
  const color = useMemo(() => getColor(msg.offset), [msg.offset]);

  return (
    <div className="message-row" style={{ borderLeftColor: color }}>
      <div className="message-meta">
        <span className="msg-offset" style={{ color }}>#{msg.offset}</span>
        <span className="msg-partition">P{msg.partition}</span>
        {msg.timestamp && (
          <span className="msg-timestamp">
            {new Date(msg.timestamp).toLocaleTimeString()}
          </span>
        )}
      </div>
      <div className="msg-kv">
        <span className="msg-kv-label">Key:</span>
        <span className="msg-kv-value">
          {highlightText(msg.key ?? '(null)', searchQuery)}
        </span>
      </div>
      <div className="msg-kv">
        <span className="msg-kv-label">Value:</span>
        <pre className="msg-payload" style={{ color }}>
          {highlightText(msg.payload ?? '(empty)', searchQuery)}
        </pre>
      </div>
    </div>
  );
}
