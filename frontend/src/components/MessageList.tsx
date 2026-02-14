import { useState, useMemo, useCallback } from 'react';
import { Virtuoso } from 'react-virtuoso';
import { KafkaMessage } from '../types';

const MESSAGE_COLORS = [
  '#00ff87', '#ff6b6b', '#69dbff', '#ffd93d', '#c084fc',
  '#ff9f43', '#0abde3', '#ff6b9d', '#a3e635', '#38bdf8',
  '#fb923c', '#f472b6', '#34d399', '#e879f9', '#facc15',
  '#22d3ee', '#f87171', '#a78bfa', '#4ade80', '#fbbf24',
];

const MAX_DISPLAY_LENGTH = 200;

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
  const [expandedMessages, setExpandedMessages] = useState<Set<string>>(new Set());

  const toggleExpand = useCallback((msgId: string) => {
    setExpandedMessages((prev) => {
      const next = new Set(prev);
      if (next.has(msgId)) next.delete(msgId);
      else next.add(msgId);
      return next;
    });
  }, []);

  if (messages.length === 0) {
    return <div className="empty-messages">Waiting for messages...</div>;
  }

  return (
    <Virtuoso
      data={messages}
      className="message-list"
      followOutput="smooth"
      itemContent={(_index, msg) => {
        const msgId = `${msg.topic}-${msg.partition}-${msg.offset}`;
        return (
          <MessageRow
            msg={msg}
            searchQuery={searchQuery}
            expanded={expandedMessages.has(msgId)}
            onToggle={() => toggleExpand(msgId)}
          />
        );
      }}
    />
  );
}

interface MessageRowProps {
  msg: KafkaMessage;
  searchQuery?: string;
  expanded: boolean;
  onToggle: () => void;
}

function MessageRow({ msg, searchQuery, expanded, onToggle }: MessageRowProps) {
  const color = useMemo(() => getColor(msg.offset), [msg.offset]);
  const payloadText = msg.payload ?? '(empty)';
  const isTruncatable = payloadText.length > MAX_DISPLAY_LENGTH;
  const displayText = !expanded && isTruncatable
    ? payloadText.slice(0, MAX_DISPLAY_LENGTH)
    : payloadText;

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
        <div className="msg-payload-container">
          <pre className="msg-payload" style={{ color }}>
            {highlightText(displayText, searchQuery)}
            {!expanded && isTruncatable && <span className="msg-truncation-ellipsis">...</span>}
          </pre>
          {isTruncatable && (
            <button className="msg-expand-toggle" onClick={onToggle}>
              {expanded ? 'Collapse' : 'Expand'}
            </button>
          )}
        </div>
      </div>
    </div>
  );
}
