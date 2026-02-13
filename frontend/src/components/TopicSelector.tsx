import { useState } from 'react';

interface TopicSelectorProps {
  topics: string[];
  onConfirm: (selected: string[]) => void;
}

export function TopicSelector({ topics, onConfirm }: TopicSelectorProps) {
  const [selected, setSelected] = useState<Set<string>>(new Set());

  function toggle(topic: string) {
    setSelected((prev) => {
      const next = new Set(prev);
      if (next.has(topic)) {
        next.delete(topic);
      } else {
        next.add(topic);
      }
      return next;
    });
  }

  function selectAll() {
    setSelected(new Set(topics));
  }

  function selectNone() {
    setSelected(new Set());
  }

  return (
    <div className="topic-selector">
      <h2>Select Topics</h2>
      <p className="topic-selector-hint">Choose which topics to monitor</p>
      <div className="topic-selector-actions">
        <button className="bp5-button bp5-minimal bp5-small" onClick={selectAll}>
          Select All
        </button>
        <button className="bp5-button bp5-minimal bp5-small" onClick={selectNone}>
          Clear
        </button>
      </div>
      <div className="topic-list">
        {topics.map((topic) => (
          <label key={topic} className={`topic-item ${selected.has(topic) ? 'selected' : ''}`}>
            <input
              type="checkbox"
              checked={selected.has(topic)}
              onChange={() => toggle(topic)}
            />
            <span className="topic-name">{topic}</span>
          </label>
        ))}
      </div>
      <button
        className="bp5-button bp5-intent-primary bp5-large topic-selector-confirm"
        disabled={selected.size === 0}
        onClick={() => onConfirm(Array.from(selected))}
      >
        Open {selected.size} {selected.size === 1 ? 'Topic' : 'Topics'}
      </button>
    </div>
  );
}
