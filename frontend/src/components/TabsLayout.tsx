import { useState } from 'react';
import { TopicWindow } from './TopicWindow';

interface TabsLayoutProps {
  topics: string[];
  searchQuery?: string;
  globalDateFilterMs?: number;
}

export function TabsLayout({ topics, searchQuery, globalDateFilterMs }: TabsLayoutProps) {
  const [activeTab, setActiveTab] = useState(topics[0]);

  // If active tab is no longer in topics, reset
  const currentTab = topics.includes(activeTab) ? activeTab : topics[0];

  return (
    <div className="tabs-layout">
      <div className="tabs-bar">
        {topics.map((t) => (
          <button
            key={t}
            className={`tab-button ${t === currentTab ? 'active' : ''}`}
            onClick={() => setActiveTab(t)}
          >
            {t}
          </button>
        ))}
      </div>
      <div className="tabs-content">
        <TopicWindow
          key={currentTab}
          topic={currentTab}
          searchQuery={searchQuery}
          globalDateFilterMs={globalDateFilterMs}
        />
      </div>
    </div>
  );
}
