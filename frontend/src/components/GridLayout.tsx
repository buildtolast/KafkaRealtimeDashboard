import { TopicWindow } from './TopicWindow';

interface GridLayoutProps {
  topics: string[];
  searchQuery?: string;
  globalDateFilterMs?: number;
}

export function GridLayout({ topics, searchQuery, globalDateFilterMs }: GridLayoutProps) {
  return (
    <div className="grid-layout">
      {topics.map((t) => (
        <div key={t} className="grid-cell">
          <div className="grid-cell-title">{t}</div>
          <TopicWindow
            topic={t}
            searchQuery={searchQuery}
            globalDateFilterMs={globalDateFilterMs}
          />
        </div>
      ))}
    </div>
  );
}
