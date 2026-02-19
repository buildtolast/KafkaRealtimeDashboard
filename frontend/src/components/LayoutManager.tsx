import { useState } from 'react';
import { MosaicLayout } from './MosaicLayout';
import { TabsLayout } from './TabsLayout';
import { GridLayout } from './GridLayout';
import { ChartView } from './ChartView';

type ViewMode = 'monitor' | 'chart';
type LayoutMode = 'mosaic' | 'tabs' | 'grid';

interface LayoutManagerProps {
  topics: string[];
  searchQuery?: string;
  globalDateFilterMs?: number;
}

export function LayoutManager({ topics, searchQuery, globalDateFilterMs }: LayoutManagerProps) {
  const [viewMode, setViewMode] = useState<ViewMode>('monitor');
  const [layoutMode, setLayoutMode] = useState<LayoutMode>(
    topics.length > 6 ? 'tabs' : 'mosaic'
  );

  return (
    <div className="layout-manager">
      <div className="layout-mode-bar">
        <div className="view-mode-group">
          <button
            className={viewMode === 'monitor' ? 'active' : ''}
            onClick={() => setViewMode('monitor')}
          >
            Monitor
          </button>
          <button
            className={viewMode === 'chart' ? 'active' : ''}
            onClick={() => setViewMode('chart')}
          >
            Chart
          </button>
        </div>
        {viewMode === 'monitor' && (
          <div className="layout-mode-group">
            <button
              className={layoutMode === 'mosaic' ? 'active' : ''}
              onClick={() => setLayoutMode('mosaic')}
            >
              Mosaic
            </button>
            <button
              className={layoutMode === 'tabs' ? 'active' : ''}
              onClick={() => setLayoutMode('tabs')}
            >
              Tabs
            </button>
            <button
              className={layoutMode === 'grid' ? 'active' : ''}
              onClick={() => setLayoutMode('grid')}
            >
              Grid
            </button>
          </div>
        )}
      </div>
      {viewMode === 'chart' ? (
        <ChartView topics={topics} />
      ) : (
        <>
          {layoutMode === 'mosaic' && (
            <MosaicLayout topics={topics} searchQuery={searchQuery} globalDateFilterMs={globalDateFilterMs} />
          )}
          {layoutMode === 'tabs' && (
            <TabsLayout topics={topics} searchQuery={searchQuery} globalDateFilterMs={globalDateFilterMs} />
          )}
          {layoutMode === 'grid' && (
            <GridLayout topics={topics} searchQuery={searchQuery} globalDateFilterMs={globalDateFilterMs} />
          )}
        </>
      )}
    </div>
  );
}
