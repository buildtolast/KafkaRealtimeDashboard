import { useState, useEffect } from 'react';
import {
  Mosaic,
  MosaicWindow,
  MosaicNode,
  createBalancedTreeFromLeaves,
} from 'react-mosaic-component';
import { TopicWindow } from './TopicWindow';

import 'react-mosaic-component/react-mosaic-component.css';
import '@blueprintjs/core/lib/css/blueprint.css';
import '@blueprintjs/icons/lib/css/blueprint-icons.css';

interface MosaicLayoutProps {
  topics: string[];
  searchQuery?: string;
}

export function MosaicLayout({ topics, searchQuery }: MosaicLayoutProps) {
  const [mosaicValue, setMosaicValue] = useState<MosaicNode<string> | null>(null);

  useEffect(() => {
    if (topics.length === 1) {
      setMosaicValue(topics[0]);
    } else if (topics.length > 1) {
      setMosaicValue(createBalancedTreeFromLeaves(topics));
    }
  }, [topics]);

  if (!mosaicValue) return null;

  return (
    <Mosaic<string>
      renderTile={(id, path) => (
        <MosaicWindow<string> path={path} title={`Topic: ${id}`}>
          <TopicWindow topic={id} searchQuery={searchQuery} />
        </MosaicWindow>
      )}
      value={mosaicValue}
      onChange={(val) => setMosaicValue(val)}
      className="mosaic-blueprint-theme bp5-dark"
    />
  );
}
