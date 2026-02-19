import { useState, useEffect, useRef, useCallback } from 'react';
import { KafkaMessage } from '../types';
import { useMessageTracking } from '../contexts/MessageTrackingContext';

const MAX_MESSAGES = 500;
const RECONNECT_DELAY = 3000;

export function useKafkaStream(topic: string) {
  const [messages, setMessages] = useState<KafkaMessage[]>([]);
  const [connected, setConnected] = useState(false);
  const [paused, setPaused] = useState(false);
  const wsRef = useRef<WebSocket | null>(null);
  const reconnectTimer = useRef<ReturnType<typeof setTimeout>>();
  const pausedRef = useRef(false);
  const tracking = useMessageTracking();

  const clear = useCallback(() => setMessages([]), []);

  const togglePause = useCallback(() => {
    setPaused((prev) => {
      pausedRef.current = !prev;
      return !prev;
    });
  }, []);

  const seekToTimestamp = useCallback(async (timestampMs: number) => {
    try {
      const res = await fetch(`/api/seek/${encodeURIComponent(topic)}`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ timestamp_ms: timestampMs, max_messages: 200 }),
      });
      if (!res.ok) throw new Error(`HTTP ${res.status}`);
      const data: KafkaMessage[] = await res.json();
      setMessages(data.reverse());
      setPaused(true);
      pausedRef.current = true;
    } catch (e) {
      console.error('Seek failed:', e);
    }
  }, [topic]);

  useEffect(() => {
    let cancelled = false;

    function connect() {
      if (cancelled) return;

      const protocol = window.location.protocol === 'https:' ? 'wss:' : 'ws:';
      const ws = new WebSocket(`${protocol}//${window.location.host}/ws/${encodeURIComponent(topic)}`);
      wsRef.current = ws;

      ws.onopen = () => {
        if (!cancelled) setConnected(true);
      };

      ws.onclose = () => {
        if (!cancelled) {
          setConnected(false);
          reconnectTimer.current = setTimeout(connect, RECONNECT_DELAY);
        }
      };

      ws.onerror = () => {
        ws.close();
      };

      ws.onmessage = (event) => {
        if (pausedRef.current) return;
        const msg: KafkaMessage = JSON.parse(event.data);

        // Feed message into shared tracking context for chart + search
        if (tracking) {
          tracking.recordMessage(topic);
          tracking.addMessage(msg);
        }

        setMessages((prev) => {
          const next = [msg, ...prev];
          return next.length > MAX_MESSAGES ? next.slice(0, MAX_MESSAGES) : next;
        });
      };
    }

    connect();

    return () => {
      cancelled = true;
      clearTimeout(reconnectTimer.current);
      wsRef.current?.close();
    };
  }, [topic, tracking]);

  return { messages, connected, paused, clear, togglePause, seekToTimestamp };
}
