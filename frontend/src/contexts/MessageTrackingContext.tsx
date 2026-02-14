import { createContext, useContext, useRef, useCallback, type ReactNode } from 'react';
import { KafkaMessage } from '../types';

const MAX_BUFFER_PER_TOPIC = 500;
const MAX_RATE_BUCKETS = 60; // 60 x 5s = 5 min history
const BUCKET_SIZE_MS = 5000;

export interface RateDataPoint {
  time: number; // bucket timestamp in seconds
  counts: Record<string, number>;
}

interface MessageTrackingContextType {
  recordMessage: (topic: string) => void;
  addMessage: (msg: KafkaMessage) => void;
  getHistory: () => RateDataPoint[];
  getAllMessages: () => Map<string, KafkaMessage[]>;
}

const MessageTrackingContext = createContext<MessageTrackingContextType | null>(null);

export function MessageTrackingProvider({ children }: { children: ReactNode }) {
  const rateRef = useRef<Map<number, Record<string, number>>>(new Map());
  const bufferRef = useRef<Map<string, KafkaMessage[]>>(new Map());

  const recordMessage = useCallback((topic: string) => {
    const bucketSec = Math.floor(Date.now() / BUCKET_SIZE_MS) * 5;
    const map = rateRef.current;
    if (!map.has(bucketSec)) {
      map.set(bucketSec, {});
      const keys = Array.from(map.keys()).sort((a, b) => a - b);
      while (keys.length > MAX_RATE_BUCKETS) {
        map.delete(keys.shift()!);
      }
    }
    const bucket = map.get(bucketSec)!;
    bucket[topic] = (bucket[topic] || 0) + 1;
  }, []);

  const addMessage = useCallback((msg: KafkaMessage) => {
    const map = bufferRef.current;
    if (!map.has(msg.topic)) map.set(msg.topic, []);
    const arr = map.get(msg.topic)!;
    arr.unshift(msg);
    if (arr.length > MAX_BUFFER_PER_TOPIC) arr.length = MAX_BUFFER_PER_TOPIC;
  }, []);

  const getHistory = useCallback((): RateDataPoint[] => {
    return Array.from(rateRef.current.entries())
      .sort(([a], [b]) => a - b)
      .map(([time, counts]) => ({ time, counts }));
  }, []);

  const getAllMessages = useCallback((): Map<string, KafkaMessage[]> => {
    return new Map(bufferRef.current);
  }, []);

  return (
    <MessageTrackingContext.Provider value={{ recordMessage, addMessage, getHistory, getAllMessages }}>
      {children}
    </MessageTrackingContext.Provider>
  );
}

export function useMessageTracking() {
  return useContext(MessageTrackingContext);
}
