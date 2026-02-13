export interface KafkaMessage {
  topic: string;
  partition: number;
  offset: number;
  key: string | null;
  payload: string | null;
  timestamp: number | null;
}

export interface TopicsResponse {
  topics: string[];
}

export interface BrokerResponse {
  brokers: string;
}
