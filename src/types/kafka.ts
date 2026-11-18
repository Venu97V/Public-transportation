export type SentimentLabel = 'positive' | 'negative' | 'neutral';

export type TransitMode = 'bus' | 'metro' | 'train';

export type IssueCategory =
  | 'Delay & punctuality'
  | 'Safety & security'
  | 'Overcrowding'
  | 'Cleanliness'
  | 'Staff behaviour'
  | 'Fares & ticketing'
  | 'Comfort & facilities'
  | 'App & information'
  | 'General';

export type FeedbackSource =
  | 'twitter'
  | 'app-review'
  | 'feedback-form'
  | 'helpline'
  | 'csv-replay';

export type StreamScenario =
  | 'normal'
  | 'signal-delay'
  | 'overcrowding-ac'
  | 'express-launch';

export interface KafkaSentimentRecord {
  id: string;
  topic: string;
  scoredTopic: string;
  partition: 0 | 1 | 2;
  offset: number;
  key: string;
  consumerGroup: string;
  consumerId: string;
  timestamp: number;
  processingLatencyMs: number;
  source: FeedbackSource;
  route: string;
  mode: TransitMode;
  text: string;
  sentiment: SentimentLabel;
  score: number;
  confidence: number;
  aspect: IssueCategory;
  matchedTerms: string[];
  analyzerEngine: 'lexicon-nlp' | 'gemini-ai';
}

export interface RouteAlertRecord {
  id: string;
  topic: string;
  timestamp: number;
  route: string;
  negativeRatio: number;
  messages: number;
  topTopic: IssueCategory;
}

export interface PartitionMetrics {
  partition: 0 | 1 | 2;
  label: string;
  keyPrefix: string;
  highWatermark: number;
  committedOffset: number;
  lag: number;
  consumerId: string;
  messagesProcessed: number;
  positiveCount: number;
  negativeCount: number;
  neutralCount: number;
  avgLatencyMs: number;
}

export interface RouteAggregate {
  route: string;
  mode: TransitMode;
  partition: 0 | 1 | 2;
  total: number;
  positive: number;
  negative: number;
  neutral: number;
  avgScore: number;
  topIssue: IssueCategory;
}

export interface AspectAggregate {
  aspect: IssueCategory;
  total: number;
  negative: number;
  neutral: number;
  positive: number;
  sharePct: number;
}

export interface TimeBucket {
  bucketId: number;
  label: string;
  positive: number;
  negative: number;
  neutral: number;
  total: number;
  avgScore: number;
}

export interface BrokerSnapshot {
  clusterId: string;
  brokerHost: string;
  rawTopic: string;
  scoredTopic: string;
  alertsTopic: string;
  consumerGroup: string;
  sinkGroup: string;
  streaming: boolean;
  ratePerSec: number;
  scenario: StreamScenario;
  totalProduced: number;
  totalConsumed: number;
  avgLatencyMs: number;
  sentimentCounts: {
    positive: number;
    negative: number;
    neutral: number;
  };
  netSentimentScore: number;
  partitions: PartitionMetrics[];
  routes: RouteAggregate[];
  aspects: AspectAggregate[];
  timeline: TimeBucket[];
  recentEvents: KafkaSentimentRecord[];
  routeAlerts: RouteAlertRecord[];
}

export interface GeminiDeepAnalysis {
  sentiment: SentimentLabel;
  score: number;
  confidence: number;
  aspect: IssueCategory;
  summary: string;
  recommendedAction: string;
  urgency: 'low' | 'medium' | 'high';
  keyPhrases: string[];
}

export interface SentimentAlertRule {
  enabled: boolean;
  sentimentTarget: 'negative' | 'positive';
  thresholdPct: number;
  windowSeconds: number;
  minSamples: number;
}

export interface EvaluationClassMetrics {
  label: SentimentLabel;
  precision: number;
  recall: number;
  f1: number;
  support: number;
}

export interface EvaluationReport {
  samples: number;
  accuracy: number;
  confusionMatrix: Record<SentimentLabel, Record<SentimentLabel, number>>;
  classes: EvaluationClassMetrics[];
  note: string;
}
