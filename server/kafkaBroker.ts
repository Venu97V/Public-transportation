import { analyzeTransitFeedback } from './sentimentEngine.ts';
import type {
  AspectAggregate,
  BrokerSnapshot,
  EvaluationClassMetrics,
  EvaluationReport,
  FeedbackSource,
  IssueCategory,
  KafkaSentimentRecord,
  PartitionMetrics,
  RouteAggregate,
  RouteAlertRecord,
  SentimentLabel,
  StreamScenario,
  TimeBucket,
  TransitMode,
} from '../src/types/kafka.ts';

interface RouteCatalogItem {
  route: string;
  mode: TransitMode;
  partition: 0 | 1 | 2;
  key: string;
}

const ROUTES: RouteCatalogItem[] = [
  { route: 'Metro Line A', mode: 'metro', partition: 0, key: 'metro-line-a' },
  { route: 'Metro Line B', mode: 'metro', partition: 0, key: 'metro-line-b' },
  { route: 'Bus 12', mode: 'bus', partition: 1, key: 'bus-12' },
  { route: 'Bus 45', mode: 'bus', partition: 1, key: 'bus-45' },
  { route: 'Bus 101', mode: 'bus', partition: 1, key: 'bus-101' },
  { route: 'Suburban Line 1', mode: 'train', partition: 2, key: 'train-suburban-1' },
  { route: 'Suburban Line 2', mode: 'train', partition: 2, key: 'train-suburban-2' },
];

const SOURCES: FeedbackSource[] = [
  'twitter',
  'app-review',
  'feedback-form',
  'helpline',
];

const POSITIVE_TEMPLATES = [
  '{route} was right on time this morning, great job!',
  'Punctual as ever, {route} never lets me down',
  'Reached the office early thanks to a super quick {mode} ride',
  'Plenty of seats on {route} this evening, so relaxing',
  'Not crowded at all today, finally a peaceful commute 😊',
  'The {mode} was spotless today, really impressed',
  'Clean seats and a fresh coach. Well done!',
  'The driver was very polite and helped an elderly passenger',
  'Station staff on {route} were so helpful, solved my ticket issue quickly',
  'Smart card top-up took seconds and the fares are reasonable',
  'Affordable fares and a hassle-free ticket system 👍',
  'Live tracking for {route} showed accurate arrival times, love it',
  'The new app makes planning my {mode} trip so easy',
  'AC works great on {route}, the ride was smooth and comfortable',
  'Comfortable seats and a smooth ride today',
  'Felt safe at the stop after dark, good lighting and helpful guards',
];

const NEGATIVE_DELAY_TEMPLATES = [
  '{route} delayed by 40 minutes again, absolutely unacceptable',
  'Waiting for {route} for an hour with no updates. Terrible service 😡',
  'Another breakdown on {route}, stuck in the middle of nowhere',
  '{route} cancelled without any notice, so frustrating',
  'Late again! {route} is never on time anymore',
  'So overcrowded, couldn\'t even breathe inside the {mode}',
  'Packed like sardines on {route}, no space to stand',
  'Rush hour on {route} is a nightmare, completely crowded',
];

const NEGATIVE_OTHER_TEMPLATES = [
  'Seats are filthy and the coach smells awful',
  'Dirty station and overflowing bins. Disgusting',
  'The {mode} floor was covered in garbage, disgusting',
  'Felt unsafe at the stop after dark, no lighting at all',
  'Driver of {route} was driving rashly, really dangerous',
  'Phone stolen on a crowded {mode}, security is a joke',
  'Conductor was rude and refused to give change',
  'Driver ignored passengers waving at the stop, so rude',
  'Ticket machine took my money and nobody helped',
  'Fares hiked again, total rip-off 😤',
  'Overcharged on my smart card and still no refund',
  'The app keeps crashing and live tracking is completely wrong',
  'Information display shows wrong timings again, useless',
  'AC is broken on {route} again, sweating the whole ride',
  'Terrible bumpy ride and uncomfortable seats',
];

const NEUTRAL_TEMPLATES = [
  '{route} ran on its usual schedule today',
  'Took the {mode} to the city centre at 9 am',
  'Is there a service on {route} on Sunday?',
  'Platform change announced for the 6:15 train',
  'Bus pass renewal counter opens at 10',
  'Which stop is closest to the central library on {route}?',
  'Boarded {route} near the market around noon',
  'Timetable for {route} updated on the website',
];

const TAGS = ['', '', '', ' #commute', ' #publictransport', ' @transit_help'];

const ALL_ASPECTS: IssueCategory[] = [
  'Delay & punctuality',
  'Overcrowding',
  'Cleanliness',
  'Comfort & facilities',
  'Staff behaviour',
  'Fares & ticketing',
  'Safety & security',
  'App & information',
  'General',
];

export class KafkaBrokerEngine {
  private readonly rawTopic = 'transport.feedback.raw';
  private readonly scoredTopic = 'transport.feedback.analyzed';
  private readonly alertsTopic = 'transport.alerts';
  private readonly consumerGroup = 'sentiment-processor';
  private readonly sinkGroup = 'sqlite-sink';
  private readonly clusterId = 'kraft-cluster-01';
  private readonly brokerHost = 'localhost:9092';

  private streaming = true;
  private ratePerSec = 1.5;
  private scenario: StreamScenario = 'normal';
  private eventSeq = 0;
  private alertSeq = 0;
  private timelineSeq = 0;

  private partitions: Map<0 | 1 | 2, PartitionMetrics> = new Map();
  private partitionLogs: Map<0 | 1 | 2, KafkaSentimentRecord[]> = new Map();
  private recentEvents: KafkaSentimentRecord[] = [];
  private routeAlerts: RouteAlertRecord[] = [];
  private timeline: TimeBucket[] = [];

  // Per-route sliding window alert detector state (mirrors alerts.py)
  private readonly alertWindowMs = 30_000;
  private readonly alertMinMsgs = 6;
  private readonly alertNegRatio = 0.7;
  private readonly alertCooldownMs = 45_000;
  private routeWindowEvents: Map<
    string,
    Array<{ ts: number; sentiment: SentimentLabel; topic: IssueCategory }>
  > = new Map();
  private routeLastAlertTs: Map<string, number> = new Map();

  private totalProduced = 0;
  private totalConsumed = 0;
  private totalLatencySum = 0;
  private sentimentCounts = { positive: 0, negative: 0, neutral: 0 };

  private routeStats: Map<
    string,
    {
      route: string;
      mode: TransitMode;
      partition: 0 | 1 | 2;
      total: number;
      positive: number;
      negative: number;
      neutral: number;
      scoreSum: number;
      aspectCounts: Record<IssueCategory, number>;
    }
  > = new Map();

  private aspectCounts: Record<
    IssueCategory,
    { total: number; positive: number; negative: number; neutral: number }
  > = {
    'Delay & punctuality': { total: 0, positive: 0, negative: 0, neutral: 0 },
    'Safety & security': { total: 0, positive: 0, negative: 0, neutral: 0 },
    Overcrowding: { total: 0, positive: 0, negative: 0, neutral: 0 },
    Cleanliness: { total: 0, positive: 0, negative: 0, neutral: 0 },
    'Staff behaviour': { total: 0, positive: 0, negative: 0, neutral: 0 },
    'Fares & ticketing': { total: 0, positive: 0, negative: 0, neutral: 0 },
    'Comfort & facilities': { total: 0, positive: 0, negative: 0, neutral: 0 },
    'App & information': { total: 0, positive: 0, negative: 0, neutral: 0 },
    General: { total: 0, positive: 0, negative: 0, neutral: 0 },
  };

  private listeners = new Set<(event: { type: string; payload: unknown }) => void>();
  private timer: ReturnType<typeof setTimeout> | null = null;

  constructor() {
    this.initializeState();
    this.seedInitialHistory();
    this.scheduleNextTick();
  }

  private initializeState() {
    const partitionDefs: Array<{ id: 0 | 1 | 2; label: string; keyPrefix: string; consumerId: string }> = [
      { id: 0, label: 'Metro Lines (A & B)', keyPrefix: 'metro', consumerId: 'processor-worker-0' },
      { id: 1, label: 'City Bus Network (12, 45, 101)', keyPrefix: 'bus', consumerId: 'processor-worker-1' },
      { id: 2, label: 'Suburban Rail (Lines 1 & 2)', keyPrefix: 'train', consumerId: 'processor-worker-2' },
    ];

    for (const def of partitionDefs) {
      this.partitions.set(def.id, {
        partition: def.id,
        label: def.label,
        keyPrefix: def.keyPrefix,
        highWatermark: 0,
        committedOffset: 0,
        lag: 0,
        consumerId: def.consumerId,
        messagesProcessed: 0,
        positiveCount: 0,
        negativeCount: 0,
        neutralCount: 0,
        avgLatencyMs: 0,
      });
      this.partitionLogs.set(def.id, []);
    }

    for (const r of ROUTES) {
      this.routeStats.set(r.route, {
        route: r.route,
        mode: r.mode,
        partition: r.partition,
        total: 0,
        positive: 0,
        negative: 0,
        neutral: 0,
        scoreSum: 0,
        aspectCounts: {
          'Delay & punctuality': 0,
          'Safety & security': 0,
          Overcrowding: 0,
          Cleanliness: 0,
          'Staff behaviour': 0,
          'Fares & ticketing': 0,
          'Comfort & facilities': 0,
          'App & information': 0,
          General: 0,
        },
      });
      this.routeWindowEvents.set(r.route, []);
    }
  }

  private seedInitialHistory() {
    const now = Date.now();
    for (let i = 32; i >= 1; i--) {
      const ts = now - i * 7500;
      this.produceSimulatedEvent(ts, false);
    }
  }

  public subscribe(listener: (event: { type: string; payload: unknown }) => void): () => void {
    this.listeners.add(listener);
    return () => {
      this.listeners.delete(listener);
    };
  }

  private broadcast(type: string, payload: unknown) {
    for (const listener of this.listeners) {
      try {
        listener({ type, payload });
      } catch {
        // Ignore disconnected listeners
      }
    }
  }

  private scheduleNextTick() {
    if (this.timer) {
      clearTimeout(this.timer);
      this.timer = null;
    }
    if (!this.streaming) return;

    const intervalMs = Math.max(250, Math.round(1000 / Math.max(0.25, this.ratePerSec)));
    this.timer = setTimeout(() => {
      if (this.streaming) {
        this.produceSimulatedEvent(Date.now(), true);
      }
      this.scheduleNextTick();
    }, intervalMs);
  }

  public setStreaming(streaming: boolean) {
    this.streaming = streaming;
    if (streaming) {
      this.scheduleNextTick();
    } else if (this.timer) {
      clearTimeout(this.timer);
      this.timer = null;
    }
    this.broadcast('snapshot', this.getSnapshot());
  }

  public setRatePerSec(rate: number) {
    this.ratePerSec = Math.max(0.5, Math.min(5, Number(rate.toFixed(1))));
    if (this.streaming) {
      this.scheduleNextTick();
    }
    this.broadcast('snapshot', this.getSnapshot());
  }

  public setScenario(scenario: StreamScenario) {
    this.scenario = scenario;
    this.broadcast('snapshot', this.getSnapshot());
  }

  public triggerBurst(count = 6) {
    const safeCount = Math.max(1, Math.min(20, count));
    const now = Date.now();
    for (let i = 0; i < safeCount; i++) {
      this.produceSimulatedEvent(now + i * 25, true);
    }
  }

  public resetConsumerOffset(partitionId: 0 | 1 | 2) {
    const p = this.partitions.get(partitionId);
    const logs = this.partitionLogs.get(partitionId);
    if (!p || !logs) return;

    const replayCount = Math.min(logs.length, 6);
    p.committedOffset = Math.max(0, p.highWatermark - replayCount);
    p.lag = p.highWatermark - p.committedOffset;
    this.broadcast('snapshot', this.getSnapshot());

    if (replayCount > 0) {
      setTimeout(() => {
        const current = this.partitions.get(partitionId);
        if (!current) return;
        current.committedOffset = current.highWatermark;
        current.lag = 0;
        this.broadcast('snapshot', this.getSnapshot());
      }, 900);
    }
  }

  public publishCustomMessage(input: {
    text: string;
    route?: string;
    source?: FeedbackSource;
    overrideAnalysis?: {
      sentiment: SentimentLabel;
      score: number;
      confidence: number;
      aspect: IssueCategory;
      matchedTerms: string[];
      analyzerEngine: 'lexicon-nlp' | 'gemini-ai';
    };
  }): KafkaSentimentRecord {
    const routeItem = ROUTES.find((r) => r.route === input.route) ?? ROUTES[0];
    const source = input.source ?? 'feedback-form';

    return this.ingestRecord({
      text: input.text.trim(),
      routeItem,
      source,
      timestamp: Date.now(),
      emitEvent: true,
      overrideAnalysis: input.overrideAnalysis,
    });
  }

  private produceSimulatedEvent(timestamp: number, emitEvent: boolean): KafkaSentimentRecord {
    let routeItem = ROUTES[Math.floor(Math.random() * ROUTES.length)];
    let pool: string[];

    if (this.scenario === 'signal-delay') {
      if (Math.random() < 0.78) {
        routeItem = Math.random() < 0.6 ? ROUTES[0] : ROUTES[2]; // Focus burst on Metro Line A / Bus 12
        pool = NEGATIVE_DELAY_TEMPLATES;
      } else {
        pool = NEUTRAL_TEMPLATES;
      }
    } else if (this.scenario === 'overcrowding-ac') {
      if (Math.random() < 0.8) {
        routeItem = Math.random() < 0.55 ? ROUTES[3] : ROUTES[1]; // Bus 45 / Metro Line B
        pool = [...NEGATIVE_DELAY_TEMPLATES.slice(5), ...NEGATIVE_OTHER_TEMPLATES];
      } else {
        pool = NEUTRAL_TEMPLATES;
      }
    } else if (this.scenario === 'express-launch') {
      pool = Math.random() < 0.78 ? POSITIVE_TEMPLATES : NEUTRAL_TEMPLATES;
    } else {
      const roll = Math.random();
      if (roll < 0.35) pool = POSITIVE_TEMPLATES;
      else if (roll < 0.65) pool = NEUTRAL_TEMPLATES;
      else pool = [...NEGATIVE_DELAY_TEMPLATES, ...NEGATIVE_OTHER_TEMPLATES];
    }

    const template = pool[Math.floor(Math.random() * pool.length)];
    const tag = TAGS[Math.floor(Math.random() * TAGS.length)];
    const text =
      template
        .replace(/\{route\}/g, routeItem.route)
        .replace(/\{mode\}/g, routeItem.mode) + tag;

    const source = SOURCES[Math.floor(Math.random() * SOURCES.length)];

    return this.ingestRecord({
      text,
      routeItem,
      source,
      timestamp,
      emitEvent,
    });
  }

  private evaluateRouteAlert(
    route: string,
    sentiment: SentimentLabel,
    topic: IssueCategory,
    now: number
  ) {
    const events = this.routeWindowEvents.get(route) ?? [];
    events.push({ ts: now, sentiment, topic });

    while (events.length > 0 && now - events[0].ts > this.alertWindowMs) {
      events.shift();
    }
    this.routeWindowEvents.set(route, events);

    if (events.length < this.alertMinMsgs) return;
    const lastAlert = this.routeLastAlertTs.get(route) ?? -Infinity;
    if (now - lastAlert < this.alertCooldownMs) return;

    const negTopics = events.filter((e) => e.sentiment === 'negative').map((e) => e.topic);
    const ratio = negTopics.length / events.length;
    if (ratio < this.alertNegRatio) return;

    this.routeLastAlertTs.set(route, now);

    const topicCounts = new Map<IssueCategory, number>();
    for (const t of negTopics) {
      topicCounts.set(t, (topicCounts.get(t) ?? 0) + 1);
    }
    let topTopic: IssueCategory = 'General';
    let bestCount = -1;
    for (const [t, c] of topicCounts.entries()) {
      if (c > bestCount) {
        bestCount = c;
        topTopic = t;
      }
    }

    this.alertSeq += 1;
    this.routeAlerts.unshift({
      id: `alert-${now}-${this.alertSeq}`,
      topic: this.alertsTopic,
      timestamp: now,
      route,
      negativeRatio: Number(ratio.toFixed(2)),
      messages: events.length,
      topTopic,
    });
    if (this.routeAlerts.length > 25) {
      this.routeAlerts.pop();
    }
  }

  private ingestRecord(params: {
    text: string;
    routeItem: RouteCatalogItem;
    source: FeedbackSource;
    timestamp: number;
    emitEvent: boolean;
    overrideAnalysis?: {
      sentiment: SentimentLabel;
      score: number;
      confidence: number;
      aspect: IssueCategory;
      matchedTerms: string[];
      analyzerEngine: 'lexicon-nlp' | 'gemini-ai';
    };
  }): KafkaSentimentRecord {
    const startNs = process.hrtime.bigint();
    const nlp = params.overrideAnalysis ?? {
      ...analyzeTransitFeedback(params.text),
      analyzerEngine: 'lexicon-nlp' as const,
    };
    const elapsedMs = Number(process.hrtime.bigint() - startNs) / 1_000_000;
    const processingLatencyMs = Number(
      Math.max(1.5, Math.min(26, elapsedMs + 1.8 + Math.random() * 5.2)).toFixed(1)
    );

    const pMetrics = this.partitions.get(params.routeItem.partition)!;
    const offset = pMetrics.highWatermark;
    pMetrics.highWatermark += 1;
    pMetrics.committedOffset = pMetrics.highWatermark;
    pMetrics.lag = 0;
    pMetrics.messagesProcessed += 1;
    pMetrics.avgLatencyMs = Number(
      (
        (pMetrics.avgLatencyMs * (pMetrics.messagesProcessed - 1) + processingLatencyMs) /
        pMetrics.messagesProcessed
      ).toFixed(1)
    );

    if (nlp.sentiment === 'positive') pMetrics.positiveCount += 1;
    else if (nlp.sentiment === 'negative') pMetrics.negativeCount += 1;
    else pMetrics.neutralCount += 1;

    this.eventSeq += 1;
    this.totalProduced += 1;
    this.totalConsumed += 1;
    this.totalLatencySum += processingLatencyMs;
    this.sentimentCounts[nlp.sentiment] += 1;

    const record: KafkaSentimentRecord = {
      id: `evt-${params.timestamp}-${this.eventSeq}`,
      topic: this.rawTopic,
      scoredTopic: this.scoredTopic,
      partition: params.routeItem.partition,
      offset,
      key: params.routeItem.route,
      consumerGroup: this.consumerGroup,
      consumerId: pMetrics.consumerId,
      timestamp: params.timestamp,
      processingLatencyMs,
      source: params.source,
      route: params.routeItem.route,
      mode: params.routeItem.mode,
      text: params.text,
      sentiment: nlp.sentiment,
      score: nlp.score,
      confidence: nlp.confidence,
      aspect: nlp.aspect,
      matchedTerms: nlp.matchedTerms,
      analyzerEngine: nlp.analyzerEngine,
    };

    const pLog = this.partitionLogs.get(params.routeItem.partition)!;
    pLog.unshift(record);
    if (pLog.length > 100) pLog.pop();

    this.recentEvents.unshift(record);
    if (this.recentEvents.length > 250) this.recentEvents.pop();

    const rStat = this.routeStats.get(params.routeItem.route)!;
    rStat.total += 1;
    rStat[nlp.sentiment] += 1;
    rStat.scoreSum += nlp.score;
    rStat.aspectCounts[nlp.aspect] += 1;

    this.aspectCounts[nlp.aspect].total += 1;
    this.aspectCounts[nlp.aspect][nlp.sentiment] += 1;

    this.evaluateRouteAlert(params.routeItem.route, nlp.sentiment, nlp.aspect, params.timestamp);
    this.updateTimelineBucket(record);

    if (params.emitEvent) {
      this.broadcast('event', {
        record,
        snapshot: this.getSnapshot(),
      });
    }

    return record;
  }

  private updateTimelineBucket(record: KafkaSentimentRecord) {
    const bucketWindowMs = 8000;
    const bucketTs = Math.floor(record.timestamp / bucketWindowMs) * bucketWindowMs;
    const d = new Date(bucketTs);
    const label = d.toTimeString().slice(0, 8);

    const last = this.timeline[this.timeline.length - 1];
    if (last && last.label === label) {
      last[record.sentiment] += 1;
      const prevSum = last.avgScore * last.total;
      last.total += 1;
      last.avgScore = Number(((prevSum + record.score) / last.total).toFixed(2));
    } else {
      this.timelineSeq += 1;
      this.timeline.push({
        bucketId: this.timelineSeq,
        label,
        positive: record.sentiment === 'positive' ? 1 : 0,
        negative: record.sentiment === 'negative' ? 1 : 0,
        neutral: record.sentiment === 'neutral' ? 1 : 0,
        total: 1,
        avgScore: record.score,
      });
      if (this.timeline.length > 24) {
        this.timeline.shift();
      }
    }
  }

  public evaluateBenchmark(sampleCount = 600): EvaluationReport {
    const labels: SentimentLabel[] = ['positive', 'neutral', 'negative'];
    const matrix: Record<SentimentLabel, Record<SentimentLabel, number>> = {
      positive: { positive: 0, neutral: 0, negative: 0 },
      neutral: { positive: 0, neutral: 0, negative: 0 },
      negative: { positive: 0, neutral: 0, negative: 0 },
    };

    const pools: Record<SentimentLabel, string[]> = {
      positive: POSITIVE_TEMPLATES,
      neutral: NEUTRAL_TEMPLATES,
      negative: [...NEGATIVE_DELAY_TEMPLATES, ...NEGATIVE_OTHER_TEMPLATES],
    };

    // Add realistic tricky/sarcastic/edge sentences so evaluation mirrors README honesty
    const realisticSet: Array<{ text: string; truth: SentimentLabel }> = [
      { text: 'Bus 12 delayed by 40 minutes again, absolutely unacceptable', truth: 'negative' },
      { text: 'Packed like sardines on Metro Line A, no space to stand', truth: 'negative' },
      { text: 'The metro was spotless today, really impressed', truth: 'positive' },
      { text: 'Right on time and very comfortable, great job!', truth: 'positive' },
      { text: 'Is there a service on Bus 12 on Sunday?', truth: 'neutral' },
      { text: 'Which stop is closest to the central library?', truth: 'neutral' },
      { text: 'The bus was not on time', truth: 'negative' },
      { text: 'Oh great, another 45 minute wait in the rain, just what I needed', truth: 'negative' },
      { text: 'Boarded Suburban Line 1 at platform 3', truth: 'neutral' },
      { text: 'Affordable fares and a hassle-free ticket system', truth: 'positive' },
      { text: 'Ticket machine took my money and nobody helped', truth: 'negative' },
    ];

    let correct = 0;
    const totalSamples = Math.max(100, Math.min(2000, sampleCount));

    for (let i = 0; i < totalSamples; i++) {
      let text: string;
      let truth: SentimentLabel;

      if (i < realisticSet.length * 4) {
        const item = realisticSet[i % realisticSet.length];
        text = item.text;
        truth = item.truth;
      } else {
        const routeItem = ROUTES[i % ROUTES.length];
        truth = labels[i % 3];
        const tplList = pools[truth];
        const tpl = tplList[i % tplList.length];
        text = tpl.replace(/\{route\}/g, routeItem.route).replace(/\{mode\}/g, routeItem.mode);
      }

      const pred = analyzeTransitFeedback(text).sentiment;
      matrix[truth][pred] += 1;
      if (pred === truth) correct += 1;
    }

    const classes: EvaluationClassMetrics[] = labels.map((c) => {
      const tp = matrix[c][c];
      const predSum = matrix.positive[c] + matrix.neutral[c] + matrix.negative[c];
      const trueSum = matrix[c].positive + matrix[c].neutral + matrix[c].negative;
      const precision = predSum > 0 ? Number((tp / predSum).toFixed(2)) : 0;
      const recall = trueSum > 0 ? Number((tp / trueSum).toFixed(2)) : 0;
      const f1 =
        precision + recall > 0
          ? Number(((2 * precision * recall) / (precision + recall)).toFixed(2))
          : 0;
      return {
        label: c,
        precision,
        recall,
        f1,
        support: trueSum,
      };
    });

    return {
      samples: totalSamples,
      accuracy: Number(((correct / totalSamples) * 100).toFixed(1)),
      confusionMatrix: matrix,
      classes,
      note: 'Includes synthetic generator phrases plus realistic edge-case sentences (including sarcasm). For an independent academic benchmark, run evaluate.py against a labelled public corpus such as Twitter US Airline Sentiment.',
    };
  }

  public getPartitionRecords(partitionId: 0 | 1 | 2): KafkaSentimentRecord[] {
    return this.partitionLogs.get(partitionId) ?? [];
  }

  public getAllRecentRecords(): KafkaSentimentRecord[] {
    return this.recentEvents;
  }

  public getSnapshot(): BrokerSnapshot {
    const routes: RouteAggregate[] = Array.from(this.routeStats.values()).map((r) => {
      let topIssue: IssueCategory = 'General';
      let maxAspectCount = -1;
      for (const aspect of ALL_ASPECTS) {
        const c = r.aspectCounts[aspect];
        if (c > maxAspectCount) {
          maxAspectCount = c;
          topIssue = aspect;
        }
      }

      return {
        route: r.route,
        mode: r.mode,
        partition: r.partition,
        total: r.total,
        positive: r.positive,
        negative: r.negative,
        neutral: r.neutral,
        avgScore: r.total > 0 ? Number((r.scoreSum / r.total).toFixed(2)) : 0,
        topIssue,
      };
    });

    const totalAll = Math.max(1, this.totalConsumed);
    const aspects: AspectAggregate[] = ALL_ASPECTS.map((aspect) => {
      const counts = this.aspectCounts[aspect];
      return {
        aspect,
        total: counts.total,
        negative: counts.negative,
        neutral: counts.neutral,
        positive: counts.positive,
        sharePct: Math.round((counts.total / totalAll) * 100),
      };
    }).sort((a, b) => b.total - a.total);

    const netSentimentScore =
      this.totalConsumed > 0
        ? Number(
            (
              ((this.sentimentCounts.positive - this.sentimentCounts.negative) /
                this.totalConsumed) *
              100
            ).toFixed(1)
          )
        : 0;

    return {
      clusterId: this.clusterId,
      brokerHost: this.brokerHost,
      rawTopic: this.rawTopic,
      scoredTopic: this.scoredTopic,
      alertsTopic: this.alertsTopic,
      consumerGroup: this.consumerGroup,
      sinkGroup: this.sinkGroup,
      streaming: this.streaming,
      ratePerSec: this.ratePerSec,
      scenario: this.scenario,
      totalProduced: this.totalProduced,
      totalConsumed: this.totalConsumed,
      avgLatencyMs:
        this.totalConsumed > 0
          ? Number((this.totalLatencySum / this.totalConsumed).toFixed(1))
          : 0,
      sentimentCounts: { ...this.sentimentCounts },
      netSentimentScore,
      partitions: Array.from(this.partitions.values()),
      routes,
      aspects,
      timeline: [...this.timeline],
      recentEvents: this.recentEvents.slice(0, 180),
      routeAlerts: [...this.routeAlerts],
    };
  }
}
