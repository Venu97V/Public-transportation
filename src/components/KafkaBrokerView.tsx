import { useEffect, useState } from 'react';
import { RotateCcw, Send, Sliders } from 'lucide-react';
import type {
  BrokerSnapshot,
  FeedbackSource,
  KafkaSentimentRecord,
  StreamScenario,
} from '../types/kafka';

interface KafkaBrokerViewProps {
  snapshot: BrokerSnapshot;
  onUpdateStream: (params: {
    streaming?: boolean;
    ratePerSec?: number;
    scenario?: StreamScenario;
    burstCount?: number;
  }) => Promise<unknown>;
  onReplayPartition: (partitionId: 0 | 1 | 2) => Promise<unknown>;
  onPublishMessage: (input: {
    text: string;
    route: string;
    source: FeedbackSource;
    useGemini?: boolean;
  }) => Promise<{ record: KafkaSentimentRecord }>;
  onInspectRecord: (record: KafkaSentimentRecord) => void;
}

const ROUTES = [
  'Metro Line A',
  'Metro Line B',
  'Bus 12',
  'Bus 45',
  'Bus 101',
  'Suburban Line 1',
  'Suburban Line 2',
];

const SOURCES: FeedbackSource[] = [
  'twitter',
  'app-review',
  'feedback-form',
  'helpline',
];

export function KafkaBrokerView({
  snapshot,
  onUpdateStream,
  onReplayPartition,
  onPublishMessage,
  onInspectRecord,
}: KafkaBrokerViewProps) {
  const [selectedPartition, setSelectedPartition] = useState<0 | 1 | 2>(0);
  const [partitionRecords, setPartitionRecords] = useState<KafkaSentimentRecord[]>([]);
  const [loadingPartition, setLoadingPartition] = useState(false);

  const [producerText, setProducerText] = useState('');
  const [producerRoute, setProducerRoute] = useState(ROUTES[0]);
  const [producerSource, setProducerSource] = useState<FeedbackSource>('twitter');
  const [producerNotice, setProducerNotice] = useState<string | null>(null);
  const [producerError, setProducerError] = useState<string | null>(null);
  const [producing, setProducing] = useState(false);

  useEffect(() => {
    let cancelled = false;
    const load = async () => {
      setLoadingPartition(true);
      try {
        const res = await fetch(`/api/partitions/${selectedPartition}`);
        if (!res.ok) return;
        const data = (await res.json()) as { records: KafkaSentimentRecord[] };
        if (!cancelled) {
          setPartitionRecords(data.records);
        }
      } finally {
        if (!cancelled) setLoadingPartition(false);
      }
    };
    load();
    return () => {
      cancelled = true;
    };
  }, [selectedPartition, snapshot.totalConsumed]);

  const handleProduce = async (e: React.FormEvent) => {
    e.preventDefault();
    if (producerText.trim().length < 3) {
      setProducerError('Enter at least 3 characters before producing to the Kafka topic.');
      return;
    }
    setProducing(true);
    setProducerError(null);
    setProducerNotice(null);
    try {
      const { record } = await onPublishMessage({
        text: producerText.trim(),
        route: producerRoute,
        source: producerSource,
      });
      setProducerText('');
      setProducerNotice(
        `Committed to P${record.partition} · offset #${record.offset} · classified as ${record.sentiment} (${record.aspect})`
      );
      setSelectedPartition(record.partition);
    } catch (err) {
      setProducerError(err instanceof Error ? err.message : 'Failed to produce event');
    } finally {
      setProducing(false);
    }
  };

  return (
    <div className="space-y-6">
      {/* Cluster & Throughput Controls */}
      <section className="bg-white border border-slate-200 rounded-lg p-5">
        <div className="flex flex-wrap items-start justify-between gap-4 border-b border-slate-200 pb-4">
          <div>
            <h2 className="text-base font-semibold text-slate-900">
              01. Broker Cluster & Producer Throughput
            </h2>
            <p className="text-xs text-slate-500 mt-0.5">
              Cluster: <span className="font-mono">{snapshot.clusterId}</span> · Broker:{' '}
              <span className="font-mono">{snapshot.brokerHost}</span> · Topics:{' '}
              <span className="font-mono">{snapshot.rawTopic}</span> →{' '}
              <span className="font-mono">{snapshot.scoredTopic}</span> +{' '}
              <span className="font-mono">{snapshot.alertsTopic}</span>
            </p>
          </div>

          <div className="flex items-center gap-3">
            <Sliders className="h-4 w-4 text-slate-400" />
            <label htmlFor="rate-slider" className="text-xs font-medium text-slate-700 whitespace-nowrap">
              Producer Rate:
            </label>
            <input
              id="rate-slider"
              type="range"
              min={0.5}
              max={5}
              step={0.5}
              value={snapshot.ratePerSec}
              onChange={(e) => onUpdateStream({ ratePerSec: Number(e.target.value) })}
              className="w-32 accent-sky-600 cursor-pointer"
            />
            <span className="font-mono text-xs tabular-nums text-slate-800 w-16 text-right">
              {snapshot.ratePerSec.toFixed(1)} msg/s
            </span>
          </div>
        </div>

        {/* 3 Partition Cards */}
        <div className="grid grid-cols-1 md:grid-cols-3 gap-4 pt-4">
          {snapshot.partitions.map((p) => {
            const isSelected = selectedPartition === p.partition;
            return (
              <div
                key={p.partition}
                className={`border rounded-lg p-4 transition-colors ${
                  isSelected
                    ? 'border-sky-600 bg-sky-50/30'
                    : 'border-slate-200 bg-white hover:border-slate-300'
                }`}
              >
                <div className="flex items-center justify-between gap-2">
                  <button
                    type="button"
                    onClick={() => setSelectedPartition(p.partition)}
                    className="text-left cursor-pointer"
                  >
                    <div className="font-mono text-xs text-slate-500">
                      Partition {p.partition} · key:{p.keyPrefix}-*
                    </div>
                    <div className="text-sm font-semibold text-slate-900 mt-0.5">
                      {p.label}
                    </div>
                  </button>

                  <button
                    type="button"
                    onClick={() => onReplayPartition(p.partition)}
                    title="Reset consumer group offset and replay recent records"
                    className="inline-flex items-center gap-1 px-2.5 py-1.5 rounded-md border border-slate-200 bg-white text-xs font-medium text-slate-700 hover:bg-slate-50 cursor-pointer whitespace-nowrap shrink-0"
                  >
                    <RotateCcw className="h-3 w-3" />
                    Replay
                  </button>
                </div>

                <div className="mt-4 grid grid-cols-3 gap-2 text-xs border-t border-slate-100 pt-3 font-mono tabular-nums">
                  <div>
                    <div className="text-slate-500 font-sans">High Offset</div>
                    <div className="text-slate-900 font-medium mt-0.5">#{p.highWatermark}</div>
                  </div>
                  <div>
                    <div className="text-slate-500 font-sans">Committed</div>
                    <div className="text-slate-900 font-medium mt-0.5">#{p.committedOffset}</div>
                  </div>
                  <div>
                    <div className="text-slate-500 font-sans">Lag</div>
                    <div
                      className={`font-medium mt-0.5 ${
                        p.lag > 0 ? 'text-amber-700' : 'text-emerald-700'
                      }`}
                    >
                      {p.lag} msgs
                    </div>
                  </div>
                </div>

                <div className="mt-2.5 flex items-center justify-between text-xs text-slate-500 font-mono tabular-nums">
                  <span>{p.consumerId}</span>
                  <span>{p.avgLatencyMs.toFixed(1)} ms avg</span>
                </div>
              </div>
            );
          })}
        </div>
      </section>

      {/* Inline Kafka Producer Console + Partition Offset Log */}
      <section className="grid grid-cols-1 lg:grid-cols-12 gap-6">
        <div className="lg:col-span-5 bg-white border border-slate-200 rounded-lg p-5 space-y-4">
          <div>
            <h2 className="text-base font-semibold text-slate-900">
              02. Kafka Producer Console
            </h2>
            <p className="text-xs text-slate-500 mt-0.5">
              Send a raw commuter message to test partitioning and real-time NLP scoring
            </p>
          </div>

          <form onSubmit={handleProduce} className="space-y-3.5">
            <div>
              <label htmlFor="broker-route" className="block text-xs font-medium text-slate-700 mb-1">
                Route Key (Determines Partition)
              </label>
              <select
                id="broker-route"
                value={producerRoute}
                onChange={(e) => setProducerRoute(e.target.value)}
                className="w-full h-9 rounded-md border border-slate-200 bg-white px-2.5 text-xs text-slate-800 focus:outline-none focus:ring-2 focus:ring-sky-600"
              >
                {ROUTES.map((r) => (
                  <option key={r} value={r}>
                    {r}
                  </option>
                ))}
              </select>
            </div>

            <div>
              <label htmlFor="broker-source" className="block text-xs font-medium text-slate-700 mb-1">
                Source Platform
              </label>
              <select
                id="broker-source"
                value={producerSource}
                onChange={(e) => setProducerSource(e.target.value as FeedbackSource)}
                className="w-full h-9 rounded-md border border-slate-200 bg-white px-2.5 text-xs text-slate-800 focus:outline-none focus:ring-2 focus:ring-sky-600"
              >
                {SOURCES.map((s) => (
                  <option key={s} value={s}>
                    {s}
                  </option>
                ))}
              </select>
            </div>

            <div>
              <label htmlFor="broker-message" className="block text-xs font-medium text-slate-700 mb-1">
                Feedback Payload
              </label>
              <textarea
                id="broker-message"
                rows={4}
                value={producerText}
                onChange={(e) => setProducerText(e.target.value)}
                placeholder="Type passenger feedback (e.g. 'Train is delayed 15 minutes and coach AC is not working')..."
                className="w-full rounded-md border border-slate-200 bg-white p-3 text-xs text-slate-900 placeholder:text-slate-400 focus:outline-none focus:ring-2 focus:ring-sky-600"
              />
            </div>

            {producerError && (
              <div className="text-xs text-red-700 bg-red-50 border border-red-200 rounded-md p-2.5">
                {producerError}
              </div>
            )}
            {producerNotice && (
              <div className="text-xs text-emerald-800 bg-emerald-50 border border-emerald-200 rounded-md p-2.5 font-mono">
                {producerNotice}
              </div>
            )}

            <button
              type="submit"
              disabled={producing}
              className="w-full inline-flex items-center justify-center gap-1.5 h-9 px-4 rounded-md bg-sky-600 text-white text-xs font-medium hover:bg-sky-700 transition-colors cursor-pointer disabled:opacity-50"
            >
              <Send className="h-3.5 w-3.5" />
              {producing ? 'Publishing...' : 'Publish Event to Kafka'}
            </button>
          </form>
        </div>

        <div className="lg:col-span-7 bg-white border border-slate-200 rounded-lg overflow-hidden">
          <div className="px-5 py-4 border-b border-slate-200 flex items-center justify-between gap-4">
            <div>
              <h2 className="text-base font-semibold text-slate-900">
                03. Partition {selectedPartition} Offset Log
              </h2>
              <p className="text-xs text-slate-500 mt-0.5">
                Ordered append-only log for{' '}
                {snapshot.partitions.find((p) => p.partition === selectedPartition)?.label}
              </p>
            </div>
            <div className="inline-flex items-center p-0.5 bg-slate-100 rounded-md border border-slate-200">
              {([0, 1, 2] as const).map((pid) => (
                <button
                  key={pid}
                  type="button"
                  onClick={() => setSelectedPartition(pid)}
                  className={`px-2.5 py-1 text-xs font-mono font-medium rounded-sm cursor-pointer whitespace-nowrap shrink-0 ${
                    selectedPartition === pid
                      ? 'bg-white text-slate-900 shadow-xs'
                      : 'text-slate-600 hover:text-slate-900'
                  }`}
                >
                  P{pid}
                </button>
              ))}
            </div>
          </div>

          <div className="divide-y divide-slate-100 max-h-[420px] overflow-y-auto">
            {loadingPartition && partitionRecords.length === 0 ? (
              <div className="p-8 text-center text-xs text-slate-500">Loading partition log...</div>
            ) : partitionRecords.length === 0 ? (
              <div className="p-8 text-center text-xs text-slate-500">
                No messages recorded on Partition {selectedPartition} yet.
              </div>
            ) : (
              partitionRecords.slice(0, 20).map((rec) => (
                <button
                  key={rec.id}
                  type="button"
                  onClick={() => onInspectRecord(rec)}
                  className="w-full text-left px-5 py-3 hover:bg-slate-50 transition-colors flex items-start justify-between gap-4 cursor-pointer"
                >
                  <div className="space-y-1 min-w-0">
                    <div className="flex items-center gap-2 text-xs text-slate-500 font-mono tabular-nums">
                      <span className="font-semibold text-slate-800">#{rec.offset}</span>
                      <span aria-hidden="true">·</span>
                      <span>{rec.key}</span>
                      <span aria-hidden="true">·</span>
                      <span className="font-sans">{rec.route}</span>
                      <span aria-hidden="true">·</span>
                      <span>{new Date(rec.timestamp).toTimeString().slice(0, 8)}</span>
                    </div>
                    <p className="text-xs text-slate-800 line-clamp-2">{rec.text}</p>
                  </div>
                  <div className="text-right shrink-0 font-mono text-xs tabular-nums">
                    <div
                      className={`capitalize font-medium ${
                        rec.sentiment === 'positive'
                          ? 'text-emerald-700'
                          : rec.sentiment === 'negative'
                          ? 'text-red-700'
                          : 'text-amber-700'
                      }`}
                    >
                      {rec.sentiment}
                    </div>
                    <div className="text-slate-500 mt-0.5">
                      {rec.score >= 0 ? `+${rec.score.toFixed(2)}` : rec.score.toFixed(2)}
                    </div>
                  </div>
                </button>
              ))
            )}
          </div>
        </div>
      </section>
    </div>
  );
}
