import { useState } from 'react';
import { Sparkles, X } from 'lucide-react';
import type { GeminiDeepAnalysis, KafkaSentimentRecord } from '../types/kafka';

interface RecordInspectorDrawerProps {
  record: KafkaSentimentRecord | null;
  onClose: () => void;
  onAnalyzeWithGemini: (text: string, route?: string) => Promise<GeminiDeepAnalysis>;
}

export function RecordInspectorDrawer({
  record,
  onClose,
  onAnalyzeWithGemini,
}: RecordInspectorDrawerProps) {
  const [geminiResult, setGeminiResult] = useState<GeminiDeepAnalysis | null>(null);
  const [geminiLoading, setGeminiLoading] = useState(false);
  const [geminiError, setGeminiError] = useState<string | null>(null);
  const [lastRecordId, setLastRecordId] = useState<string | null>(null);

  if (!record) return null;

  if (lastRecordId !== record.id) {
    setLastRecordId(record.id);
    setGeminiResult(null);
    setGeminiError(null);
  }

  const handleRunGemini = async () => {
    setGeminiLoading(true);
    setGeminiError(null);
    try {
      const analysis = await onAnalyzeWithGemini(record.text, record.route);
      setGeminiResult(analysis);
    } catch (err) {
      setGeminiError(err instanceof Error ? err.message : 'Unable to run Gemini analysis');
    } finally {
      setGeminiLoading(false);
    }
  };

  const rawKafkaPayload = {
    topic: record.topic,
    partition: record.partition,
    offset: record.offset,
    key: record.key,
    timestamp: new Date(record.timestamp).toISOString(),
    consumerGroup: record.consumerGroup,
    consumerId: record.consumerId,
    headers: {
      source: record.source,
      route: record.route,
      mode: record.mode,
    },
    value: {
      text: record.text,
      sentiment: record.sentiment,
      score: record.score,
      confidence: record.confidence,
      aspect: record.aspect,
      matchedTerms: record.matchedTerms,
      engine: record.analyzerEngine,
      latencyMs: record.processingLatencyMs,
    },
  };

  return (
    <div
      className="fixed inset-0 z-50 flex justify-end bg-slate-900/30 backdrop-blur-[1px]"
      onClick={onClose}
    >
      <aside
        className="w-full max-w-lg bg-white border-l border-slate-200 h-full overflow-y-auto p-6 flex flex-col justify-between"
        onClick={(e) => e.stopPropagation()}
        aria-label="Kafka Record Inspector"
      >
        <div className="space-y-6">
          <div className="flex items-start justify-between gap-4 border-b border-slate-200 pb-4">
            <div>
              <div className="text-xs font-mono text-slate-500">
                Partition {record.partition} · Offset #{record.offset} · {record.key}
              </div>
              <h3 className="text-lg font-semibold text-slate-900 mt-1">
                Kafka Event Inspector
              </h3>
            </div>
            <button
              type="button"
              onClick={onClose}
              aria-label="Close inspector"
              className="p-2 rounded-md text-slate-500 hover:bg-slate-100 hover:text-slate-800 cursor-pointer"
            >
              <X className="h-4 w-4" />
            </button>
          </div>

          {/* Passenger Message */}
          <div className="space-y-2">
            <div className="text-xs font-medium text-slate-500">Passenger Feedback Message</div>
            <p className="text-sm text-slate-900 bg-slate-50 border border-slate-200 rounded-md p-3.5 leading-relaxed">
              {record.text}
            </p>
            <div className="flex flex-wrap items-center gap-2 text-xs text-slate-500">
              <span>{record.route}</span>
              <span aria-hidden="true">·</span>
              <span>{record.mode}</span>
              <span aria-hidden="true">·</span>
              <span>{record.source}</span>
              <span aria-hidden="true">·</span>
              <span className="font-mono tabular-nums">{record.processingLatencyMs} ms</span>
            </div>
          </div>

          {/* Classification Metrics */}
          <div className="grid grid-cols-2 gap-3">
            <div className="border border-slate-200 rounded-md p-3">
              <div className="text-xs text-slate-500">Sentiment & Polarity</div>
              <div className="mt-1 text-sm font-semibold capitalize text-slate-900 font-mono tabular-nums">
                {record.sentiment} ({record.score >= 0 ? `+${record.score}` : record.score})
              </div>
            </div>
            <div className="border border-slate-200 rounded-md p-3">
              <div className="text-xs text-slate-500">Primary Aspect</div>
              <div className="mt-1 text-sm font-semibold text-slate-900">
                {record.aspect}
              </div>
            </div>
          </div>

          {record.matchedTerms.length > 0 && (
            <div className="space-y-1.5">
              <div className="text-xs font-medium text-slate-500">Matched NLP Tokens</div>
              <div className="text-xs font-mono text-slate-700">
                {record.matchedTerms.join(' · ')}
              </div>
            </div>
          )}

          {/* Optional Gemini Deep AI Evaluation */}
          <div className="border border-slate-200 rounded-md p-4 space-y-3 bg-slate-50/50">
            <div className="flex items-center justify-between gap-3">
              <div>
                <div className="text-xs font-semibold text-slate-900">
                  Deep AI Operational Evaluation
                </div>
                <div className="text-xs text-slate-500">
                  Classify urgency and recommended transit dispatch action with Gemini
                </div>
              </div>
              <button
                type="button"
                onClick={handleRunGemini}
                disabled={geminiLoading}
                className="inline-flex items-center gap-1.5 h-8 px-3 rounded-md bg-sky-600 text-white text-xs font-medium hover:bg-sky-700 transition-colors whitespace-nowrap shrink-0 cursor-pointer disabled:opacity-50"
              >
                <Sparkles className="h-3.5 w-3.5" />
                {geminiLoading ? 'Analyzing...' : 'Analyze with Gemini'}
              </button>
            </div>

            {geminiError && (
              <div className="text-xs text-red-700 bg-red-50 border border-red-200 rounded-md p-2.5">
                {geminiError}
              </div>
            )}

            {geminiResult && (
              <div className="space-y-2.5 pt-2 border-t border-slate-200 text-xs">
                <div className="flex items-center justify-between text-slate-700 font-mono tabular-nums">
                  <span>
                    Sentiment: <strong className="capitalize">{geminiResult.sentiment}</strong> (
                    {geminiResult.score})
                  </span>
                  <span>
                    Urgency: <strong className="uppercase">{geminiResult.urgency}</strong>
                  </span>
                </div>
                <div>
                  <span className="font-medium text-slate-700">Summary: </span>
                  <span className="text-slate-600">{geminiResult.summary}</span>
                </div>
                <div>
                  <span className="font-medium text-slate-700">Recommended Dispatch Action: </span>
                  <span className="text-slate-800">{geminiResult.recommendedAction}</span>
                </div>
              </div>
            )}
          </div>

          {/* Raw Kafka JSON Payload */}
          <div className="space-y-2">
            <div className="text-xs font-medium text-slate-500">
              Serialized Kafka Consumer Record (JSON)
            </div>
            <pre className="bg-slate-900 text-slate-100 rounded-md p-3.5 text-xs font-mono overflow-x-auto leading-relaxed">
              {JSON.stringify(rawKafkaPayload, null, 2)}
            </pre>
          </div>
        </div>
      </aside>
    </div>
  );
}
