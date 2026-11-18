import { useEffect, useMemo, useRef, useState } from 'react';
import {
  AlertCircle,
  AlertTriangle,
  Bell,
  CheckCircle2,
  Eye,
  MinusCircle,
  Pause,
  Play,
  Search,
  Zap,
} from 'lucide-react';
import type {
  BrokerSnapshot,
  KafkaSentimentRecord,
  SentimentAlertRule,
  SentimentLabel,
  StreamScenario,
  TransitMode,
} from '../types/kafka';
import {
  AspectBreakdownPanel,
  SentimentRatioChart,
  SentimentTimelineChart,
} from './SentimentCharts';
import {
  AlertConfigModal,
  DEFAULT_ALERT_RULE,
  formatWindowLabel,
} from './AlertConfigModal';
import {
  SentimentAlertToast,
  type ActiveNegativeToast,
} from './SentimentAlertToast';

interface LiveMonitorViewProps {
  snapshot: BrokerSnapshot;
  wsConnected: boolean;
  onToggleStream: (streaming: boolean) => Promise<unknown>;
  onChangeScenario: (scenario: StreamScenario) => Promise<unknown>;
  onTriggerBurst: () => Promise<unknown>;
  onInspectRecord: (record: KafkaSentimentRecord) => void;
  alertModalOpen?: boolean;
  onAlertModalOpenChange?: (open: boolean) => void;
}

const SCENARIO_LABELS: Record<StreamScenario, string> = {
  normal: 'Normal Commute',
  'signal-delay': 'Signal Failure Delay',
  'overcrowding-ac': 'Overcrowding & AC Outage',
  'express-launch': 'Express Service Praise',
};

function SentimentCell({ sentiment }: { sentiment: SentimentLabel }) {
  if (sentiment === 'positive') {
    return (
      <span className="inline-flex items-center gap-1.5 text-xs font-medium text-emerald-700 whitespace-nowrap">
        <CheckCircle2 className="h-3.5 w-3.5 text-emerald-600 shrink-0" />
        Positive
      </span>
    );
  }
  if (sentiment === 'negative') {
    return (
      <span className="inline-flex items-center gap-1.5 text-xs font-medium text-red-700 whitespace-nowrap">
        <AlertCircle className="h-3.5 w-3.5 text-red-600 shrink-0" />
        Negative
      </span>
    );
  }
  return (
    <span className="inline-flex items-center gap-1.5 text-xs font-medium text-amber-700 whitespace-nowrap">
      <MinusCircle className="h-3.5 w-3.5 text-amber-600 shrink-0" />
      Neutral
    </span>
  );
}

const ALERT_STORAGE_KEY = 'transitpulse_alert_rule_v1';

function loadSavedAlertRule(): SentimentAlertRule {
  try {
    const raw = window.localStorage.getItem(ALERT_STORAGE_KEY);
    if (!raw) return DEFAULT_ALERT_RULE;
    const parsed = JSON.parse(raw) as Partial<SentimentAlertRule>;
    return {
      enabled: typeof parsed.enabled === 'boolean' ? parsed.enabled : DEFAULT_ALERT_RULE.enabled,
      sentimentTarget:
        parsed.sentimentTarget === 'positive' || parsed.sentimentTarget === 'negative'
          ? parsed.sentimentTarget
          : DEFAULT_ALERT_RULE.sentimentTarget,
      thresholdPct:
        typeof parsed.thresholdPct === 'number' && parsed.thresholdPct >= 1 && parsed.thresholdPct <= 100
          ? parsed.thresholdPct
          : DEFAULT_ALERT_RULE.thresholdPct,
      windowSeconds:
        typeof parsed.windowSeconds === 'number' && parsed.windowSeconds >= 10
          ? parsed.windowSeconds
          : DEFAULT_ALERT_RULE.windowSeconds,
      minSamples:
        typeof parsed.minSamples === 'number' && parsed.minSamples >= 1
          ? parsed.minSamples
          : DEFAULT_ALERT_RULE.minSamples,
    };
  } catch {
    return DEFAULT_ALERT_RULE;
  }
}

export function LiveMonitorView({
  snapshot,
  wsConnected,
  onToggleStream,
  onChangeScenario,
  onTriggerBurst,
  onInspectRecord,
  alertModalOpen: controlledAlertModalOpen,
  onAlertModalOpenChange,
}: LiveMonitorViewProps) {
  const [sentimentFilter, setSentimentFilter] = useState<'all' | SentimentLabel>('all');
  const [modeFilter, setModeFilter] = useState<'all' | TransitMode>('all');
  const [searchQuery, setSearchQuery] = useState('');
  const [busyAction, setBusyAction] = useState(false);

  const [alertRule, setAlertRule] = useState<SentimentAlertRule>(() => loadSavedAlertRule());
  const [internalAlertModalOpen, setInternalAlertModalOpen] = useState(false);
  const alertModalOpen = controlledAlertModalOpen ?? internalAlertModalOpen;
  const setAlertModalOpen = (next: boolean) => {
    setInternalAlertModalOpen(next);
    onAlertModalOpenChange?.(next);
  };
  const [alertDismissed, setAlertDismissed] = useState(false);
  const [activeToast, setActiveToast] = useState<ActiveNegativeToast | null>(null);
  const prevNegativeBreachedRef = useRef(false);
  const [nowMs, setNowMs] = useState(() => Date.now());

  useEffect(() => {
    const id = window.setInterval(() => {
      setNowMs(Date.now());
    }, 1000);
    return () => window.clearInterval(id);
  }, []);

  const alertEvaluation = useMemo(() => {
    const cutoff = nowMs - alertRule.windowSeconds * 1000;
    const windowEvents = snapshot.recentEvents.filter((evt) => evt.timestamp >= cutoff);
    const windowTotal = windowEvents.length;
    const matchingEvents = windowEvents.filter(
      (evt) => evt.sentiment === alertRule.sentimentTarget
    );
    const matchingCount = matchingEvents.length;
    const matchingPct =
      windowTotal > 0 ? Number(((matchingCount / windowTotal) * 100).toFixed(1)) : 0;

    const triggered =
      alertRule.enabled &&
      windowTotal >= alertRule.minSamples &&
      matchingPct > alertRule.thresholdPct;

    let topAspect = '';
    if (matchingEvents.length > 0) {
      const aspectCounts = new Map<string, number>();
      for (const evt of matchingEvents) {
        aspectCounts.set(evt.aspect, (aspectCounts.get(evt.aspect) ?? 0) + 1);
      }
      let bestCount = -1;
      for (const [asp, cnt] of aspectCounts.entries()) {
        if (cnt > bestCount) {
          bestCount = cnt;
          topAspect = asp;
        }
      }
    }

    return {
      windowTotal,
      matchingCount,
      matchingPct,
      triggered,
      topAspect,
    };
  }, [snapshot.recentEvents, alertRule, nowMs]);

  useEffect(() => {
    if (!alertEvaluation.triggered && alertDismissed) {
      setAlertDismissed(false);
    }
  }, [alertEvaluation.triggered, alertDismissed]);

  const isNegativeBreached =
    alertRule.enabled &&
    alertRule.sentimentTarget === 'negative' &&
    alertEvaluation.triggered;

  useEffect(() => {
    if (isNegativeBreached && !prevNegativeBreachedRef.current) {
      const triggeredAt = Date.now();
      setActiveToast({
        id: `toast-${triggeredAt}-${alertEvaluation.matchingCount}`,
        triggeredAt,
        matchingPct: alertEvaluation.matchingPct,
        matchingCount: alertEvaluation.matchingCount,
        windowTotal: alertEvaluation.windowTotal,
        thresholdPct: alertRule.thresholdPct,
        windowSeconds: alertRule.windowSeconds,
        topAspect: alertEvaluation.topAspect,
      });
    } else if (!isNegativeBreached) {
      setActiveToast(null);
    }
    prevNegativeBreachedRef.current = isNegativeBreached;
  }, [
    isNegativeBreached,
    alertEvaluation.matchingCount,
    alertEvaluation.matchingPct,
    alertEvaluation.windowTotal,
    alertEvaluation.topAspect,
    alertRule.thresholdPct,
    alertRule.windowSeconds,
  ]);

  const handleSaveAlertRule = (nextRule: SentimentAlertRule) => {
    prevNegativeBreachedRef.current = false;
    setAlertRule(nextRule);
    setAlertDismissed(false);
    try {
      window.localStorage.setItem(ALERT_STORAGE_KEY, JSON.stringify(nextRule));
    } catch {
      // Ignore storage errors
    }
  };

  const total = Math.max(1, snapshot.totalConsumed);
  const negativeShare = ((snapshot.sentimentCounts.negative / total) * 100).toFixed(1);
  const positiveShare = ((snapshot.sentimentCounts.positive / total) * 100).toFixed(1);

  const filteredEvents = useMemo(() => {
    const q = searchQuery.trim().toLowerCase();
    return snapshot.recentEvents.filter((evt) => {
      if (sentimentFilter !== 'all' && evt.sentiment !== sentimentFilter) return false;
      if (modeFilter !== 'all' && evt.mode !== modeFilter) return false;
      if (
        q &&
        !evt.text.toLowerCase().includes(q) &&
        !evt.route.toLowerCase().includes(q) &&
        !evt.aspect.toLowerCase().includes(q)
      ) {
        return false;
      }
      return true;
    });
  }, [snapshot.recentEvents, sentimentFilter, modeFilter, searchQuery]);

  const handleToggle = async () => {
    setBusyAction(true);
    try {
      await onToggleStream(!snapshot.streaming);
    } finally {
      setBusyAction(false);
    }
  };

  const handleBurst = async () => {
    setBusyAction(true);
    try {
      await onTriggerBurst();
    } finally {
      setBusyAction(false);
    }
  };

  return (
    <div className="space-y-6">
      {/* Stream Control Bar */}
      <section className="bg-white border border-slate-200 rounded-lg px-5 py-4 flex flex-wrap items-center justify-between gap-4">
        <div className="space-y-1">
          <div className="flex flex-wrap items-center gap-2.5 text-xs text-slate-500">
            <span className="font-medium text-slate-700">Topic: {snapshot.rawTopic}</span>
            <span aria-hidden="true">·</span>
            <span>Group: {snapshot.consumerGroup}</span>
            <span aria-hidden="true">·</span>
            <span className="font-mono tabular-nums">
              {wsConnected ? 'WebSocket Connected' : 'Reconnecting...'}
            </span>
            <span aria-hidden="true">·</span>
            <span className="font-mono tabular-nums text-slate-600">
              {alertRule.enabled
                ? `Alert rule: >${alertRule.thresholdPct}% ${alertRule.sentimentTarget} / ${formatWindowLabel(alertRule.windowSeconds)} (${alertEvaluation.matchingPct.toFixed(1)}% now)`
                : 'Alerts paused'}
            </span>
          </div>
          <p className="text-sm text-slate-600">
            Continuous ingestion of passenger feedback across 3 Kafka partitions with real-time sentiment classification.
          </p>
        </div>

        <div className="flex flex-wrap items-center gap-2.5">
          <label className="sr-only" htmlFor="scenario-select">
            Stream Scenario
          </label>
          <select
            id="scenario-select"
            value={snapshot.scenario}
            onChange={(e) => onChangeScenario(e.target.value as StreamScenario)}
            className="h-9 rounded-md border border-slate-200 bg-white px-3 text-xs font-medium text-slate-700 focus:outline-none focus:ring-2 focus:ring-sky-600"
          >
            {(Object.keys(SCENARIO_LABELS) as StreamScenario[]).map((key) => (
              <option key={key} value={key}>
                Scenario: {SCENARIO_LABELS[key]}
              </option>
            ))}
          </select>

          <button
            type="button"
            onClick={() => setAlertModalOpen(true)}
            aria-keyshortcuts="a"
            className={`inline-flex items-center gap-1.5 h-9 px-3.5 rounded-md border text-xs font-medium transition-colors whitespace-nowrap shrink-0 cursor-pointer ${
              alertEvaluation.triggered
                ? 'border-red-300 bg-red-50 text-red-700 hover:bg-red-100'
                : 'border-slate-200 bg-white text-slate-700 hover:bg-slate-50'
            }`}
          >
            <Bell
              className={`h-3.5 w-3.5 ${
                alertEvaluation.triggered ? 'text-red-600' : 'text-slate-500'
              }`}
            />
            <span>Configure Alerts</span>
            <kbd
              className={`ml-0.5 inline-flex items-center justify-center h-4 px-1 rounded-sm border font-mono text-[10px] leading-none ${
                alertEvaluation.triggered
                  ? 'border-red-200 bg-white/80 text-red-700'
                  : 'border-slate-200 bg-slate-50 text-slate-500'
              }`}
            >
              A
            </kbd>
          </button>

          <button
            type="button"
            onClick={handleBurst}
            disabled={busyAction}
            className="inline-flex items-center gap-1.5 h-9 px-3.5 rounded-md border border-slate-200 bg-white text-xs font-medium text-slate-700 hover:bg-slate-50 transition-colors whitespace-nowrap shrink-0 cursor-pointer disabled:opacity-50"
          >
            <Zap className="h-3.5 w-3.5 text-sky-600" />
            Inject Burst (6)
          </button>

          <button
            type="button"
            onClick={handleToggle}
            disabled={busyAction}
            aria-keyshortcuts="s"
            className={`inline-flex items-center gap-1.5 h-9 px-4 rounded-md text-xs font-medium transition-colors whitespace-nowrap shrink-0 cursor-pointer disabled:opacity-50 ${
              snapshot.streaming
                ? 'bg-slate-900 text-white hover:bg-slate-800'
                : 'bg-sky-600 text-white hover:bg-sky-700'
            }`}
          >
            {snapshot.streaming ? (
              <>
                <Pause className="h-3.5 w-3.5" />
                <span>Pause Stream</span>
              </>
            ) : (
              <>
                <Play className="h-3.5 w-3.5" />
                <span>Resume Stream</span>
              </>
            )}
            <kbd className="ml-0.5 inline-flex items-center justify-center h-4 px-1 rounded-sm border border-white/20 bg-white/10 font-mono text-[10px] leading-none text-white/90">
              S
            </kbd>
          </button>
        </div>
      </section>

      {/* Active Sentiment Threshold Alert Banner */}
      {alertEvaluation.triggered && !alertDismissed && (
        <section
          role="alert"
          className={`border rounded-lg px-5 py-3.5 flex flex-wrap items-center justify-between gap-4 ${
            alertRule.sentimentTarget === 'negative'
              ? 'bg-red-50 border-red-200 text-red-900'
              : 'bg-emerald-50 border-emerald-200 text-emerald-900'
          }`}
        >
          <div className="flex items-start gap-3">
            <AlertTriangle
              className={`h-4 w-4 mt-0.5 shrink-0 ${
                alertRule.sentimentTarget === 'negative' ? 'text-red-600' : 'text-emerald-600'
              }`}
            />
            <div className="space-y-0.5 text-xs">
              <div className="font-semibold">
                Sentiment Threshold Exceeded:{' '}
                <span className="font-mono tabular-nums">
                  {alertEvaluation.matchingPct.toFixed(1)}%
                </span>{' '}
                {alertRule.sentimentTarget} messages in the last{' '}
                {formatWindowLabel(alertRule.windowSeconds)}
              </div>
              <div
                className={
                  alertRule.sentimentTarget === 'negative' ? 'text-red-800' : 'text-emerald-800'
                }
              >
                <span className="font-mono tabular-nums">
                  {alertEvaluation.matchingCount} of {alertEvaluation.windowTotal}
                </span>{' '}
                events exceeded your configured{' '}
                <span className="font-mono tabular-nums">&gt;{alertRule.thresholdPct}%</span>{' '}
                threshold
                {alertEvaluation.topAspect
                  ? ` · Primary aspect: ${alertEvaluation.topAspect}`
                  : ''}
              </div>
            </div>
          </div>

          <div className="flex items-center gap-2 shrink-0">
            <button
              type="button"
              onClick={() => setAlertModalOpen(true)}
              className={`h-8 px-3 rounded-md border bg-white text-xs font-medium transition-colors whitespace-nowrap shrink-0 cursor-pointer ${
                alertRule.sentimentTarget === 'negative'
                  ? 'border-red-200 text-red-800 hover:bg-red-100/60'
                  : 'border-emerald-200 text-emerald-800 hover:bg-emerald-100/60'
              }`}
            >
              Adjust Threshold
            </button>
            <button
              type="button"
              onClick={() => setAlertDismissed(true)}
              className={`h-8 px-3 rounded-md text-xs font-medium transition-colors whitespace-nowrap shrink-0 cursor-pointer ${
                alertRule.sentimentTarget === 'negative'
                  ? 'text-red-700 hover:bg-red-100'
                  : 'text-emerald-700 hover:bg-emerald-100'
              }`}
            >
              Dismiss
            </button>
          </div>
        </section>
      )}

      {/* 4-Column KPI Strip */}
      <section className="grid grid-cols-1 sm:grid-cols-2 xl:grid-cols-4 gap-4">
        <div className="bg-white border border-slate-200 rounded-lg p-5">
          <div className="text-xs font-medium text-slate-500">Kafka Events Consumed</div>
          <div className="mt-2 flex items-baseline justify-between gap-2">
            <span className="text-2xl font-semibold font-mono tabular-nums text-slate-900">
              {snapshot.totalConsumed.toLocaleString()}
            </span>
            <span className="text-xs font-mono tabular-nums text-slate-500">
              {snapshot.ratePerSec.toFixed(1)} msg/s
            </span>
          </div>
          <div className="mt-2 text-xs text-slate-500">
            0 uncommitted lag across 3 partitions
          </div>
        </div>

        <div className="bg-white border border-slate-200 rounded-lg p-5">
          <div className="text-xs font-medium text-slate-500">Net Sentiment Index</div>
          <div className="mt-2 flex items-baseline justify-between gap-2">
            <span
              className={`text-2xl font-semibold font-mono tabular-nums ${
                snapshot.netSentimentScore >= 0 ? 'text-emerald-700' : 'text-red-700'
              }`}
            >
              {snapshot.netSentimentScore >= 0
                ? `+${snapshot.netSentimentScore}`
                : snapshot.netSentimentScore}
            </span>
            <span className="text-xs font-mono tabular-nums text-emerald-700">
              {positiveShare}% positive
            </span>
          </div>
          <div className="mt-2 text-xs text-slate-500">
            Scale from -100 (all negative) to +100
          </div>
        </div>

        <div className="bg-white border border-slate-200 rounded-lg p-5">
          <div className="text-xs font-medium text-slate-500">Negative Alert Share</div>
          <div className="mt-2 flex items-baseline justify-between gap-2">
            <span className="text-2xl font-semibold font-mono tabular-nums text-red-700">
              {negativeShare}%
            </span>
            <span className="text-xs font-mono tabular-nums text-slate-500">
              {snapshot.sentimentCounts.negative} complaints
            </span>
          </div>
          <div className="mt-2 text-xs text-slate-500">
            Top driver: {snapshot.aspects[0]?.aspect ?? 'Delays'}
          </div>
        </div>

        <div className="bg-white border border-slate-200 rounded-lg p-5">
          <div className="text-xs font-medium text-slate-500">Mean Consumer Latency</div>
          <div className="mt-2 flex items-baseline justify-between gap-2">
            <span className="text-2xl font-semibold font-mono tabular-nums text-slate-900">
              {snapshot.avgLatencyMs.toFixed(1)} ms
            </span>
            <span className="text-xs font-mono tabular-nums text-slate-500">
              3 workers
            </span>
          </div>
          <div className="mt-2 text-xs text-slate-500">
            Tokenization & aspect classification time
          </div>
        </div>
      </section>

      {/* Main Analytics Grid: Rolling Timeline + Circular Ratio + Issue Breakdown */}
      <section className="grid grid-cols-1 md:grid-cols-12 gap-6 items-stretch">
        <div className="md:col-span-12 xl:col-span-6 bg-white border border-slate-200 rounded-lg p-5 flex flex-col justify-between">
          <div className="mb-3">
            <h2 className="text-base font-semibold text-slate-900">
              01. Real-Time Sentiment Throughput
            </h2>
            <p className="text-xs text-slate-500 mt-0.5">
              Rolling 8-second tumbling windows from scored topic <span className="font-mono">{snapshot.scoredTopic}</span>
            </p>
          </div>
          <SentimentTimelineChart timeline={snapshot.timeline} />
        </div>

        <div className="md:col-span-6 xl:col-span-3 bg-white border border-slate-200 rounded-lg p-5 flex flex-col justify-between">
          <div className="mb-3">
            <h2 className="text-base font-semibold text-slate-900">
              02. Sentiment Ratio
            </h2>
            <p className="text-xs text-slate-500 mt-0.5">
              Live share across all consumed events
            </p>
          </div>
          <SentimentRatioChart
            positive={snapshot.sentimentCounts.positive}
            neutral={snapshot.sentimentCounts.neutral}
            negative={snapshot.sentimentCounts.negative}
          />
        </div>

        <div className="md:col-span-6 xl:col-span-3 bg-white border border-slate-200 rounded-lg p-5">
          <div className="mb-4">
            <h2 className="text-base font-semibold text-slate-900">
              03. Detected Issue Aspects
            </h2>
            <p className="text-xs text-slate-500 mt-0.5">
              Complaint & praise distribution by category
            </p>
          </div>
          <AspectBreakdownPanel aspects={snapshot.aspects} />
        </div>
      </section>

      {/* Live Scored Kafka Stream Table */}
      <section className="bg-white border border-slate-200 rounded-lg overflow-hidden">
        <div className="px-5 py-4 border-b border-slate-200 flex flex-wrap items-center justify-between gap-4">
          <div>
            <h2 className="text-base font-semibold text-slate-900">
              04. Live Scored Event Stream
            </h2>
            <p className="text-xs text-slate-500 mt-0.5">
              Showing {filteredEvents.length} of {snapshot.recentEvents.length} recent records · Click any row to inspect Kafka metadata or run Gemini AI analysis
            </p>
          </div>

          <div className="flex flex-wrap items-center gap-2.5">
            {/* Search */}
            <div className="relative">
              <Search className="h-3.5 w-3.5 text-slate-400 absolute left-3 top-1/2 -translate-y-1/2 pointer-events-none" />
              <input
                type="text"
                value={searchQuery}
                onChange={(e) => setSearchQuery(e.target.value)}
                placeholder="Filter text or route..."
                className="h-9 w-52 rounded-md border border-slate-200 bg-white pl-8 pr-3 text-xs text-slate-800 placeholder:text-slate-400 focus:outline-none focus:ring-2 focus:ring-sky-600"
              />
            </div>

            {/* Sentiment Filter Segmented Control */}
            <div className="inline-flex items-center p-0.5 bg-slate-100 rounded-md border border-slate-200">
              {(['all', 'positive', 'neutral', 'negative'] as const).map((val) => (
                <button
                  key={val}
                  type="button"
                  onClick={() => setSentimentFilter(val)}
                  className={`px-2.5 py-1 text-xs font-medium rounded-sm transition-colors capitalize whitespace-nowrap shrink-0 cursor-pointer ${
                    sentimentFilter === val
                      ? 'bg-white text-slate-900 shadow-xs'
                      : 'text-slate-600 hover:text-slate-900'
                  }`}
                >
                  {val}
                </button>
              ))}
            </div>

            {/* Mode Filter */}
            <select
              aria-label="Filter by transit mode"
              value={modeFilter}
              onChange={(e) => setModeFilter(e.target.value as 'all' | TransitMode)}
              className="h-9 rounded-md border border-slate-200 bg-white px-2.5 text-xs font-medium text-slate-700 focus:outline-none focus:ring-2 focus:ring-sky-600"
            >
              <option value="all">All Modes</option>
              <option value="metro">Metro</option>
              <option value="bus">Bus</option>
              <option value="train">Train</option>
            </select>
          </div>
        </div>

        <div className="overflow-x-auto">
          <table className="w-full text-left border-collapse">
            <thead>
              <tr className="border-b border-slate-200 bg-slate-50/70 text-xs font-medium text-slate-500">
                <th className="py-2.5 px-4 whitespace-nowrap">Time</th>
                <th className="py-2.5 px-3 whitespace-nowrap">Offset</th>
                <th className="py-2.5 px-3 whitespace-nowrap">Route & Source</th>
                <th className="py-2.5 px-3 whitespace-nowrap">Passenger Feedback</th>
                <th className="py-2.5 px-3 whitespace-nowrap">Aspect</th>
                <th className="py-2.5 px-3 whitespace-nowrap">Sentiment</th>
                <th className="py-2.5 px-3 text-right whitespace-nowrap">Score</th>
                <th className="py-2.5 px-4 text-right whitespace-nowrap">Inspect</th>
              </tr>
            </thead>
            <tbody className="divide-y divide-slate-100 text-sm">
              {filteredEvents.length === 0 ? (
                <tr>
                  <td colSpan={8} className="py-10 text-center text-sm text-slate-500">
                    No Kafka records match your current filter. Clear filters or resume stream.
                  </td>
                </tr>
              ) : (
                filteredEvents.slice(0, 25).map((evt) => (
                  <tr
                    key={evt.id}
                    onClick={() => onInspectRecord(evt)}
                    className="hover:bg-slate-50/80 transition-colors cursor-pointer"
                  >
                    <td className="py-2.5 px-4 font-mono text-xs tabular-nums text-slate-500 whitespace-nowrap">
                      {new Date(evt.timestamp).toTimeString().slice(0, 8)}
                    </td>
                    <td className="py-2.5 px-3 font-mono text-xs tabular-nums text-slate-700 whitespace-nowrap">
                      P{evt.partition} · #{evt.offset}
                    </td>
                    <td className="py-2.5 px-3 whitespace-nowrap">
                      <div className="text-xs font-medium text-slate-900">{evt.route}</div>
                      <div className="text-xs text-slate-500">{evt.source}</div>
                    </td>
                    <td className="py-2.5 px-3 max-w-md">
                      <p className="text-xs text-slate-700 line-clamp-2 leading-relaxed">
                        {evt.text}
                      </p>
                    </td>
                    <td className="py-2.5 px-3 text-xs text-slate-600 whitespace-nowrap">
                      {evt.aspect}
                    </td>
                    <td className="py-2.5 px-3 whitespace-nowrap">
                      <SentimentCell sentiment={evt.sentiment} />
                    </td>
                    <td className="py-2.5 px-3 text-right font-mono text-xs tabular-nums whitespace-nowrap">
                      <span
                        className={
                          evt.score > 0.15
                            ? 'text-emerald-700'
                            : evt.score < -0.15
                            ? 'text-red-700'
                            : 'text-slate-600'
                        }
                      >
                        {evt.score >= 0 ? `+${evt.score.toFixed(2)}` : evt.score.toFixed(2)}
                      </span>
                    </td>
                    <td className="py-2.5 px-4 text-right whitespace-nowrap">
                      <button
                        type="button"
                        onClick={(e) => {
                          e.stopPropagation();
                          onInspectRecord(evt);
                        }}
                        className="inline-flex items-center gap-1 text-xs font-medium text-sky-700 hover:text-sky-900 cursor-pointer"
                      >
                        <Eye className="h-3.5 w-3.5" />
                        Inspect
                      </button>
                    </td>
                  </tr>
                ))
              )}
            </tbody>
          </table>
        </div>
      </section>

      <AlertConfigModal
        open={alertModalOpen}
        rule={alertRule}
        onClose={() => setAlertModalOpen(false)}
        onSave={handleSaveAlertRule}
      />

      <SentimentAlertToast
        toast={activeToast}
        onDismiss={() => setActiveToast(null)}
        onAdjustRule={() => setAlertModalOpen(true)}
      />
    </div>
  );
}
