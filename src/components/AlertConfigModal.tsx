import { useEffect, useState } from 'react';
import { Bell, X } from 'lucide-react';
import type { SentimentAlertRule } from '../types/kafka';

interface AlertConfigModalProps {
  open: boolean;
  rule: SentimentAlertRule;
  onClose: () => void;
  onSave: (nextRule: SentimentAlertRule) => void;
}

export const DEFAULT_ALERT_RULE: SentimentAlertRule = {
  enabled: true,
  sentimentTarget: 'negative',
  thresholdPct: 30,
  windowSeconds: 60,
  minSamples: 5,
};

const WINDOW_OPTIONS = [
  { seconds: 30, label: '30 sec' },
  { seconds: 60, label: '1 min' },
  { seconds: 120, label: '2 min' },
  { seconds: 300, label: '5 min' },
];

const THRESHOLD_PRESETS = [20, 30, 40, 50];

export function formatWindowLabel(seconds: number): string {
  if (seconds < 60) return `${seconds} seconds`;
  if (seconds === 60) return '1 minute';
  return `${Math.round(seconds / 60)} minutes`;
}

export function AlertConfigModal({ open, rule, onClose, onSave }: AlertConfigModalProps) {
  const [draft, setDraft] = useState<SentimentAlertRule>(rule);
  const [error, setError] = useState<string | null>(null);

  useEffect(() => {
    if (open) {
      setDraft(rule);
      setError(null);
    }
  }, [open, rule]);

  if (!open) return null;

  const handleSubmit = (e: React.FormEvent) => {
    e.preventDefault();
    if (
      Number.isNaN(draft.thresholdPct) ||
      draft.thresholdPct < 1 ||
      draft.thresholdPct > 100
    ) {
      setError('Threshold percentage must be between 1% and 100%.');
      return;
    }
    if (Number.isNaN(draft.minSamples) || draft.minSamples < 1 || draft.minSamples > 100) {
      setError('Minimum message count must be between 1 and 100.');
      return;
    }
    onSave({
      ...draft,
      thresholdPct: Math.round(draft.thresholdPct),
      minSamples: Math.round(draft.minSamples),
    });
    onClose();
  };

  return (
    <div
      className="fixed inset-0 z-50 flex items-center justify-center bg-slate-900/35 p-4"
      onClick={onClose}
    >
      <div
        className="w-full max-w-md bg-white border border-slate-200 rounded-lg p-5 space-y-4"
        onClick={(e) => e.stopPropagation()}
        role="dialog"
        aria-modal="true"
        aria-labelledby="alert-modal-title"
      >
        <div className="flex items-start justify-between gap-4 border-b border-slate-200 pb-3">
          <div className="flex items-center gap-2">
            <Bell className="h-4 w-4 text-sky-600 shrink-0" />
            <div>
              <h2 id="alert-modal-title" className="text-base font-semibold text-slate-900">
                Sentiment Threshold Alerts
              </h2>
              <p className="text-xs text-slate-500">
                Trigger an operational alert when stream sentiment crosses a threshold
              </p>
            </div>
          </div>
          <button
            type="button"
            onClick={onClose}
            aria-label="Close alert configuration"
            className="p-1.5 rounded-md text-slate-500 hover:bg-slate-100 cursor-pointer"
          >
            <X className="h-4 w-4" />
          </button>
        </div>

        <form onSubmit={handleSubmit} className="space-y-4">
          {/* Enable Switch */}
          <label className="flex items-center justify-between gap-3 border border-slate-200 rounded-md px-3.5 py-2.5 cursor-pointer select-none">
            <div>
              <div className="text-xs font-medium text-slate-900">Enable threshold monitoring</div>
              <div className="text-xs text-slate-500">
                Evaluate rolling Kafka stream events continuously
              </div>
            </div>
            <input
              type="checkbox"
              checked={draft.enabled}
              onChange={(e) => setDraft((prev) => ({ ...prev, enabled: e.target.checked }))}
              className="h-4 w-4 rounded border-slate-300 text-sky-600 focus:ring-sky-600"
            />
          </label>

          {/* Target Sentiment & Minimum Sample */}
          <div className="grid grid-cols-2 gap-3">
            <div>
              <label
                htmlFor="alert-sentiment-target"
                className="block text-xs font-medium text-slate-700 mb-1"
              >
                Target Sentiment
              </label>
              <select
                id="alert-sentiment-target"
                value={draft.sentimentTarget}
                onChange={(e) =>
                  setDraft((prev) => ({
                    ...prev,
                    sentimentTarget: e.target.value as 'negative' | 'positive',
                  }))
                }
                disabled={!draft.enabled}
                className="w-full h-9 rounded-md border border-slate-200 bg-white px-2.5 text-xs font-medium text-slate-800 focus:outline-none focus:ring-2 focus:ring-sky-600 disabled:opacity-50"
              >
                <option value="negative">Negative</option>
                <option value="positive">Positive</option>
              </select>
            </div>

            <div>
              <label
                htmlFor="alert-min-samples"
                className="block text-xs font-medium text-slate-700 mb-1"
              >
                Min. Events in Window
              </label>
              <input
                id="alert-min-samples"
                type="number"
                min={1}
                max={100}
                value={draft.minSamples}
                onChange={(e) =>
                  setDraft((prev) => ({ ...prev, minSamples: Number(e.target.value) }))
                }
                disabled={!draft.enabled}
                className="w-full h-9 rounded-md border border-slate-200 bg-white px-2.5 text-xs font-mono tabular-nums text-slate-800 focus:outline-none focus:ring-2 focus:ring-sky-600 disabled:opacity-50"
              />
            </div>
          </div>

          {/* Threshold % */}
          <div className="space-y-1.5">
            <div className="flex items-center justify-between">
              <label htmlFor="alert-threshold-pct" className="text-xs font-medium text-slate-700">
                Alert Threshold (%)
              </label>
              <span className="text-xs font-mono tabular-nums text-slate-500">
                &gt; {draft.thresholdPct || 0}%
              </span>
            </div>
            <div className="flex items-center gap-2">
              <input
                id="alert-threshold-pct"
                type="number"
                min={1}
                max={100}
                value={draft.thresholdPct}
                onChange={(e) =>
                  setDraft((prev) => ({ ...prev, thresholdPct: Number(e.target.value) }))
                }
                disabled={!draft.enabled}
                className="w-24 h-9 rounded-md border border-slate-200 bg-white px-2.5 text-xs font-mono tabular-nums text-slate-800 focus:outline-none focus:ring-2 focus:ring-sky-600 disabled:opacity-50"
              />
              <div className="flex items-center gap-1 flex-1 justify-end">
                {THRESHOLD_PRESETS.map((preset) => (
                  <button
                    key={preset}
                    type="button"
                    disabled={!draft.enabled}
                    onClick={() => setDraft((prev) => ({ ...prev, thresholdPct: preset }))}
                    className={`h-8 px-2.5 rounded-md border text-xs font-mono tabular-nums transition-colors cursor-pointer whitespace-nowrap shrink-0 disabled:opacity-50 ${
                      draft.thresholdPct === preset
                        ? 'border-sky-600 bg-sky-50 text-sky-700 font-medium'
                        : 'border-slate-200 bg-white text-slate-600 hover:bg-slate-50'
                    }`}
                  >
                    {preset}%
                  </button>
                ))}
              </div>
            </div>
          </div>

          {/* Rolling Window */}
          <div className="space-y-1.5">
            <div className="text-xs font-medium text-slate-700">Rolling Time Window</div>
            <div className="grid grid-cols-4 gap-1.5 p-1 bg-slate-100 rounded-md border border-slate-200">
              {WINDOW_OPTIONS.map((opt) => (
                <button
                  key={opt.seconds}
                  type="button"
                  disabled={!draft.enabled}
                  onClick={() => setDraft((prev) => ({ ...prev, windowSeconds: opt.seconds }))}
                  className={`py-1.5 text-xs font-medium rounded-sm transition-colors whitespace-nowrap shrink-0 cursor-pointer disabled:opacity-50 ${
                    draft.windowSeconds === opt.seconds
                      ? 'bg-white text-slate-900 shadow-xs'
                      : 'text-slate-600 hover:text-slate-900'
                  }`}
                >
                  {opt.label}
                </button>
              ))}
            </div>
          </div>

          {/* Rule Summary */}
          <div className="bg-slate-50 border border-slate-200 rounded-md px-3 py-2.5 text-xs text-slate-600 leading-relaxed">
            {draft.enabled ? (
              <>
                Alert when more than{' '}
                <strong className="font-mono tabular-nums text-slate-900">
                  {draft.thresholdPct}%
                </strong>{' '}
                of messages in the last{' '}
                <strong className="text-slate-900">{formatWindowLabel(draft.windowSeconds)}</strong>{' '}
                are <strong className="text-slate-900">{draft.sentimentTarget}</strong> (min.{' '}
                <span className="font-mono tabular-nums">{draft.minSamples}</span> events).
              </>
            ) : (
              'Threshold alerting is currently paused.'
            )}
          </div>

          {error && (
            <div className="text-xs text-red-700 bg-red-50 border border-red-200 rounded-md p-2.5">
              {error}
            </div>
          )}

          <div className="flex items-center justify-between gap-3 pt-2 border-t border-slate-200">
            <button
              type="button"
              onClick={() => setDraft(DEFAULT_ALERT_RULE)}
              className="text-xs text-slate-500 hover:text-slate-800 underline underline-offset-2 cursor-pointer"
            >
              Reset Default
            </button>

            <div className="flex items-center gap-2">
              <button
                type="button"
                onClick={onClose}
                className="h-9 px-3.5 rounded-md border border-slate-200 text-xs font-medium text-slate-700 hover:bg-slate-50 cursor-pointer whitespace-nowrap shrink-0"
              >
                Cancel
              </button>
              <button
                type="submit"
                className="h-9 px-4 rounded-md bg-sky-600 text-white text-xs font-medium hover:bg-sky-700 transition-colors cursor-pointer whitespace-nowrap shrink-0"
              >
                Save Rule
              </button>
            </div>
          </div>
        </form>
      </div>
    </div>
  );
}
