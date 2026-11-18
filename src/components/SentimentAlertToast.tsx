import { useEffect, useState } from 'react';
import { AnimatePresence, motion } from 'motion/react';
import { AlertTriangle, X } from 'lucide-react';
import { formatWindowLabel } from './AlertConfigModal';

export interface ActiveNegativeToast {
  id: string;
  triggeredAt: number;
  matchingPct: number;
  matchingCount: number;
  windowTotal: number;
  thresholdPct: number;
  windowSeconds: number;
  topAspect: string;
}

interface SentimentAlertToastProps {
  toast: ActiveNegativeToast | null;
  onDismiss: () => void;
  onAdjustRule: () => void;
}

const AUTO_DISMISS_MS = 8500;

export function SentimentAlertToast({
  toast,
  onDismiss,
  onAdjustRule,
}: SentimentAlertToastProps) {
  const [paused, setPaused] = useState(false);

  useEffect(() => {
    if (!toast || paused) return;
    const timer = window.setTimeout(() => {
      onDismiss();
    }, AUTO_DISMISS_MS);
    return () => window.clearTimeout(timer);
  }, [toast, paused, onDismiss]);

  return (
    <div
      className="fixed bottom-5 right-5 z-50 w-[calc(100vw-2.5rem)] max-w-sm pointer-events-none"
      aria-live="assertive"
      aria-atomic="true"
    >
      <AnimatePresence mode="wait">
        {toast && (
          <motion.div
            key={toast.id}
            initial={{ opacity: 0, y: 14 }}
            animate={{ opacity: 1, y: 0 }}
            exit={{ opacity: 0, y: 10 }}
            transition={{ duration: 0.18, ease: [0.16, 1, 0.3, 1] }}
            onMouseEnter={() => setPaused(true)}
            onMouseLeave={() => setPaused(false)}
            onFocus={() => setPaused(true)}
            onBlur={() => setPaused(false)}
            role="status"
            className="pointer-events-auto bg-white border border-red-200 rounded-lg shadow-sm p-4 space-y-2.5"
          >
            <div className="flex items-start justify-between gap-3">
              <div className="flex items-center gap-2 min-w-0">
                <AlertTriangle className="h-4 w-4 text-red-600 shrink-0" />
                <span className="text-xs font-semibold text-slate-900 truncate">
                  Negative Sentiment Threshold Exceeded
                </span>
              </div>

              <div className="flex items-center gap-2 shrink-0">
                <span className="text-[11px] font-mono tabular-nums text-slate-400">
                  {new Date(toast.triggeredAt).toTimeString().slice(0, 8)}
                </span>
                <button
                  type="button"
                  onClick={onDismiss}
                  aria-label="Close notification"
                  className="p-1 rounded-md text-slate-400 hover:text-slate-700 hover:bg-slate-100 transition-colors cursor-pointer"
                >
                  <X className="h-3.5 w-3.5" />
                </button>
              </div>
            </div>

            <div className="text-xs text-slate-700 leading-relaxed">
              <span className="font-mono tabular-nums font-semibold text-red-700">
                {toast.matchingPct.toFixed(1)}%
              </span>{' '}
              of messages in the last{' '}
              <span className="font-medium text-slate-900">
                {formatWindowLabel(toast.windowSeconds)}
              </span>{' '}
              are negative (
              <span className="font-mono tabular-nums">
                {toast.matchingCount}/{toast.windowTotal}
              </span>{' '}
              events, threshold{' '}
              <span className="font-mono tabular-nums">&gt;{toast.thresholdPct}%</span>).
              {toast.topAspect ? (
                <span className="block mt-1 text-slate-500">
                  Primary driver: <span className="text-slate-700 font-medium">{toast.topAspect}</span>
                </span>
              ) : null}
            </div>

            <div className="flex items-center justify-end gap-2 pt-1 border-t border-slate-100">
              <button
                type="button"
                onClick={onDismiss}
                className="h-8 px-3 rounded-md text-xs font-medium text-slate-600 hover:bg-slate-100 transition-colors whitespace-nowrap shrink-0 cursor-pointer"
              >
                Dismiss
              </button>
              <button
                type="button"
                onClick={() => {
                  onDismiss();
                  onAdjustRule();
                }}
                className="h-8 px-3 rounded-md bg-red-600 text-white text-xs font-medium hover:bg-red-700 transition-colors whitespace-nowrap shrink-0 cursor-pointer"
              >
                Adjust Threshold
              </button>
            </div>
          </motion.div>
        )}
      </AnimatePresence>
    </div>
  );
}
