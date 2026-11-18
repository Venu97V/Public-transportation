import { useEffect, useRef, useState } from 'react';
import { Send, X } from 'lucide-react';
import type { FeedbackSource, KafkaSentimentRecord } from '../types/kafka';

interface PublishModalProps {
  open: boolean;
  onClose: () => void;
  onPublish: (input: {
    text: string;
    route: string;
    source: FeedbackSource;
    useGemini?: boolean;
  }) => Promise<{ record: KafkaSentimentRecord }>;
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
  'feedback-form',
  'twitter',
  'app-review',
  'helpline',
];

const QUICK_EXAMPLES = [
  'Bus 12 delayed by 40 minutes again, absolutely unacceptable',
  'The metro was spotless today, really impressed',
  'Smart card top-up took seconds and the fares are reasonable',
];

export function PublishModal({ open, onClose, onPublish }: PublishModalProps) {
  const [text, setText] = useState('');
  const [route, setRoute] = useState(ROUTES[0]);
  const [source, setSource] = useState<FeedbackSource>('feedback-form');
  const [useGemini, setUseGemini] = useState(false);
  const [submitting, setSubmitting] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const textareaRef = useRef<HTMLTextAreaElement | null>(null);

  useEffect(() => {
    if (open) {
      const frame = window.requestAnimationFrame(() => {
        textareaRef.current?.focus();
      });
      return () => window.cancelAnimationFrame(frame);
    }
  }, [open]);

  if (!open) return null;

  const handleSubmit = async (e: React.FormEvent) => {
    e.preventDefault();
    if (text.trim().length < 3) {
      setError('Please enter at least 3 characters of passenger feedback.');
      return;
    }

    setSubmitting(true);
    setError(null);
    try {
      await onPublish({ text: text.trim(), route, source, useGemini });
      setText('');
      onClose();
    } catch (err) {
      setError(err instanceof Error ? err.message : 'Failed to publish event to Kafka topic');
    } finally {
      setSubmitting(false);
    }
  };

  return (
    <div
      className="fixed inset-0 z-50 flex items-center justify-center bg-slate-900/35 p-4"
      onClick={onClose}
    >
      <div
        className="w-full max-w-lg bg-white border border-slate-200 rounded-lg p-6 space-y-5"
        onClick={(e) => e.stopPropagation()}
        role="dialog"
        aria-modal="true"
        aria-labelledby="publish-modal-title"
      >
        <div className="flex items-start justify-between gap-4 border-b border-slate-200 pb-3.5">
          <div>
            <h2 id="publish-modal-title" className="text-base font-semibold text-slate-900">
              Publish Passenger Feedback to Kafka
            </h2>
            <p className="text-xs text-slate-500 mt-0.5">
              Produces a live event into <span className="font-mono">transport.feedback.raw</span>
            </p>
          </div>
          <button
            type="button"
            onClick={onClose}
            aria-label="Close dialog"
            className="p-1.5 rounded-md text-slate-500 hover:bg-slate-100 cursor-pointer"
          >
            <X className="h-4 w-4" />
          </button>
        </div>

        <form onSubmit={handleSubmit} className="space-y-4">
          <div className="grid grid-cols-1 sm:grid-cols-2 gap-3">
            <div>
              <label htmlFor="modal-route" className="block text-xs font-medium text-slate-700 mb-1">
                Transit Route / Asset
              </label>
              <select
                id="modal-route"
                value={route}
                onChange={(e) => setRoute(e.target.value)}
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
              <label htmlFor="modal-source" className="block text-xs font-medium text-slate-700 mb-1">
                Ingestion Channel
              </label>
              <select
                id="modal-source"
                value={source}
                onChange={(e) => setSource(e.target.value as FeedbackSource)}
                className="w-full h-9 rounded-md border border-slate-200 bg-white px-2.5 text-xs text-slate-800 focus:outline-none focus:ring-2 focus:ring-sky-600"
              >
                {SOURCES.map((s) => (
                  <option key={s} value={s}>
                    {s}
                  </option>
                ))}
              </select>
            </div>
          </div>

          <div>
            <label htmlFor="modal-text" className="block text-xs font-medium text-slate-700 mb-1">
              Passenger Feedback Message
            </label>
            <textarea
              ref={textareaRef}
              id="modal-text"
              rows={3}
              value={text}
              onChange={(e) => setText(e.target.value)}
              placeholder="Enter realistic passenger complaint, compliment, or operational report..."
              className="w-full rounded-md border border-slate-200 bg-white p-3 text-sm text-slate-900 placeholder:text-slate-400 focus:outline-none focus:ring-2 focus:ring-sky-600"
            />
            <div className="mt-2 flex flex-wrap gap-2">
              {QUICK_EXAMPLES.map((ex, i) => (
                <button
                  key={i}
                  type="button"
                  onClick={() => setText(ex)}
                  className="text-xs text-sky-700 hover:text-sky-900 underline underline-offset-2 cursor-pointer"
                >
                  Sample {i + 1}
                </button>
              ))}
            </div>
          </div>

          <label className="flex items-center gap-2 text-xs text-slate-700 cursor-pointer select-none">
            <input
              type="checkbox"
              checked={useGemini}
              onChange={(e) => setUseGemini(e.target.checked)}
              className="rounded border-slate-300 text-sky-600 focus:ring-sky-600"
            />
            Score using Gemini AI classifier (requires configured GEMINI_API_KEY)
          </label>

          {error && (
            <div className="text-xs text-red-700 bg-red-50 border border-red-200 rounded-md p-2.5">
              {error}
            </div>
          )}

          <div className="flex items-center justify-end gap-2.5 pt-2 border-t border-slate-200">
            <button
              type="button"
              onClick={onClose}
              className="h-9 px-4 rounded-md border border-slate-200 text-xs font-medium text-slate-700 hover:bg-slate-50 cursor-pointer"
            >
              Cancel
            </button>
            <button
              type="submit"
              disabled={submitting}
              className="inline-flex items-center gap-1.5 h-9 px-4 rounded-md bg-sky-600 text-white text-xs font-medium hover:bg-sky-700 transition-colors cursor-pointer disabled:opacity-50"
            >
              <Send className="h-3.5 w-3.5" />
              {submitting ? 'Producing...' : 'Produce to Topic'}
            </button>
          </div>
        </form>
      </div>
    </div>
  );
}
