import { Keyboard, X } from 'lucide-react';
import { GLOBAL_SHORTCUTS, type ShortcutDefinition } from '../hooks/useGlobalShortcuts';

interface ShortcutCheatsheetModalProps {
  open: boolean;
  onClose: () => void;
}

const CATEGORIES: ShortcutDefinition['category'][] = [
  'Navigation',
  'Actions & Stream',
  'Overlays & Help',
];

export function ShortcutCheatsheetModal({ open, onClose }: ShortcutCheatsheetModalProps) {
  if (!open) return null;

  return (
    <div
      className="fixed inset-0 z-50 flex items-center justify-center bg-slate-900/35 p-4"
      onClick={onClose}
    >
      <div
        className="w-full max-w-lg bg-white border border-slate-200 rounded-lg p-5 space-y-5"
        onClick={(e) => e.stopPropagation()}
        role="dialog"
        aria-modal="true"
        aria-labelledby="cheatsheet-modal-title"
      >
        <div className="flex items-start justify-between gap-4 border-b border-slate-200 pb-3.5">
          <div className="flex items-center gap-2.5">
            <Keyboard className="h-4 w-4 text-sky-600 shrink-0" />
            <div>
              <h2 id="cheatsheet-modal-title" className="text-base font-semibold text-slate-900">
                Keyboard Shortcuts
              </h2>
              <p className="text-xs text-slate-500 mt-0.5">
                Single-key shortcuts active whenever focus is outside text inputs
              </p>
            </div>
          </div>
          <button
            type="button"
            onClick={onClose}
            aria-label="Close keyboard shortcuts"
            className="p-1.5 rounded-md text-slate-500 hover:bg-slate-100 cursor-pointer"
          >
            <X className="h-4 w-4" />
          </button>
        </div>

        <div className="space-y-5 max-h-[65vh] overflow-y-auto pr-1">
          {CATEGORIES.map((category) => {
            const items = GLOBAL_SHORTCUTS.filter((s) => s.category === category);
            return (
              <div key={category} className="space-y-2">
                <h3 className="text-xs font-semibold text-slate-500">{category}</h3>
                <div className="divide-y divide-slate-100 border border-slate-200 rounded-md">
                  {items.map((item) => (
                    <div
                      key={item.id}
                      className="px-3.5 py-2.5 flex items-center justify-between gap-4"
                    >
                      <div className="min-w-0">
                        <div className="text-xs font-medium text-slate-900">{item.label}</div>
                        <div className="text-xs text-slate-500">{item.description}</div>
                      </div>
                      <div className="flex items-center gap-1.5 shrink-0">
                        {item.keys.map((keyLabel, idx) => (
                          <span key={keyLabel} className="inline-flex items-center gap-1">
                            {idx > 0 && <span className="text-[11px] text-slate-400">/</span>}
                            <kbd className="inline-flex items-center justify-center min-w-[24px] h-6 px-1.5 rounded-sm border border-slate-200 bg-slate-50 font-mono text-[11px] font-medium tabular-nums text-slate-700">
                              {keyLabel}
                            </kbd>
                          </span>
                        ))}
                      </div>
                    </div>
                  ))}
                </div>
              </div>
            );
          })}
        </div>

        <div className="flex items-center justify-between gap-3 pt-2 border-t border-slate-200 text-xs text-slate-500">
          <span>Press Esc at any time to close an active modal or inspector</span>
          <button
            type="button"
            onClick={onClose}
            className="h-8 px-3.5 rounded-md bg-slate-900 text-white text-xs font-medium hover:bg-slate-800 transition-colors cursor-pointer whitespace-nowrap shrink-0"
          >
            Close
          </button>
        </div>
      </div>
    </div>
  );
}
