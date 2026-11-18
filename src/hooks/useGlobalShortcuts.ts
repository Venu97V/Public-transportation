import { useEffect } from 'react';

export interface ShortcutDefinition {
  id: string;
  keys: string[];
  label: string;
  description: string;
  category: 'Navigation' | 'Actions & Stream' | 'Overlays & Help';
}

export const GLOBAL_SHORTCUTS: ShortcutDefinition[] = [
  {
    id: 'nav-monitor',
    keys: ['M'],
    label: 'Live Monitor',
    description: 'Switch to the Live Monitor dashboard',
    category: 'Navigation',
  },
  {
    id: 'nav-broker',
    keys: ['B'],
    label: 'Kafka Broker',
    description: 'Switch to Partition & Producer Broker view',
    category: 'Navigation',
  },
  {
    id: 'nav-routes',
    keys: ['R'],
    label: 'Route Analytics',
    description: 'Switch to Transit Route Sentiment matrix',
    category: 'Navigation',
  },
  {
    id: 'nav-code',
    keys: ['C'],
    label: 'Project & Code',
    description: 'Switch to BDT architecture & source scripts',
    category: 'Navigation',
  },
  {
    id: 'action-publish',
    keys: ['P'],
    label: 'Publish Feedback',
    description: 'Open modal to produce a new passenger event',
    category: 'Actions & Stream',
  },
  {
    id: 'action-alerts',
    keys: ['A'],
    label: 'Configure Alerts',
    description: 'Open Sentiment Threshold Alerts settings',
    category: 'Actions & Stream',
  },
  {
    id: 'action-stream',
    keys: ['S', 'Space'],
    label: 'Pause / Resume Stream',
    description: 'Toggle live Kafka event stream ingestion',
    category: 'Actions & Stream',
  },
  {
    id: 'overlay-help',
    keys: ['?'],
    label: 'Shortcut Cheatsheet',
    description: 'Open or close this keyboard reference',
    category: 'Overlays & Help',
  },
  {
    id: 'overlay-esc',
    keys: ['Esc'],
    label: 'Dismiss Active Overlay',
    description: 'Close the topmost modal or event inspector',
    category: 'Overlays & Help',
  },
];

interface ShortcutHandlers {
  onNavigateMonitor: () => void;
  onNavigateBroker: () => void;
  onNavigateRoutes: () => void;
  onNavigateCode: () => void;
  onOpenPublish: () => void;
  onOpenAlerts: () => void;
  onToggleStream: () => void;
  onToggleCheatsheet: () => void;
  onEscape: () => boolean;
}

function isEditableTarget(target: EventTarget | null): boolean {
  if (!(target instanceof HTMLElement)) return false;
  const tag = target.tagName;
  if (tag === 'INPUT' || tag === 'TEXTAREA' || tag === 'SELECT') {
    return true;
  }
  if (target.isContentEditable) return true;
  if (target.getAttribute('role') === 'textbox') return true;
  return false;
}

function isNativeActivatable(target: EventTarget | null): boolean {
  if (!(target instanceof HTMLElement)) return false;
  const tag = target.tagName;
  return tag === 'BUTTON' || tag === 'A' || target.getAttribute('role') === 'button';
}

export function useGlobalShortcuts(handlers: ShortcutHandlers) {
  useEffect(() => {
    const handleKeyDown = (event: KeyboardEvent) => {
      if (event.key === 'Escape') {
        const closedSomething = handlers.onEscape();
        if (closedSomething) {
          event.preventDefault();
        }
        return;
      }

      if (event.repeat) return;
      if (event.ctrlKey || event.metaKey || event.altKey) return;
      if (isEditableTarget(event.target)) return;

      if (event.key === '?' || (event.key === '/' && event.shiftKey)) {
        event.preventDefault();
        handlers.onToggleCheatsheet();
        return;
      }

      if (event.key === ' ') {
        if (isNativeActivatable(event.target)) return;
        event.preventDefault();
        handlers.onToggleStream();
        return;
      }

      const key = event.key.toLowerCase();
      switch (key) {
        case 'm':
          event.preventDefault();
          handlers.onNavigateMonitor();
          break;
        case 'b':
          event.preventDefault();
          handlers.onNavigateBroker();
          break;
        case 'r':
          event.preventDefault();
          handlers.onNavigateRoutes();
          break;
        case 'c':
          event.preventDefault();
          handlers.onNavigateCode();
          break;
        case 'p':
          event.preventDefault();
          handlers.onOpenPublish();
          break;
        case 'a':
          event.preventDefault();
          handlers.onOpenAlerts();
          break;
        case 's':
          event.preventDefault();
          handlers.onToggleStream();
          break;
        default:
          break;
      }
    };

    window.addEventListener('keydown', handleKeyDown);
    return () => window.removeEventListener('keydown', handleKeyDown);
  }, [handlers]);
}
