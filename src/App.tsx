import { useCallback, useMemo, useState } from 'react';
import { motion } from 'motion/react';
import {
  Activity,
  BookOpen,
  Download,
  GitBranch,
  HeartPulse,
  Keyboard,
  Menu,
  Plus,
  Route as RouteIcon,
  Server,
  X,
} from 'lucide-react';
import { useKafkaStream } from './hooks/useKafkaStream';
import { useGlobalShortcuts } from './hooks/useGlobalShortcuts';
import { LiveMonitorView } from './components/LiveMonitorView';
import { KafkaBrokerView } from './components/KafkaBrokerView';
import { RouteAnalyticsView } from './components/RouteAnalyticsView';
import { ProjectBlueprintView } from './components/ProjectBlueprintView';
import { PublishModal } from './components/PublishModal';
import { RecordInspectorDrawer } from './components/RecordInspectorDrawer';
import { ShortcutCheatsheetModal } from './components/ShortcutCheatsheetModal';
import type { KafkaSentimentRecord } from './types/kafka';

type NavTab = 'monitor' | 'broker' | 'routes' | 'blueprint';

const NAV_ITEMS: Array<{
  id: NavTab;
  label: string;
  breadcrumb: string;
  shortcut: string;
  icon: typeof Activity;
}> = [
  { id: 'monitor', label: 'Live Monitor', breadcrumb: 'Live Monitor', shortcut: 'M', icon: Activity },
  { id: 'broker', label: 'Kafka Broker', breadcrumb: 'Kafka Broker', shortcut: 'B', icon: Server },
  { id: 'routes', label: 'Route Analytics', breadcrumb: 'Route Analytics', shortcut: 'R', icon: RouteIcon },
  { id: 'blueprint', label: 'Project & Code', breadcrumb: 'Project & Code', shortcut: 'C', icon: BookOpen },
];

export default function App() {
  const {
    snapshot,
    wsConnected,
    latencyMs,
    lastHeartbeatAt,
    heartbeatSeq,
    loading,
    error,
    updateStreamControl,
    replayPartition,
    publishMessage,
    analyzeWithGemini,
    refetch,
  } = useKafkaStream();

  const [activeTab, setActiveTab] = useState<NavTab>('monitor');
  const [mobileNavOpen, setMobileNavOpen] = useState(false);
  const [publishModalOpen, setPublishModalOpen] = useState(false);
  const [alertModalOpen, setAlertModalOpen] = useState(false);
  const [cheatsheetOpen, setCheatsheetOpen] = useState(false);
  const [inspectedRecord, setInspectedRecord] = useState<KafkaSentimentRecord | null>(null);

  const handleEscape = useCallback(() => {
    if (cheatsheetOpen) {
      setCheatsheetOpen(false);
      return true;
    }
    if (publishModalOpen) {
      setPublishModalOpen(false);
      return true;
    }
    if (alertModalOpen) {
      setAlertModalOpen(false);
      return true;
    }
    if (inspectedRecord) {
      setInspectedRecord(null);
      return true;
    }
    if (mobileNavOpen) {
      setMobileNavOpen(false);
      return true;
    }
    return false;
  }, [cheatsheetOpen, publishModalOpen, alertModalOpen, inspectedRecord, mobileNavOpen]);

  const shortcutHandlers = useMemo(
    () => ({
      onNavigateMonitor: () => {
        setCheatsheetOpen(false);
        setActiveTab('monitor');
        setMobileNavOpen(false);
      },
      onNavigateBroker: () => {
        setCheatsheetOpen(false);
        setActiveTab('broker');
        setMobileNavOpen(false);
      },
      onNavigateRoutes: () => {
        setCheatsheetOpen(false);
        setActiveTab('routes');
        setMobileNavOpen(false);
      },
      onNavigateCode: () => {
        setCheatsheetOpen(false);
        setActiveTab('blueprint');
        setMobileNavOpen(false);
      },
      onOpenPublish: () => {
        setCheatsheetOpen(false);
        setAlertModalOpen(false);
        setPublishModalOpen(true);
      },
      onOpenAlerts: () => {
        setCheatsheetOpen(false);
        setPublishModalOpen(false);
        setActiveTab('monitor');
        setAlertModalOpen(true);
      },
      onToggleStream: () => {
        if (!snapshot) return;
        void updateStreamControl({ streaming: !snapshot.streaming });
      },
      onToggleCheatsheet: () => {
        setCheatsheetOpen((prev) => !prev);
      },
      onEscape: handleEscape,
    }),
    [snapshot, updateStreamControl, handleEscape]
  );

  useGlobalShortcuts(shortcutHandlers);

  const currentNav = NAV_ITEMS.find((item) => item.id === activeTab) ?? NAV_ITEMS[0];

  return (
    <div className="min-h-screen bg-slate-50 text-slate-900 flex">
      {/* Mobile Sidebar Backdrop */}
      {mobileNavOpen && (
        <div
          className="fixed inset-0 z-40 bg-slate-900/30 lg:hidden"
          onClick={() => setMobileNavOpen(false)}
        />
      )}

      {/* Left Sidebar (256px) */}
      <aside
        className={`fixed inset-y-0 left-0 z-50 w-64 bg-white border-r border-slate-200 flex flex-col justify-between transition-transform duration-200 lg:static lg:translate-x-0 ${
          mobileNavOpen ? 'translate-x-0' : '-translate-x-full'
        }`}
      >
        <div>
          <div className="h-14 px-5 border-b border-slate-200 flex items-center justify-between">
            <a
              href="#monitor"
              onClick={(e) => {
                e.preventDefault();
                setActiveTab('monitor');
              }}
              className="flex items-center gap-2.5 text-base font-bold tracking-tight text-slate-900 whitespace-nowrap shrink-0"
            >
              <GitBranch className="h-4 w-4 text-sky-600 shrink-0" />
              <span>TransitPulse</span>
            </a>
            <button
              type="button"
              onClick={() => setMobileNavOpen(false)}
              className="p-1.5 rounded-md text-slate-500 hover:bg-slate-100 lg:hidden cursor-pointer"
              aria-label="Close navigation"
            >
              <X className="h-4 w-4" />
            </button>
          </div>

          <nav className="p-3 space-y-1" aria-label="Primary Workspace Navigation">
            {NAV_ITEMS.map((item) => {
              const Icon = item.icon;
              const isActive = activeTab === item.id;
              return (
                <button
                  key={item.id}
                  type="button"
                  aria-keyshortcuts={item.shortcut.toLowerCase()}
                  onClick={() => {
                    setActiveTab(item.id);
                    setMobileNavOpen(false);
                  }}
                  className={`w-full flex items-center justify-between gap-2 px-3 py-2.5 rounded-md text-sm font-medium transition-colors whitespace-nowrap shrink-0 cursor-pointer ${
                    isActive
                      ? 'bg-sky-50 text-sky-700'
                      : 'text-slate-600 hover:bg-slate-50 hover:text-slate-900'
                  }`}
                >
                  <span className="flex items-center gap-2.5 min-w-0">
                    <Icon className={`h-4 w-4 shrink-0 ${isActive ? 'text-sky-600' : 'text-slate-400'}`} />
                    <span>{item.label}</span>
                  </span>
                  <kbd
                    className={`inline-flex items-center justify-center h-5 min-w-[20px] px-1 rounded-sm border font-mono text-[10px] leading-none ${
                      isActive
                        ? 'border-sky-200 bg-white text-sky-700'
                        : 'border-slate-200 bg-slate-50 text-slate-400'
                    }`}
                  >
                    {item.shortcut}
                  </kbd>
                </button>
              );
            })}
          </nav>
        </div>

        <div className="p-3 border-t border-slate-200 space-y-2.5">
          <button
            type="button"
            onClick={() => setCheatsheetOpen(true)}
            aria-keyshortcuts="?"
            className="w-full flex items-center justify-between gap-2 px-3 py-2 rounded-md border border-slate-200 bg-slate-50/70 text-xs font-medium text-slate-700 hover:bg-slate-100 transition-colors whitespace-nowrap shrink-0 cursor-pointer"
          >
            <span className="flex items-center gap-2">
              <Keyboard className="h-3.5 w-3.5 text-slate-500 shrink-0" />
              <span>Keyboard Shortcuts</span>
            </span>
            <kbd className="inline-flex items-center justify-center h-5 min-w-[20px] px-1 rounded-sm border border-slate-200 bg-white font-mono text-[10px] leading-none text-slate-600">
              ?
            </kbd>
          </button>

          <div className="px-1 text-xs text-slate-500 space-y-0.5">
            <div className="font-medium text-slate-700">BDT College Project</div>
            <div>Apache Kafka Sentiment Monitor</div>
          </div>
        </div>
      </aside>

      {/* Main Content Area */}
      <div className="flex-1 min-w-0 flex flex-col">
        {/* Top Bar Contract: Breadcrumb on left, 1-2 single-line actions on right */}
        <header className="h-14 bg-white border-b border-slate-200 px-4 sm:px-6 flex items-center justify-between gap-8 sticky top-0 z-30">
          <div className="flex items-center gap-3 min-w-0">
            <button
              type="button"
              onClick={() => setMobileNavOpen(true)}
              className="p-1.5 -ml-1.5 rounded-md text-slate-600 hover:bg-slate-100 lg:hidden cursor-pointer"
              aria-label="Open navigation"
            >
              <Menu className="h-4 w-4" />
            </button>
            <div className="flex items-center gap-2 text-xs sm:text-sm truncate">
              <span className="text-slate-500 hidden sm:inline">BDT Project</span>
              <span className="text-slate-300 hidden sm:inline">/</span>
              <span className="text-slate-500">Transit Sentiment</span>
              <span className="text-slate-300">/</span>
              <span className="font-medium text-slate-900">{currentNav.breadcrumb}</span>
            </div>
          </div>

          <div className="flex items-center gap-3 shrink-0">
            {/* Real-Time WebSocket Heartbeat & RTT Latency Indicator */}
            <div
              className="hidden sm:flex items-center gap-2 pr-3 border-r border-slate-200 text-xs whitespace-nowrap shrink-0"
              role="status"
              aria-live="polite"
              title={
                wsConnected
                  ? `WebSocket Connected${
                      latencyMs !== null ? ` · RTT ${latencyMs} ms` : ''
                    }${
                      lastHeartbeatAt
                        ? ` · Last heartbeat ${new Date(lastHeartbeatAt).toTimeString().slice(0, 8)}`
                        : ''
                    }`
                  : 'WebSocket Reconnecting...'
              }
            >
              <motion.span
                key={wsConnected ? heartbeatSeq : 'offline'}
                initial={wsConnected ? { scale: 1.25, opacity: 0.7 } : false}
                animate={{ scale: 1, opacity: 1 }}
                transition={{ duration: 0.25, ease: [0.16, 1, 0.3, 1] }}
                className="inline-flex items-center"
              >
                <HeartPulse
                  className={`h-3.5 w-3.5 shrink-0 ${
                    !wsConnected
                      ? 'text-red-600'
                      : latencyMs !== null && latencyMs > 250
                      ? 'text-amber-600'
                      : 'text-emerald-600'
                  }`}
                />
              </motion.span>
              <span
                className={`font-medium ${
                  !wsConnected
                    ? 'text-red-700'
                    : latencyMs !== null && latencyMs > 250
                    ? 'text-amber-700'
                    : 'text-slate-700'
                }`}
              >
                {wsConnected ? 'WS Live' : 'Reconnecting'}
              </span>
              <span className="text-slate-300" aria-hidden="true">
                ·
              </span>
              <span
                className={`font-mono tabular-nums ${
                  !wsConnected
                    ? 'text-red-600'
                    : latencyMs !== null && latencyMs > 250
                    ? 'text-amber-700'
                    : 'text-slate-600'
                }`}
              >
                {wsConnected && latencyMs !== null ? `${latencyMs} ms` : '-- ms'}
              </span>
            </div>

            <a
              href="/api/export/csv"
              download="transit-kafka-sentiment-stream.csv"
              className="inline-flex items-center gap-1.5 h-9 px-3.5 rounded-md border border-slate-200 bg-white text-xs font-medium text-slate-700 hover:bg-slate-50 transition-colors whitespace-nowrap shrink-0"
            >
              <Download className="h-3.5 w-3.5 text-slate-500" />
              Export CSV
            </a>
            <button
              type="button"
              onClick={() => setPublishModalOpen(true)}
              aria-keyshortcuts="p"
              className="inline-flex items-center gap-1.5 h-9 px-4 rounded-md bg-sky-600 text-white text-xs font-medium hover:bg-sky-700 transition-colors whitespace-nowrap shrink-0 cursor-pointer"
            >
              <Plus className="h-3.5 w-3.5" />
              <span>Publish Feedback</span>
              <kbd className="ml-0.5 inline-flex items-center justify-center h-4 px-1 rounded-sm border border-white/25 bg-white/15 font-mono text-[10px] leading-none text-white">
                P
              </kbd>
            </button>
          </div>
        </header>

        {/* Workspace Canvas */}
        <main className="flex-1 p-4 sm:p-6 lg:p-8 max-w-[1360px] w-full mx-auto">
          {loading && !snapshot ? (
            <div className="space-y-6" aria-busy="true">
              <div className="h-20 bg-white border border-slate-200 rounded-lg animate-pulse" />
              <div className="grid grid-cols-1 sm:grid-cols-2 xl:grid-cols-4 gap-4">
                {[0, 1, 2, 3].map((i) => (
                  <div
                    key={i}
                    className="h-28 bg-white border border-slate-200 rounded-lg animate-pulse"
                  />
                ))}
              </div>
              <div className="h-72 bg-white border border-slate-200 rounded-lg animate-pulse" />
            </div>
          ) : error && !snapshot ? (
            <div className="bg-white border border-red-200 rounded-lg p-6 max-w-lg mx-auto text-center space-y-3">
              <div className="text-sm font-semibold text-red-700">
                Unable to Connect to Kafka Stream Server
              </div>
              <p className="text-xs text-slate-600">{error}</p>
              <button
                type="button"
                onClick={refetch}
                className="h-9 px-4 rounded-md bg-slate-900 text-white text-xs font-medium hover:bg-slate-800 cursor-pointer"
              >
                Retry Connection
              </button>
            </div>
          ) : snapshot ? (
            <>
              {activeTab === 'monitor' && (
                <LiveMonitorView
                  snapshot={snapshot}
                  wsConnected={wsConnected}
                  onToggleStream={(streaming) => updateStreamControl({ streaming })}
                  onChangeScenario={(scenario) => updateStreamControl({ scenario })}
                  onTriggerBurst={() => updateStreamControl({ burstCount: 6 })}
                  onInspectRecord={(record) => setInspectedRecord(record)}
                  alertModalOpen={alertModalOpen}
                  onAlertModalOpenChange={setAlertModalOpen}
                />
              )}

              {activeTab === 'broker' && (
                <KafkaBrokerView
                  snapshot={snapshot}
                  onUpdateStream={updateStreamControl}
                  onReplayPartition={replayPartition}
                  onPublishMessage={publishMessage}
                  onInspectRecord={(record) => setInspectedRecord(record)}
                />
              )}

              {activeTab === 'routes' && <RouteAnalyticsView snapshot={snapshot} />}

              {activeTab === 'blueprint' && <ProjectBlueprintView />}
            </>
          ) : null}
        </main>
      </div>

      {/* Modals & Drawers */}
      <PublishModal
        open={publishModalOpen}
        onClose={() => setPublishModalOpen(false)}
        onPublish={publishMessage}
      />

      <RecordInspectorDrawer
        record={inspectedRecord}
        onClose={() => setInspectedRecord(null)}
        onAnalyzeWithGemini={analyzeWithGemini}
      />

      <ShortcutCheatsheetModal
        open={cheatsheetOpen}
        onClose={() => setCheatsheetOpen(false)}
      />
    </div>
  );
}
