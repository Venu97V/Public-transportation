import { useMemo, useState } from 'react';
import { AlertTriangle } from 'lucide-react';
import type { BrokerSnapshot } from '../types/kafka';

interface RouteAnalyticsViewProps {
  snapshot: BrokerSnapshot;
}

export function RouteAnalyticsView({ snapshot }: RouteAnalyticsViewProps) {
  const [sortBy, setSortBy] = useState<'total' | 'avgScore' | 'negative'>('total');

  const sortedRoutes = useMemo(() => {
    const copy = [...snapshot.routes];
    copy.sort((a, b) => {
      if (sortBy === 'avgScore') return a.avgScore - b.avgScore;
      if (sortBy === 'negative') return b.negative - a.negative;
      return b.total - a.total;
    });
    return copy;
  }, [snapshot.routes, sortBy]);

  return (
    <div className="space-y-6">
      <section className="bg-white border border-slate-200 rounded-lg overflow-hidden">
        <div className="px-5 py-4 border-b border-slate-200 flex flex-wrap items-center justify-between gap-4">
          <div>
            <h2 className="text-base font-semibold text-slate-900">
              01. Transit Route Sentiment Matrix
            </h2>
            <p className="text-xs text-slate-500 mt-0.5">
              Aggregated passenger sentiment and primary operational bottlenecks per line and service
            </p>
          </div>

          <div className="inline-flex items-center p-0.5 bg-slate-100 rounded-md border border-slate-200">
            <button
              type="button"
              onClick={() => setSortBy('total')}
              className={`px-3 py-1 text-xs font-medium rounded-sm cursor-pointer whitespace-nowrap shrink-0 ${
                sortBy === 'total' ? 'bg-white text-slate-900 shadow-xs' : 'text-slate-600'
              }`}
            >
              Highest Volume
            </button>
            <button
              type="button"
              onClick={() => setSortBy('negative')}
              className={`px-3 py-1 text-xs font-medium rounded-sm cursor-pointer whitespace-nowrap shrink-0 ${
                sortBy === 'negative' ? 'bg-white text-slate-900 shadow-xs' : 'text-slate-600'
              }`}
            >
              Most Complaints
            </button>
            <button
              type="button"
              onClick={() => setSortBy('avgScore')}
              className={`px-3 py-1 text-xs font-medium rounded-sm cursor-pointer whitespace-nowrap shrink-0 ${
                sortBy === 'avgScore' ? 'bg-white text-slate-900 shadow-xs' : 'text-slate-600'
              }`}
            >
              Lowest Polarity
            </button>
          </div>
        </div>

        <div className="overflow-x-auto">
          <table className="w-full text-left border-collapse">
            <thead>
              <tr className="border-b border-slate-200 bg-slate-50/70 text-xs font-medium text-slate-500">
                <th className="py-2.5 px-5 whitespace-nowrap">Route / Service</th>
                <th className="py-2.5 px-3 whitespace-nowrap">Mode</th>
                <th className="py-2.5 px-3 whitespace-nowrap">Kafka Partition</th>
                <th className="py-2.5 px-3 whitespace-nowrap">Sentiment Distribution</th>
                <th className="py-2.5 px-3 whitespace-nowrap">Dominant Aspect</th>
                <th className="py-2.5 px-3 text-right whitespace-nowrap">Events</th>
                <th className="py-2.5 px-5 text-right whitespace-nowrap">Avg Polarity</th>
              </tr>
            </thead>
            <tbody className="divide-y divide-slate-100 text-xs">
              {sortedRoutes.map((r) => {
                const total = Math.max(1, r.total);
                const posPct = (r.positive / total) * 100;
                const neuPct = (r.neutral / total) * 100;
                const negPct = (r.negative / total) * 100;

                return (
                  <tr key={r.route} className="hover:bg-slate-50/80 transition-colors">
                    <td className="py-3 px-5 font-medium text-slate-900 whitespace-nowrap">
                      {r.route}
                    </td>
                    <td className="py-3 px-3 text-slate-600 capitalize whitespace-nowrap">
                      {r.mode}
                    </td>
                    <td className="py-3 px-3 font-mono text-slate-600 whitespace-nowrap">
                      Partition {r.partition}
                    </td>
                    <td className="py-3 px-3 min-w-[200px]">
                      <div className="flex items-center gap-2.5">
                        <div className="h-2 flex-1 bg-slate-100 rounded-xs overflow-hidden flex">
                          {posPct > 0 && (
                            <div style={{ width: `${posPct}%` }} className="bg-emerald-600 h-full" />
                          )}
                          {neuPct > 0 && (
                            <div style={{ width: `${neuPct}%` }} className="bg-amber-500 h-full" />
                          )}
                          {negPct > 0 && (
                            <div style={{ width: `${negPct}%` }} className="bg-red-600 h-full" />
                          )}
                        </div>
                        <span className="font-mono tabular-nums text-slate-500 w-24 text-right whitespace-nowrap">
                          {r.positive}+ / {r.neutral}~ / {r.negative}-
                        </span>
                      </div>
                    </td>
                    <td className="py-3 px-3 text-slate-700 whitespace-nowrap">{r.topIssue}</td>
                    <td className="py-3 px-3 text-right font-mono tabular-nums text-slate-700 whitespace-nowrap">
                      {r.total}
                    </td>
                    <td className="py-3 px-5 text-right font-mono tabular-nums font-medium whitespace-nowrap">
                      <span
                        className={
                          r.avgScore >= 0.05
                            ? 'text-emerald-700'
                            : r.avgScore <= -0.05
                            ? 'text-red-700'
                            : 'text-slate-700'
                        }
                      >
                        {r.avgScore >= 0 ? `+${r.avgScore.toFixed(2)}` : r.avgScore.toFixed(2)}
                      </span>
                    </td>
                  </tr>
                );
              })}
            </tbody>
          </table>
        </div>
      </section>

      {/* Per-Route Sliding Window Alerts (transport.alerts) */}
      <section className="bg-white border border-slate-200 rounded-lg overflow-hidden">
        <div className="px-5 py-4 border-b border-slate-200 flex flex-wrap items-center justify-between gap-4">
          <div>
            <h2 className="text-base font-semibold text-slate-900">
              02. Route Incident Alerts (<span className="font-mono">{snapshot.alertsTopic}</span>)
            </h2>
            <p className="text-xs text-slate-500 mt-0.5">
              Sliding-window detector flags any route when &ge; 70% of its last 30s messages (&ge; 6 msgs) are negative
            </p>
          </div>
          <span className="font-mono text-xs tabular-nums text-slate-500">
            {snapshot.routeAlerts.length} alerts emitted
          </span>
        </div>

        {snapshot.routeAlerts.length === 0 ? (
          <div className="p-6 text-xs text-slate-500">
            No route-level burst alerts emitted yet. Switch to the <strong className="text-slate-700">Signal Failure Delay</strong> or <strong className="text-slate-700">Overcrowding &amp; AC Outage</strong> scenario and inject a burst in Live Monitor to trigger a route alert.
          </div>
        ) : (
          <div className="divide-y divide-slate-100">
            {snapshot.routeAlerts.map((alert) => (
              <div
                key={alert.id}
                className="px-5 py-3 flex flex-wrap items-center justify-between gap-4 text-xs"
              >
                <div className="flex items-center gap-3">
                  <AlertTriangle className="h-4 w-4 text-red-600 shrink-0" />
                  <div>
                    <span className="font-semibold text-slate-900">{alert.route}</span>
                    <span className="text-slate-500 mx-2">·</span>
                    <span className="text-red-700 font-mono tabular-nums font-medium">
                      {Math.round(alert.negativeRatio * 100)}% negative
                    </span>
                    <span className="text-slate-500">
                      {' '}
                      across <span className="font-mono tabular-nums">{alert.messages}</span> msgs
                    </span>
                    <span className="text-slate-500 mx-2">·</span>
                    <span className="text-slate-700">Top issue: {alert.topTopic}</span>
                  </div>
                </div>
                <div className="font-mono tabular-nums text-slate-500">
                  {new Date(alert.timestamp).toTimeString().slice(0, 8)}
                </div>
              </div>
            ))}
          </div>
        )}
      </section>
    </div>
  );
}
