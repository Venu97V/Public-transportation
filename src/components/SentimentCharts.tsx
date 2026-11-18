import { useState } from 'react';
import type { AspectAggregate, TimeBucket } from '../types/kafka';

interface TimelineChartProps {
  timeline: TimeBucket[];
}

export function SentimentTimelineChart({ timeline }: TimelineChartProps) {
  const [hoveredIndex, setHoveredIndex] = useState<number | null>(null);

  if (timeline.length === 0) {
    return (
      <div className="flex h-64 items-center justify-center border border-dashed border-slate-200 rounded-lg text-sm text-slate-500">
        Waiting for Kafka consumer events to populate time windows...
      </div>
    );
  }

  const maxTotal = Math.max(4, ...timeline.map((b) => b.total));
  const chartW = 720;
  const chartH = 220;
  const padLeft = 34;
  const padRight = 14;
  const padTop = 16;
  const padBottom = 28;
  const plotW = chartW - padLeft - padRight;
  const plotH = chartH - padTop - padBottom;

  const slotW = plotW / Math.max(1, timeline.length);
  const barW = Math.min(22, Math.max(8, slotW * 0.62));

  const yTicks = [0, Math.ceil(maxTotal / 2), maxTotal];
  const hovered = hoveredIndex !== null ? timeline[hoveredIndex] : null;

  return (
    <div className="relative">
      <div className="flex flex-wrap items-center justify-between gap-4 mb-4">
        <div className="flex items-center gap-5 text-xs text-slate-600">
          <span className="inline-flex items-center gap-1.5">
            <span className="h-2.5 w-2.5 rounded-xs bg-emerald-600 inline-block" />
            Positive
          </span>
          <span className="inline-flex items-center gap-1.5">
            <span className="h-2.5 w-2.5 rounded-xs bg-amber-500 inline-block" />
            Neutral
          </span>
          <span className="inline-flex items-center gap-1.5">
            <span className="h-2.5 w-2.5 rounded-xs bg-red-600 inline-block" />
            Negative
          </span>
        </div>

        <div className="text-xs font-mono tabular-nums text-slate-500 min-h-[18px]">
          {hovered ? (
            <span>
              Window {hovered.label} · +{hovered.positive} / ~{hovered.neutral} / -{hovered.negative}{' '}
              (avg {hovered.avgScore >= 0 ? `+${hovered.avgScore.toFixed(2)}` : hovered.avgScore.toFixed(2)})
            </span>
          ) : (
            <span>Hover any time window to inspect sentiment volume</span>
          )}
        </div>
      </div>

      <svg
        viewBox={`0 0 ${chartW} ${chartH}`}
        className="w-full h-56 overflow-visible"
        role="img"
        aria-label="Real-time passenger sentiment volume across rolling Kafka time windows"
      >
        {/* Horizontal gridlines */}
        {yTicks.map((tick) => {
          const y = padTop + plotH - (tick / maxTotal) * plotH;
          return (
            <g key={tick}>
              <line
                x1={padLeft}
                x2={chartW - padRight}
                y1={y}
                y2={y}
                stroke="#e2e8f0"
                strokeWidth={1}
              />
              <text
                x={padLeft - 8}
                y={y + 4}
                textAnchor="end"
                className="fill-slate-400 text-[10px] font-mono tabular-nums"
              >
                {tick}
              </text>
            </g>
          );
        })}

        {/* Stacked sentiment bars */}
        {timeline.map((bucket, idx) => {
          const x = padLeft + idx * slotW + (slotW - barW) / 2;
          const posH = (bucket.positive / maxTotal) * plotH;
          const neuH = (bucket.neutral / maxTotal) * plotH;
          const negH = (bucket.negative / maxTotal) * plotH;

          const posY = padTop + plotH - posH;
          const neuY = posY - neuH;
          const negY = neuY - negH;
          const isHovered = hoveredIndex === idx;

          return (
            <g
              key={bucket.bucketId}
              onMouseEnter={() => setHoveredIndex(idx)}
              onMouseLeave={() => setHoveredIndex(null)}
              className="cursor-pointer"
            >
              <rect
                x={padLeft + idx * slotW}
                y={padTop}
                width={slotW}
                height={plotH}
                fill={isHovered ? '#f1f5f9' : 'transparent'}
                rx={2}
              />
              {bucket.positive > 0 && (
                <rect
                  x={x}
                  y={posY}
                  width={barW}
                  height={Math.max(2, posH)}
                  fill="#16a34a"
                  rx={1}
                />
              )}
              {bucket.neutral > 0 && (
                <rect
                  x={x}
                  y={neuY}
                  width={barW}
                  height={Math.max(2, neuH)}
                  fill="#f59e0b"
                  rx={1}
                />
              )}
              {bucket.negative > 0 && (
                <rect
                  x={x}
                  y={negY}
                  width={barW}
                  height={Math.max(2, negH)}
                  fill="#dc2626"
                  rx={1}
                />
              )}
              {(idx === 0 || idx === timeline.length - 1 || idx % 4 === 0) && (
                <text
                  x={x + barW / 2}
                  y={chartH - 6}
                  textAnchor="middle"
                  className="fill-slate-400 text-[10px] font-mono tabular-nums"
                >
                  {bucket.label.slice(0, 5)}
                </text>
              )}
            </g>
          );
        })}
      </svg>
    </div>
  );
}

interface AspectBreakdownProps {
  aspects: AspectAggregate[];
}

export function AspectBreakdownPanel({ aspects }: AspectBreakdownProps) {
  const maxVal = Math.max(1, ...aspects.map((a) => a.total));

  return (
    <div className="space-y-3.5">
      {aspects.map((item) => {
        const negPct = item.total > 0 ? (item.negative / maxVal) * 100 : 0;
        const neuPct = item.total > 0 ? (item.neutral / maxVal) * 100 : 0;
        const posPct = item.total > 0 ? (item.positive / maxVal) * 100 : 0;

        return (
          <div key={item.aspect} className="space-y-1.5">
            <div className="flex items-baseline justify-between gap-2 text-xs">
              <span className="font-medium text-slate-800 truncate">{item.aspect}</span>
              <span className="font-mono tabular-nums text-slate-500 whitespace-nowrap shrink-0">
                {item.total} events · {item.sharePct}%
              </span>
            </div>
            <div className="h-2 w-full bg-slate-100 rounded-xs overflow-hidden flex">
              {negPct > 0 && (
                <div
                  style={{ width: `${negPct}%` }}
                  className="bg-red-600 h-full transition-all duration-200"
                  title={`Negative: ${item.negative}`}
                />
              )}
              {neuPct > 0 && (
                <div
                  style={{ width: `${neuPct}%` }}
                  className="bg-amber-500 h-full transition-all duration-200"
                  title={`Neutral: ${item.neutral}`}
                />
              )}
              {posPct > 0 && (
                <div
                  style={{ width: `${posPct}%` }}
                  className="bg-emerald-600 h-full transition-all duration-200"
                  title={`Positive: ${item.positive}`}
                />
              )}
            </div>
          </div>
        );
      })}
    </div>
  );
}

interface SentimentRatioChartProps {
  positive: number;
  neutral: number;
  negative: number;
}

export function SentimentRatioChart({
  positive,
  neutral,
  negative,
}: SentimentRatioChartProps) {
  const [hoveredKey, setHoveredKey] = useState<'positive' | 'neutral' | 'negative' | null>(null);

  const total = positive + neutral + negative;
  const size = 140;
  const center = size / 2;
  const radius = 52;
  const strokeWidth = 16;
  const circumference = 2 * Math.PI * radius;

  const segments = [
    {
      key: 'positive' as const,
      label: 'Positive',
      count: positive,
      color: '#16a34a',
      swatchClass: 'bg-emerald-600',
      textClass: 'text-emerald-700',
    },
    {
      key: 'neutral' as const,
      label: 'Neutral',
      count: neutral,
      color: '#f59e0b',
      swatchClass: 'bg-amber-500',
      textClass: 'text-amber-700',
    },
    {
      key: 'negative' as const,
      label: 'Negative',
      count: negative,
      color: '#dc2626',
      swatchClass: 'bg-red-600',
      textClass: 'text-red-700',
    },
  ];

  const activeSegment = hoveredKey ? segments.find((s) => s.key === hoveredKey) : null;
  const activePct =
    activeSegment && total > 0 ? ((activeSegment.count / total) * 100).toFixed(1) : null;

  let cumulativeOffset = 0;

  return (
    <div className="flex flex-col items-center justify-between gap-4">
      <div className="relative flex items-center justify-center">
        <svg
          width={size}
          height={size}
          viewBox={`0 0 ${size} ${size}`}
          role="img"
          aria-label="Real-time ratio of positive, neutral, and negative passenger feedback"
          className="-rotate-90"
        >
          <circle
            cx={center}
            cy={center}
            r={radius}
            fill="none"
            stroke="#f1f5f9"
            strokeWidth={strokeWidth}
          />
          {total > 0 &&
            segments.map((seg) => {
              if (seg.count === 0) return null;
              const segLen = (seg.count / total) * circumference;
              const dashArray = `${segLen} ${circumference - segLen}`;
              const dashOffset = -cumulativeOffset;
              cumulativeOffset += segLen;
              const isDimmed = hoveredKey !== null && hoveredKey !== seg.key;

              return (
                <circle
                  key={seg.key}
                  cx={center}
                  cy={center}
                  r={radius}
                  fill="none"
                  stroke={seg.color}
                  strokeWidth={hoveredKey === seg.key ? strokeWidth + 2 : strokeWidth}
                  strokeDasharray={dashArray}
                  strokeDashoffset={dashOffset}
                  onMouseEnter={() => setHoveredKey(seg.key)}
                  onMouseLeave={() => setHoveredKey(null)}
                  className="cursor-pointer transition-all duration-200"
                  style={{ opacity: isDimmed ? 0.35 : 1 }}
                />
              );
            })}
        </svg>

        <div className="absolute inset-0 flex flex-col items-center justify-center pointer-events-none text-center">
          {activeSegment && activePct !== null ? (
            <>
              <span
                className={`text-lg font-semibold font-mono tabular-nums ${activeSegment.textClass}`}
              >
                {activePct}%
              </span>
              <span className="text-[11px] font-medium text-slate-600">
                {activeSegment.label}
              </span>
            </>
          ) : (
            <>
              <span className="text-lg font-semibold font-mono tabular-nums text-slate-900">
                {total.toLocaleString()}
              </span>
              <span className="text-[11px] text-slate-500">Total Events</span>
            </>
          )}
        </div>
      </div>

      <div className="w-full divide-y divide-slate-100 border-t border-slate-100 text-xs">
        {segments.map((seg) => {
          const pct = total > 0 ? ((seg.count / total) * 100).toFixed(1) : '0.0';
          return (
            <div
              key={seg.key}
              onMouseEnter={() => setHoveredKey(seg.key)}
              onMouseLeave={() => setHoveredKey(null)}
              className="py-2 flex items-center justify-between gap-2 cursor-pointer hover:bg-slate-50/70 px-1 rounded-xs transition-colors"
            >
              <span className="inline-flex items-center gap-2 text-slate-700 font-medium">
                <span className={`h-2.5 w-2.5 rounded-xs inline-block ${seg.swatchClass}`} />
                {seg.label}
              </span>
              <span className="font-mono tabular-nums text-slate-600">
                {seg.count} · <span className="font-medium text-slate-900">{pct}%</span>
              </span>
            </div>
          );
        })}
      </div>
    </div>
  );
}
