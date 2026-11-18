import { useCallback, useEffect, useRef, useState } from 'react';
import type {
  BrokerSnapshot,
  FeedbackSource,
  GeminiDeepAnalysis,
  KafkaSentimentRecord,
  StreamScenario,
} from '../types/kafka';

type WsPayload =
  | { type: 'init' | 'snapshot'; payload: BrokerSnapshot }
  | { type: 'event'; payload: { record: KafkaSentimentRecord; snapshot: BrokerSnapshot } }
  | { type: 'pong'; clientTs: number; serverTs: number };

const HEARTBEAT_INTERVAL_MS = 4000;
const HEARTBEAT_TIMEOUT_MS = 10000;

export function useKafkaStream() {
  const [snapshot, setSnapshot] = useState<BrokerSnapshot | null>(null);
  const [wsConnected, setWsConnected] = useState(false);
  const [latencyMs, setLatencyMs] = useState<number | null>(null);
  const [lastHeartbeatAt, setLastHeartbeatAt] = useState<number | null>(null);
  const [heartbeatSeq, setHeartbeatSeq] = useState(0);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);

  const wsRef = useRef<WebSocket | null>(null);
  const reconnectTimerRef = useRef<ReturnType<typeof setTimeout> | null>(null);
  const heartbeatIntervalRef = useRef<ReturnType<typeof setInterval> | null>(null);
  const lastPingSentRef = useRef<number | null>(null);
  const lastPongAtRef = useRef<number>(0);

  const fetchSnapshot = useCallback(async () => {
    try {
      const res = await fetch('/api/snapshot');
      if (!res.ok) throw new Error('Failed to load broker snapshot');
      const data = (await res.json()) as BrokerSnapshot;
      setSnapshot(data);
      setError(null);
    } catch (err) {
      setError(err instanceof Error ? err.message : 'Unable to reach Kafka broker API');
    } finally {
      setLoading(false);
    }
  }, []);

  useEffect(() => {
    let unmounted = false;

    const clearHeartbeat = () => {
      if (heartbeatIntervalRef.current) {
        clearInterval(heartbeatIntervalRef.current);
        heartbeatIntervalRef.current = null;
      }
    };

    const sendPing = (socket: WebSocket) => {
      if (socket.readyState !== WebSocket.OPEN) return;
      const now = Date.now();
      if (
        lastPingSentRef.current !== null &&
        lastPongAtRef.current < lastPingSentRef.current &&
        now - lastPingSentRef.current > HEARTBEAT_TIMEOUT_MS
      ) {
        setWsConnected(false);
        socket.close();
        return;
      }
      lastPingSentRef.current = now;
      socket.send(JSON.stringify({ type: 'ping', clientTs: now }));
    };

    const connect = () => {
      if (unmounted) return;
      const protocol = window.location.protocol === 'https:' ? 'wss:' : 'ws:';
      const wsUrl = `${protocol}//${window.location.host}/ws`;
      const socket = new WebSocket(wsUrl);
      wsRef.current = socket;

      socket.onopen = () => {
        if (unmounted) return;
        setWsConnected(true);
        setError(null);
        lastPongAtRef.current = Date.now();
        lastPingSentRef.current = null;
        sendPing(socket);
        clearHeartbeat();
        heartbeatIntervalRef.current = setInterval(() => {
          sendPing(socket);
        }, HEARTBEAT_INTERVAL_MS);
      };

      socket.onmessage = (event) => {
        if (unmounted) return;
        try {
          const msg = JSON.parse(event.data as string) as WsPayload;
          if (msg.type === 'pong') {
            const now = Date.now();
            lastPongAtRef.current = now;
            const rtt = Math.max(1, Math.round(now - msg.clientTs));
            setLatencyMs(rtt);
            setLastHeartbeatAt(now);
            setHeartbeatSeq((prev) => prev + 1);
            setWsConnected(true);
          } else if (msg.type === 'init' || msg.type === 'snapshot') {
            setSnapshot(msg.payload);
            setLoading(false);
          } else if (msg.type === 'event') {
            setSnapshot(msg.payload.snapshot);
            setLoading(false);
          }
        } catch {
          // Ignore malformed frame
        }
      };

      socket.onclose = () => {
        if (unmounted) return;
        clearHeartbeat();
        setWsConnected(false);
        reconnectTimerRef.current = setTimeout(connect, 2000);
      };

      socket.onerror = () => {
        if (!unmounted) {
          setWsConnected(false);
        }
      };
    };

    fetchSnapshot();
    connect();

    return () => {
      unmounted = true;
      clearHeartbeat();
      if (reconnectTimerRef.current) clearTimeout(reconnectTimerRef.current);
      if (wsRef.current) wsRef.current.close();
    };
  }, [fetchSnapshot]);

  const updateStreamControl = useCallback(
    async (params: {
      streaming?: boolean;
      ratePerSec?: number;
      scenario?: StreamScenario;
      burstCount?: number;
    }) => {
      const res = await fetch('/api/stream/control', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(params),
      });
      if (!res.ok) {
        const body = (await res.json().catch(() => ({}))) as { error?: string };
        throw new Error(body.error || 'Failed to update stream settings');
      }
      const next = (await res.json()) as BrokerSnapshot;
      setSnapshot(next);
      return next;
    },
    []
  );

  const replayPartition = useCallback(async (partitionId: 0 | 1 | 2) => {
    const res = await fetch(`/api/partitions/${partitionId}/replay`, {
      method: 'POST',
    });
    if (!res.ok) {
      throw new Error('Failed to replay partition offsets');
    }
    const next = (await res.json()) as BrokerSnapshot;
    setSnapshot(next);
    return next;
  }, []);

  const publishMessage = useCallback(
    async (input: {
      text: string;
      route: string;
      source: FeedbackSource;
      useGemini?: boolean;
    }): Promise<{ record: KafkaSentimentRecord; deepAnalysis?: GeminiDeepAnalysis }> => {
      const res = await fetch('/api/messages', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(input),
      });
      const body = (await res.json()) as {
        record?: KafkaSentimentRecord;
        deepAnalysis?: GeminiDeepAnalysis;
        error?: string;
      };
      if (!res.ok || !body.record) {
        throw new Error(body.error || 'Failed to publish feedback to Kafka topic');
      }
      return { record: body.record, deepAnalysis: body.deepAnalysis };
    },
    []
  );

  const analyzeWithGemini = useCallback(
    async (text: string, route?: string): Promise<GeminiDeepAnalysis> => {
      const res = await fetch('/api/analyze/gemini', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ text, route }),
      });
      const body = (await res.json()) as {
        analysis?: GeminiDeepAnalysis;
        error?: string;
      };
      if (!res.ok || !body.analysis) {
        throw new Error(body.error || 'Gemini analysis failed');
      }
      return body.analysis;
    },
    []
  );

  return {
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
    refetch: fetchSnapshot,
  };
}
