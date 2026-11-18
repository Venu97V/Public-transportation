import { useCallback, useEffect, useRef, useState } from 'react';
import type {
  BrokerSnapshot,
  FeedbackSource,
  GeminiDeepAnalysis,
  KafkaSentimentRecord,
  StreamScenario,
} from '../types/kafka';

interface WsPayload {
  type: 'init' | 'snapshot' | 'event';
  payload: BrokerSnapshot | { record: KafkaSentimentRecord; snapshot: BrokerSnapshot };
}

export function useKafkaStream() {
  const [snapshot, setSnapshot] = useState<BrokerSnapshot | null>(null);
  const [wsConnected, setWsConnected] = useState(false);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);
  const wsRef = useRef<WebSocket | null>(null);
  const reconnectTimerRef = useRef<ReturnType<typeof setTimeout> | null>(null);

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

    const connect = () => {
      if (unmounted) return;
      const protocol = window.location.protocol === 'https:' ? 'wss:' : 'ws:';
      const wsUrl = `${protocol}//${window.location.host}/ws`;
      const socket = new WebSocket(wsUrl);
      wsRef.current = socket;

      socket.onopen = () => {
        if (!unmounted) {
          setWsConnected(true);
          setError(null);
        }
      };

      socket.onmessage = (event) => {
        if (unmounted) return;
        try {
          const msg = JSON.parse(event.data as string) as WsPayload;
          if (msg.type === 'init' || msg.type === 'snapshot') {
            setSnapshot(msg.payload as BrokerSnapshot);
            setLoading(false);
          } else if (msg.type === 'event') {
            const payload = msg.payload as {
              record: KafkaSentimentRecord;
              snapshot: BrokerSnapshot;
            };
            setSnapshot(payload.snapshot);
            setLoading(false);
          }
        } catch {
          // Ignore malformed frame
        }
      };

      socket.onclose = () => {
        if (unmounted) return;
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
    loading,
    error,
    updateStreamControl,
    replayPartition,
    publishMessage,
    analyzeWithGemini,
    refetch: fetchSnapshot,
  };
}
