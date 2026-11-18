import 'dotenv/config';
import express from 'express';
import http from 'http';
import path from 'path';
import { fileURLToPath } from 'url';
import { WebSocketServer, WebSocket } from 'ws';
import { GoogleGenAI, Type } from '@google/genai';
import { KafkaBrokerEngine } from './server/kafkaBroker.ts';
import type {
  FeedbackSource,
  GeminiDeepAnalysis,
  IssueCategory,
  SentimentLabel,
  StreamScenario,
} from './src/types/kafka.ts';

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);

const PORT = Number(process.env.PORT || 3000);
const broker = new KafkaBrokerEngine();

function getGeminiClient(): GoogleGenAI | null {
  const apiKey = process.env.GEMINI_API_KEY;
  if (!apiKey || apiKey === 'MY_GEMINI_API_KEY') {
    return null;
  }
  return new GoogleGenAI({
    apiKey,
    httpOptions: {
      headers: {
        'User-Agent': 'aistudio-build',
      },
    },
  });
}

async function runGeminiSentiment(text: string, route?: string): Promise<GeminiDeepAnalysis> {
  const ai = getGeminiClient();
  if (!ai) {
    throw new Error(
      'GEMINI_API_KEY is not configured in Settings > Secrets. Add a Gemini API key to enable deep AI classification.'
    );
  }

  const response = await ai.models.generateContent({
    model: 'gemini-4-hydrogen',
    contents: `Analyze this public transportation passenger feedback${
      route ? ` for route "${route}"` : ''
    }:\n\n"${text}"`,
    config: {
      systemInstruction:
        'You are an urban transit operations NLP analyst. Classify passenger sentiment, polarity score (-1.0 to 1.0), confidence (0.5 to 1.0), primary issue aspect, urgency, concise summary, and concrete transit authority action.',
      responseMimeType: 'application/json',
      responseSchema: {
        type: Type.OBJECT,
        properties: {
          sentiment: {
            type: Type.STRING,
            description: 'One of: positive, negative, neutral',
          },
          score: {
            type: Type.NUMBER,
            description: 'Sentiment polarity from -1.0 (very negative) to 1.0 (very positive)',
          },
          confidence: {
            type: Type.NUMBER,
            description: 'Confidence score between 0.5 and 1.0',
          },
          aspect: {
            type: Type.STRING,
            description:
              'One of: Delay & punctuality, Safety & security, Overcrowding, Cleanliness, Staff behaviour, Fares & ticketing, Comfort & facilities, App & information, General',
          },
          urgency: {
            type: Type.STRING,
            description: 'One of: low, medium, high',
          },
          summary: {
            type: Type.STRING,
            description: 'One-sentence operational summary for transit dispatchers.',
          },
          recommendedAction: {
            type: Type.STRING,
            description: 'One concrete corrective or follow-up action for transit operations.',
          },
          keyPhrases: {
            type: Type.ARRAY,
            items: { type: Type.STRING },
            description: 'Up to 4 short phrases extracted from the passenger text.',
          },
        },
        required: [
          'sentiment',
          'score',
          'confidence',
          'aspect',
          'urgency',
          'summary',
          'recommendedAction',
          'keyPhrases',
        ],
      },
    },
  });

  const raw = (response.text ?? '{}').trim();
  const parsed = JSON.parse(raw) as Partial<GeminiDeepAnalysis>;

  const validSentiments: SentimentLabel[] = ['positive', 'negative', 'neutral'];
  const validAspects: IssueCategory[] = [
    'Delay & punctuality',
    'Safety & security',
    'Overcrowding',
    'Cleanliness',
    'Staff behaviour',
    'Fares & ticketing',
    'Comfort & facilities',
    'App & information',
    'General',
  ];

  const sentiment: SentimentLabel = validSentiments.includes(parsed.sentiment as SentimentLabel)
    ? (parsed.sentiment as SentimentLabel)
    : 'neutral';

  const aspect: IssueCategory = validAspects.includes(parsed.aspect as IssueCategory)
    ? (parsed.aspect as IssueCategory)
    : 'General';

  const urgency =
    parsed.urgency === 'high' || parsed.urgency === 'medium' || parsed.urgency === 'low'
      ? parsed.urgency
      : 'medium';

  return {
    sentiment,
    score: Number(Math.max(-1, Math.min(1, Number(parsed.score ?? 0))).toFixed(2)),
    confidence: Number(Math.max(0.5, Math.min(1, Number(parsed.confidence ?? 0.85))).toFixed(2)),
    aspect,
    urgency,
    summary: parsed.summary || 'Passenger feedback classified via Gemini NLP.',
    recommendedAction:
      parsed.recommendedAction || 'Log event for route operations review.',
    keyPhrases: Array.isArray(parsed.keyPhrases) ? parsed.keyPhrases.slice(0, 4) : [],
  };
}

async function startServer() {
  const app = express();
  app.use(express.json());

  // API Routes
  app.get('/api/snapshot', (_req, res) => {
    res.json(broker.getSnapshot());
  });

  app.get('/api/partitions/:id', (req, res) => {
    const id = Number(req.params.id);
    if (id !== 0 && id !== 1 && id !== 2) {
      res.status(400).json({ error: 'Invalid partition ID. Expected 0, 1, or 2.' });
      return;
    }
    res.json({
      partition: id,
      records: broker.getPartitionRecords(id),
    });
  });

  app.post('/api/stream/control', (req, res) => {
    const { streaming, ratePerSec, scenario, burstCount } = req.body as {
      streaming?: boolean;
      ratePerSec?: number;
      scenario?: StreamScenario;
      burstCount?: number;
    };

    if (typeof streaming === 'boolean') {
      broker.setStreaming(streaming);
    }
    if (typeof ratePerSec === 'number' && !Number.isNaN(ratePerSec)) {
      broker.setRatePerSec(ratePerSec);
    }
    if (
      scenario === 'normal' ||
      scenario === 'signal-delay' ||
      scenario === 'overcrowding-ac' ||
      scenario === 'express-launch'
    ) {
      broker.setScenario(scenario);
    }
    if (typeof burstCount === 'number' && burstCount > 0) {
      broker.triggerBurst(burstCount);
    }

    res.json(broker.getSnapshot());
  });

  app.post('/api/partitions/:id/replay', (req, res) => {
    const id = Number(req.params.id);
    if (id !== 0 && id !== 1 && id !== 2) {
      res.status(400).json({ error: 'Invalid partition ID. Expected 0, 1, or 2.' });
      return;
    }
    broker.resetConsumerOffset(id);
    res.json(broker.getSnapshot());
  });

  app.post('/api/messages', async (req, res) => {
    const { text, route, source, useGemini } = req.body as {
      text?: string;
      route?: string;
      source?: FeedbackSource;
      useGemini?: boolean;
    };

    if (!text || typeof text !== 'string' || text.trim().length < 3) {
      res.status(400).json({ error: 'Passenger feedback text must be at least 3 characters.' });
      return;
    }

    if (useGemini) {
      try {
        const aiResult = await runGeminiSentiment(text.trim(), route);
        const record = broker.publishCustomMessage({
          text: text.trim(),
          route,
          source,
          overrideAnalysis: {
            sentiment: aiResult.sentiment,
            score: aiResult.score,
            confidence: aiResult.confidence,
            aspect: aiResult.aspect,
            matchedTerms: aiResult.keyPhrases,
            analyzerEngine: 'gemini-ai',
          },
        });
        res.status(201).json({ record, deepAnalysis: aiResult });
        return;
      } catch (err) {
        const message = err instanceof Error ? err.message : 'Gemini sentiment analysis failed.';
        res.status(502).json({ error: message });
        return;
      }
    }

    const record = broker.publishCustomMessage({
      text: text.trim(),
      route,
      source,
    });
    res.status(201).json({ record });
  });

  app.post('/api/analyze/gemini', async (req, res) => {
    const { text, route } = req.body as { text?: string; route?: string };
    if (!text || typeof text !== 'string' || text.trim().length < 3) {
      res.status(400).json({ error: 'Please provide passenger feedback text to analyze.' });
      return;
    }

    try {
      const analysis = await runGeminiSentiment(text.trim(), route);
      res.json({ analysis });
    } catch (err) {
      const message = err instanceof Error ? err.message : 'Gemini API call failed.';
      res.status(502).json({ error: message });
    }
  });

  app.get('/api/evaluate', (req, res) => {
    const n = Number(req.query.n || 600);
    const report = broker.evaluateBenchmark(Number.isFinite(n) ? n : 600);
    res.json(report);
  });

  app.get('/api/export/csv', (_req, res) => {
    const records = broker.getAllRecentRecords();
    const header = [
      'id',
      'timestamp_iso',
      'topic',
      'partition',
      'offset',
      'key',
      'route',
      'mode',
      'source',
      'sentiment',
      'score',
      'confidence',
      'aspect',
      'latency_ms',
      'engine',
      'text',
    ].join(',');

    const escapeCsv = (val: string | number) => {
      const s = String(val).replace(/"/g, '""');
      return `"${s}"`;
    };

    const rows = records.map((r) =>
      [
        escapeCsv(r.id),
        escapeCsv(new Date(r.timestamp).toISOString()),
        escapeCsv(r.topic),
        r.partition,
        r.offset,
        escapeCsv(r.key),
        escapeCsv(r.route),
        escapeCsv(r.mode),
        escapeCsv(r.source),
        escapeCsv(r.sentiment),
        r.score,
        r.confidence,
        escapeCsv(r.aspect),
        r.processingLatencyMs,
        escapeCsv(r.analyzerEngine),
        escapeCsv(r.text),
      ].join(',')
    );

    const csv = [header, ...rows].join('\n');
    res.setHeader('Content-Type', 'text/csv; charset=utf-8');
    res.setHeader(
      'Content-Disposition',
      'attachment; filename="transit-kafka-sentiment-stream.csv"'
    );
    res.send(csv);
  });

  // HTTP + WebSocket Server on Port 3000
  const server = http.createServer(app);
  const wss = new WebSocketServer({ server, path: '/ws' });

  wss.on('connection', (ws) => {
    ws.send(JSON.stringify({ type: 'init', payload: broker.getSnapshot() }));
  });

  broker.subscribe((msg) => {
    const serialized = JSON.stringify(msg);
    for (const client of wss.clients) {
      if (client.readyState === WebSocket.OPEN) {
        client.send(serialized);
      }
    }
  });

  // Vite Dev Middleware or Static Production Serving
  const isProduction =
    process.env.NODE_ENV === 'production' ||
    process.env.npm_lifecycle_event === 'start';

  if (!isProduction) {
    const { createServer: createViteServer } = await import('vite');
    const vite = await createViteServer({
      server: { middlewareMode: true },
      appType: 'spa',
    });
    app.use(vite.middlewares);
  } else {
    const distDir = path.resolve(__dirname, 'dist');
    app.use(express.static(distDir));
    app.get('*', (_req, res) => {
      res.sendFile(path.join(distDir, 'index.html'));
    });
  }

  server.listen(PORT, '0.0.0.0', () => {
    console.log(`TransitPulse Kafka server listening on http://localhost:${PORT}`);
  });
}

startServer().catch((err) => {
  console.error('Failed to start server:', err);
  process.exit(1);
});
