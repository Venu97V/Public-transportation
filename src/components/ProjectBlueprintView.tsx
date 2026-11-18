import { useEffect, useState } from 'react';
import { Check, Copy, Download, Play } from 'lucide-react';
import type { EvaluationReport, SentimentLabel } from '../types/kafka';

interface RepoFileItem {
  id: string;
  label: string;
  role: string;
  assetPath?: string;
  inlineContent?: string;
}

const DOCKER_COMPOSE_KRAFT = `services:
  kafka:
    image: apache/kafka:3.7.0
    ports:
      - "9092:9092"
    environment:
      KAFKA_NODE_ID: 1
      KAFKA_PROCESS_ROLES: broker,controller
      KAFKA_LISTENERS: PLAINTEXT://:9092,CONTROLLER://:9093
      KAFKA_ADVERTISED_LISTENERS: PLAINTEXT://localhost:9092
      KAFKA_CONTROLLER_LISTENER_NAMES: CONTROLLER
      KAFKA_LISTENER_SECURITY_PROTOCOL_MAP: CONTROLLER:PLAINTEXT,PLAINTEXT:PLAINTEXT
      KAFKA_CONTROLLER_QUORUM_VOTERS: 1@localhost:9093
      KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR: 1
      KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR: 1
      KAFKA_TRANSACTION_STATE_LOG_MIN_ISR: 1
      CLUSTER_ID: "MkU3OEVBNTcwNTJENDM2Qk"

  kafka-ui:
    image: provectuslabs/kafka-ui:latest
    depends_on:
      - kafka
    ports:
      - "8080:8080"
    environment:
      KAFKA_CLUSTERS_0_NAME: local
      KAFKA_CLUSTERS_0_BOOTSTRAPSERVERS: kafka:9092`;

const REPO_FILES: RepoFileItem[] = [
  {
    id: 'readme',
    label: 'README.md',
    role: 'Project guide, demo script, Kafka concepts & honest evaluation notes',
    assetPath: '/assets/aistudio/README_9b2ca9e9.md',
  },
  {
    id: 'docker',
    label: 'docker-compose.yml',
    role: 'Single-node Apache Kafka (KRaft mode, no ZooKeeper) + Kafka UI',
    inlineContent: DOCKER_COMPOSE_KRAFT,
  },
  {
    id: 'requirements',
    label: 'requirements.txt',
    role: 'Python dependencies (confluent-kafka, vaderSentiment, pandas, streamlit, plotly)',
    assetPath: '/assets/aistudio/requirements_7ab168d1.txt',
  },
  {
    id: 'config',
    label: 'config.py',
    role: 'Shared Kafka topics, partition count, SQLite path & alert window settings',
    assetPath: '/assets/aistudio/config_bc2c393b.py',
  },
  {
    id: 'create_topics',
    label: 'create_topics.py',
    role: 'Creates transport.feedback.raw, analyzed & alerts (3 partitions each)',
    assetPath: '/assets/aistudio/create_topics_fbe0f783.py',
  },
  {
    id: 'data_generator',
    label: 'data_generator.py',
    role: 'Synthetic commuter feedback generator with complaint burst simulation',
    assetPath: '/assets/aistudio/data_generator_fa3eaa40.py',
  },
  {
    id: 'producer',
    label: 'producer.py',
    role: 'Publishes synthetic stream or CSV dataset to transport.feedback.raw',
    assetPath: '/assets/aistudio/producer_f35a2418.py',
  },
  {
    id: 'sentiment',
    label: 'sentiment.py',
    role: 'VADER + custom transport domain lexicon & 8-category topic classifier',
    assetPath: '/assets/aistudio/sentiment_8248b603.py',
  },
  {
    id: 'alerts',
    label: 'alerts.py',
    role: 'Sliding-window negative complaint burst detector (>=70% in 30s)',
    assetPath: '/assets/aistudio/alerts_2a3176d2.py',
  },
  {
    id: 'processor',
    label: 'processor.py',
    role: 'Kafka consumer/producer scaling across sentiment-processor group',
    assetPath: '/assets/aistudio/processor_88d696bb.py',
  },
  {
    id: 'sink',
    label: 'sink.py',
    role: 'Independent sqlite-sink consumer writing idempotently to SQLite WAL',
    assetPath: '/assets/aistudio/sink_50c05a01.py',
  },
  {
    id: 'storage',
    label: 'storage.py',
    role: 'SQLite WAL schema and DataFrame query helpers',
    assetPath: '/assets/aistudio/storage_3781f75b.py',
  },
  {
    id: 'dashboard',
    label: 'dashboard.py',
    role: 'Streamlit + Plotly live dashboard alternative',
    assetPath: '/assets/aistudio/dashboard_57dea826.py',
  },
  {
    id: 'evaluate',
    label: 'evaluate.py',
    role: 'Accuracy, confusion matrix, precision, recall & F1 evaluation script',
    assetPath: '/assets/aistudio/evaluate_5f8db4d4.py',
  },
  {
    id: 'seed_db',
    label: 'seed_db.py',
    role: 'Populates demo SQLite data offline without running Kafka',
    assetPath: '/assets/aistudio/seed_db_8243c5be.py',
  },
  {
    id: 'test_pipeline',
    label: 'test_pipeline.py',
    role: 'Offline unit tests for sentiment negation, topics & alert cooldowns',
    assetPath: '/assets/aistudio/test_pipeline_3954b85c.py',
  },
];

const EVAL_LABELS: SentimentLabel[] = ['positive', 'neutral', 'negative'];

export function ProjectBlueprintView() {
  const [selectedFileId, setSelectedFileId] = useState<string>('readme');
  const [fileContents, setFileContents] = useState<Record<string, string>>({
    docker: DOCKER_COMPOSE_KRAFT,
  });
  const [loadingFile, setLoadingFile] = useState(false);
  const [copied, setCopied] = useState(false);

  const [evalReport, setEvalReport] = useState<EvaluationReport | null>(null);
  const [evalLoading, setEvalLoading] = useState(false);
  const [evalError, setEvalError] = useState<string | null>(null);

  const selectedFile = REPO_FILES.find((f) => f.id === selectedFileId) ?? REPO_FILES[0];

  useEffect(() => {
    if (!selectedFile.assetPath || fileContents[selectedFile.id] !== undefined) {
      return;
    }
    let cancelled = false;
    setLoadingFile(true);
    fetch(selectedFile.assetPath)
      .then((res) => {
        if (!res.ok) throw new Error('Unable to load file');
        return res.text();
      })
      .then((text) => {
        if (!cancelled) {
          setFileContents((prev) => ({ ...prev, [selectedFile.id]: text }));
        }
      })
      .catch(() => {
        if (!cancelled) {
          setFileContents((prev) => ({
            ...prev,
            [selectedFile.id]: '# Could not load attachment preview.',
          }));
        }
      })
      .finally(() => {
        if (!cancelled) setLoadingFile(false);
      });

    return () => {
      cancelled = true;
    };
  }, [selectedFile, fileContents]);

  const runEvaluation = async (sampleCount = 600) => {
    setEvalLoading(true);
    setEvalError(null);
    try {
      const res = await fetch(`/api/evaluate?n=${sampleCount}`);
      if (!res.ok) throw new Error('Evaluation failed');
      const data = (await res.json()) as EvaluationReport;
      setEvalReport(data);
    } catch (err) {
      setEvalError(err instanceof Error ? err.message : 'Unable to run benchmark');
    } finally {
      setEvalLoading(false);
    }
  };

  useEffect(() => {
    void runEvaluation(600);
  }, []);

  const currentCode =
    selectedFile.inlineContent ?? fileContents[selectedFile.id] ?? '';

  const handleCopy = async () => {
    if (!currentCode) return;
    try {
      await navigator.clipboard.writeText(currentCode);
      setCopied(true);
      setTimeout(() => setCopied(false), 1800);
    } catch {
      // Clipboard unavailable
    }
  };

  return (
    <div className="space-y-6">
      {/* Academic Abstract & Problem Statement */}
      <section className="bg-white border border-slate-200 rounded-lg p-6 space-y-5">
        <div className="border-b border-slate-200 pb-4">
          <div className="text-xs font-medium text-sky-700">
            Big Data Tools and Technologies (BDT) · Project Documentation
          </div>
          <h2 className="text-xl font-semibold text-slate-900 mt-1">
            Public Transportation Sentiment Analysis Using Apache Kafka
          </h2>
        </div>

        <div className="grid grid-cols-1 lg:grid-cols-2 gap-6 text-sm leading-relaxed text-slate-700">
          <div className="space-y-2">
            <h3 className="text-sm font-semibold text-slate-900">Problem Statement</h3>
            <p>
              Public transportation is an important part of daily life, and passengers frequently
              share experiences, complaints, and suggestions across social media, transit apps, and
              feedback forms. Analyzing these reactions in real time helps transit authorities detect
              delays, overcrowding, poor cleanliness, and service degradation early.
            </p>
          </div>

          <div className="space-y-2">
            <h3 className="text-sm font-semibold text-slate-900">Architecture & Solution</h3>
            <p>
              Messages flow from <span className="font-mono text-xs">producer.py</span> into{' '}
              <span className="font-mono text-xs">transport.feedback.raw</span> (3 partitions keyed
              by route), get scored by the <span className="font-mono text-xs">sentiment-processor</span>{' '}
              consumer group (VADER + transport lexicon + sliding-window alerts), and stream into{' '}
              <span className="font-mono text-xs">transport.feedback.analyzed</span> and{' '}
              <span className="font-mono text-xs">transport.alerts</span> for downstream storage and
              live monitoring.
            </p>
          </div>
        </div>
      </section>

      {/* End-to-End 3-Topic Kafka Data Flow Architecture Diagram */}
      <section className="bg-white border border-slate-200 rounded-lg p-6 space-y-4">
        <div>
          <h3 className="text-base font-semibold text-slate-900">
            01. End-to-End Apache Kafka Pipeline
          </h3>
          <p className="text-xs text-slate-500 mt-0.5">
            Decoupled topics with independent consumer groups (<span className="font-mono">sentiment-processor</span> &amp; <span className="font-mono">sqlite-sink</span>)
          </p>
        </div>

        <div className="overflow-x-auto pt-2">
          <svg
            viewBox="0 0 980 225"
            className="w-full min-w-[760px] h-auto"
            role="img"
            aria-label="Apache Kafka Three-Topic Pipeline Architecture"
          >
            <defs>
              <marker
                id="arrow-arch"
                viewBox="0 0 10 10"
                refX="9"
                refY="5"
                markerWidth="6"
                markerHeight="6"
                orient="auto-start-reverse"
              >
                <path d="M 0 1 L 10 5 L 0 9 z" fill="#64748b" />
              </marker>
            </defs>

            {/* Producer */}
            <rect x="10" y="55" width="155" height="110" rx="8" fill="#f0f9ff" stroke="#0284c7" />
            <text x="87" y="84" textAnchor="middle" className="fill-sky-900 text-[12px] font-semibold">
              producer.py
            </text>
            <text x="87" y="106" textAnchor="middle" className="fill-sky-800 text-[10px]">
              Synthetic Stream /
            </text>
            <text x="87" y="122" textAnchor="middle" className="fill-sky-800 text-[10px]">
              CSV Replay
            </text>
            <text x="87" y="146" textAnchor="middle" className="fill-sky-700 text-[10px] font-mono">
              key = route
            </text>

            <line
              x1="165"
              y1="110"
              x2="210"
              y2="110"
              stroke="#64748b"
              strokeWidth="1.5"
              markerEnd="url(#arrow-arch)"
            />

            {/* Topic Raw */}
            <rect x="215" y="45" width="185" height="130" rx="8" fill="#ffffff" stroke="#0f172a" />
            <text x="307" y="68" textAnchor="middle" className="fill-slate-900 text-[11px] font-semibold font-mono">
              transport.feedback.raw
            </text>
            <rect x="232" y="82" width="151" height="24" rx="4" fill="#f1f5f9" stroke="#cbd5e1" />
            <text x="307" y="98" textAnchor="middle" className="fill-slate-700 text-[10px] font-mono">
              Partition 0 (Metro)
            </text>
            <rect x="232" y="112" width="151" height="24" rx="4" fill="#f1f5f9" stroke="#cbd5e1" />
            <text x="307" y="128" textAnchor="middle" className="fill-slate-700 text-[10px] font-mono">
              Partition 1 (Bus)
            </text>
            <rect x="232" y="142" width="151" height="24" rx="4" fill="#f1f5f9" stroke="#cbd5e1" />
            <text x="307" y="158" textAnchor="middle" className="fill-slate-700 text-[10px] font-mono">
              Partition 2 (Train)
            </text>

            <line
              x1="400"
              y1="110"
              x2="445"
              y2="110"
              stroke="#64748b"
              strokeWidth="1.5"
              markerEnd="url(#arrow-arch)"
            />

            {/* Processor */}
            <rect x="450" y="38" width="175" height="145" rx="8" fill="#f0fdf4" stroke="#16a34a" />
            <text x="537" y="62" textAnchor="middle" className="fill-emerald-950 text-[12px] font-semibold">
              processor.py
            </text>
            <text x="537" y="80" textAnchor="middle" className="fill-emerald-800 text-[10px] font-mono">
              group: sentiment-processor
            </text>
            <text x="537" y="106" textAnchor="middle" className="fill-emerald-900 text-[10px]">
              • VADER + Domain Lexicon
            </text>
            <text x="537" y="126" textAnchor="middle" className="fill-emerald-900 text-[10px]">
              • 8-Topic Regex Tagger
            </text>
            <text x="537" y="146" textAnchor="middle" className="fill-emerald-900 text-[10px]">
              • 30s Sliding-Window Alerts
            </text>

            <line
              x1="625"
              y1="82"
              x2="670"
              y2="70"
              stroke="#64748b"
              strokeWidth="1.5"
              markerEnd="url(#arrow-arch)"
            />
            <line
              x1="625"
              y1="138"
              x2="670"
              y2="152"
              stroke="#dc2626"
              strokeWidth="1.5"
              markerEnd="url(#arrow-arch)"
            />

            {/* Output Topics */}
            <rect x="675" y="32" width="160" height="68" rx="6" fill="#ffffff" stroke="#0f172a" />
            <text x="755" y="57" textAnchor="middle" className="fill-slate-900 text-[10px] font-semibold font-mono">
              transport.feedback.analyzed
            </text>
            <text x="755" y="78" textAnchor="middle" className="fill-slate-500 text-[10px]">
              3 partitions · scored JSON
            </text>

            <rect x="675" y="120" width="160" height="68" rx="6" fill="#fef2f2" stroke="#dc2626" />
            <text x="755" y="146" textAnchor="middle" className="fill-red-900 text-[11px] font-semibold font-mono">
              transport.alerts
            </text>
            <text x="755" y="166" textAnchor="middle" className="fill-red-700 text-[10px]">
              &ge; 70% negative in 30s
            </text>

            <line
              x1="835"
              y1="66"
              x2="868"
              y2="98"
              stroke="#64748b"
              strokeWidth="1.5"
              markerEnd="url(#arrow-arch)"
            />
            <line
              x1="835"
              y1="154"
              x2="868"
              y2="122"
              stroke="#64748b"
              strokeWidth="1.5"
              markerEnd="url(#arrow-arch)"
            />

            {/* Sink & UI */}
            <rect x="872" y="55" width="98" height="110" rx="8" fill="#0f172a" />
            <text x="921" y="88" textAnchor="middle" className="fill-white text-[11px] font-semibold">
              sink.py
            </text>
            <text x="921" y="108" textAnchor="middle" className="fill-slate-300 text-[9px] font-mono">
              sqlite-sink
            </text>
            <text x="921" y="130" textAnchor="middle" className="fill-slate-300 text-[10px]">
              SQLite WAL
            </text>
            <text x="921" y="148" textAnchor="middle" className="fill-sky-400 text-[10px]">
              Live Dashboard
            </text>
          </svg>
        </div>
      </section>

      {/* Model Evaluation Benchmark (mirrors evaluate.py) */}
      <section className="bg-white border border-slate-200 rounded-lg p-6 space-y-5">
        <div className="flex flex-wrap items-start justify-between gap-4 border-b border-slate-200 pb-4">
          <div>
            <h3 className="text-base font-semibold text-slate-900">
              02. Sentiment Model Evaluation (<span className="font-mono">evaluate.py</span>)
            </h3>
            <p className="text-xs text-slate-500 mt-0.5">
              Live confusion matrix, precision, recall, and F1 scores computed by the server NLP analyzer
            </p>
          </div>
          <button
            type="button"
            onClick={() => runEvaluation(600)}
            disabled={evalLoading}
            className="inline-flex items-center gap-1.5 h-8 px-3.5 rounded-md border border-slate-200 bg-white text-xs font-medium text-slate-700 hover:bg-slate-50 cursor-pointer whitespace-nowrap shrink-0 disabled:opacity-50"
          >
            <Play className="h-3.5 w-3.5 text-sky-600" />
            {evalLoading ? 'Running...' : 'Re-run Evaluation (600 msgs)'}
          </button>
        </div>

        {evalError && (
          <div className="text-xs text-red-700 bg-red-50 border border-red-200 rounded-md p-3">
            {evalError}
          </div>
        )}

        {evalReport && (
          <div className="space-y-4">
            <div className="grid grid-cols-1 lg:grid-cols-12 gap-6">
              {/* Confusion Matrix */}
              <div className="lg:col-span-6 space-y-2">
                <div className="flex items-center justify-between text-xs">
                  <span className="font-semibold text-slate-800">
                    Confusion Matrix (Rows = Actual, Cols = Predicted)
                  </span>
                  <span className="font-mono tabular-nums text-slate-700">
                    Accuracy: <strong>{evalReport.accuracy}%</strong> ({evalReport.samples} samples)
                  </span>
                </div>
                <div className="border border-slate-200 rounded-md overflow-hidden">
                  <table className="w-full text-xs border-collapse">
                    <thead>
                      <tr className="bg-slate-50 border-b border-slate-200 text-slate-500">
                        <th className="py-2 px-3 text-left font-medium">Actual \ Pred</th>
                        {EVAL_LABELS.map((col) => (
                          <th key={col} className="py-2 px-3 text-right font-medium capitalize">
                            {col}
                          </th>
                        ))}
                      </tr>
                    </thead>
                    <tbody className="divide-y divide-slate-100 font-mono tabular-nums">
                      {EVAL_LABELS.map((row) => (
                        <tr key={row}>
                          <td className="py-2 px-3 font-sans font-medium text-slate-800 capitalize">
                            {row}
                          </td>
                          {EVAL_LABELS.map((col) => {
                            const val = evalReport.confusionMatrix[row][col];
                            const isDiag = row === col;
                            return (
                              <td
                                key={col}
                                className={`py-2 px-3 text-right ${
                                  isDiag
                                    ? 'font-semibold text-emerald-700 bg-emerald-50/40'
                                    : val > 0
                                    ? 'text-red-700'
                                    : 'text-slate-400'
                                }`}
                              >
                                {val}
                              </td>
                            );
                          })}
                        </tr>
                      ))}
                    </tbody>
                  </table>
                </div>
              </div>

              {/* Per-Class Precision / Recall / F1 */}
              <div className="lg:col-span-6 space-y-2">
                <div className="text-xs font-semibold text-slate-800">
                  Per-Class Classification Metrics
                </div>
                <div className="border border-slate-200 rounded-md overflow-hidden">
                  <table className="w-full text-xs border-collapse">
                    <thead>
                      <tr className="bg-slate-50 border-b border-slate-200 text-slate-500">
                        <th className="py-2 px-3 text-left font-medium">Class</th>
                        <th className="py-2 px-3 text-right font-medium">Precision</th>
                        <th className="py-2 px-3 text-right font-medium">Recall</th>
                        <th className="py-2 px-3 text-right font-medium">F1 Score</th>
                        <th className="py-2 px-3 text-right font-medium">Support</th>
                      </tr>
                    </thead>
                    <tbody className="divide-y divide-slate-100 font-mono tabular-nums">
                      {evalReport.classes.map((c) => (
                        <tr key={c.label}>
                          <td className="py-2 px-3 font-sans font-medium text-slate-800 capitalize">
                            {c.label}
                          </td>
                          <td className="py-2 px-3 text-right text-slate-700">
                            {c.precision.toFixed(2)}
                          </td>
                          <td className="py-2 px-3 text-right text-slate-700">
                            {c.recall.toFixed(2)}
                          </td>
                          <td className="py-2 px-3 text-right font-semibold text-slate-900">
                            {c.f1.toFixed(2)}
                          </td>
                          <td className="py-2 px-3 text-right text-slate-500">{c.support}</td>
                        </tr>
                      ))}
                    </tbody>
                  </table>
                </div>
              </div>
            </div>

            <div className="bg-slate-50 border border-slate-200 rounded-md p-3 text-xs text-slate-600 leading-relaxed">
              <strong className="text-slate-800">Academic Note:</strong> {evalReport.note}
            </div>
          </div>
        )}
      </section>

      {/* Complete 16-File BDT Python + Kafka Reference Repository */}
      <section className="bg-white border border-slate-200 rounded-lg overflow-hidden">
        <div className="px-6 py-4 border-b border-slate-200 flex flex-wrap items-center justify-between gap-4">
          <div>
            <h3 className="text-base font-semibold text-slate-900">
              03. Complete Apache Kafka &amp; Python Project Files
            </h3>
            <p className="text-xs text-slate-500 mt-0.5">
              Browse, copy, or download all Python pipeline modules, unit tests, SQLite sink, and documentation
            </p>
          </div>

          <div className="flex items-center gap-2 shrink-0">
            {selectedFile.assetPath && (
              <a
                href={selectedFile.assetPath}
                download={selectedFile.label}
                className="inline-flex items-center gap-1.5 h-8 px-3 rounded-md border border-slate-200 bg-white text-xs font-medium text-slate-700 hover:bg-slate-50 whitespace-nowrap shrink-0"
              >
                <Download className="h-3.5 w-3.5" />
                Download
              </a>
            )}
            <button
              type="button"
              onClick={handleCopy}
              disabled={!currentCode}
              className="inline-flex items-center gap-1.5 h-8 px-3 rounded-md border border-slate-200 bg-white text-xs font-medium text-slate-700 hover:bg-slate-50 cursor-pointer whitespace-nowrap shrink-0 disabled:opacity-50"
            >
              {copied ? (
                <>
                  <Check className="h-3.5 w-3.5 text-emerald-600" />
                  Copied
                </>
              ) : (
                <>
                  <Copy className="h-3.5 w-3.5" />
                  Copy File
                </>
              )}
            </button>
          </div>
        </div>

        <div className="grid grid-cols-1 lg:grid-cols-12">
          {/* File Tree Sidebar */}
          <div className="lg:col-span-4 border-b lg:border-b-0 lg:border-r border-slate-200 divide-y divide-slate-100 max-h-[520px] overflow-y-auto">
            {REPO_FILES.map((file) => {
              const isActive = selectedFileId === file.id;
              return (
                <button
                  key={file.id}
                  type="button"
                  onClick={() => setSelectedFileId(file.id)}
                  className={`w-full text-left px-4 py-2.5 transition-colors cursor-pointer ${
                    isActive ? 'bg-sky-50' : 'hover:bg-slate-50'
                  }`}
                >
                  <div
                    className={`font-mono text-xs font-medium ${
                      isActive ? 'text-sky-700' : 'text-slate-900'
                    }`}
                  >
                    {file.label}
                  </div>
                  <div className="text-[11px] text-slate-500 mt-0.5 line-clamp-2">
                    {file.role}
                  </div>
                </button>
              );
            })}
          </div>

          {/* Code Viewer */}
          <div className="lg:col-span-8 bg-slate-900 text-slate-100 p-5 max-h-[520px] overflow-auto">
            <div className="text-xs text-slate-400 mb-3 font-mono border-b border-slate-800 pb-2">
              {selectedFile.label} — {selectedFile.role}
            </div>
            {loadingFile && !currentCode ? (
              <div className="text-xs text-slate-400 font-mono">Loading {selectedFile.label}...</div>
            ) : (
              <pre className="text-xs font-mono leading-relaxed whitespace-pre-wrap break-words">
                {currentCode}
              </pre>
            )}
          </div>
        </div>
      </section>
    </div>
  );
}
