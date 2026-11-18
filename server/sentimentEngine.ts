import type { IssueCategory, SentimentLabel } from '../src/types/kafka.ts';

// VADER + transport-domain lexicon (mirrors sentiment.py valence scale -4..+4)
const DOMAIN_LEXICON: Record<string, number> = {
  // Complaints
  delayed: -2.4,
  delay: -2.0,
  delays: -2.0,
  late: -1.6,
  cancelled: -2.4,
  canceled: -2.4,
  breakdown: -2.3,
  stuck: -1.8,
  stranded: -2.2,
  wait: -0.8,
  waiting: -1.0,
  overcrowded: -2.6,
  crowded: -1.9,
  packed: -1.6,
  stuffy: -1.6,
  filthy: -2.8,
  dirty: -2.3,
  smelly: -2.0,
  smells: -1.8,
  garbage: -2.0,
  broken: -2.0,
  unsafe: -2.6,
  rash: -2.0,
  rashly: -2.0,
  rude: -2.5,
  stolen: -2.2,
  overcharged: -2.2,
  'rip-off': -2.6,
  ripoff: -2.6,
  crashing: -2.0,
  crashes: -2.0,
  unacceptable: -2.6,
  useless: -2.4,
  bumpy: -1.4,
  hiked: -1.4,
  unhelpful: -2.2,
  uninformed: -1.8,
  terrible: -2.5,
  frustrating: -2.1,
  nightmare: -2.6,
  disgusting: -2.7,
  dangerous: -2.4,
  joke: -1.5,
  sweating: -1.5,
  wrong: -1.8,
  ignored: -1.9,
  refused: -1.7,
  hate: -2.7,
  poor: -1.9,
  awful: -2.6,

  // Neutral domain nouns
  stop: 0.0,
  stops: 0.0,

  // Praise
  punctual: 2.4,
  spotless: 2.7,
  clean: 1.9,
  smooth: 1.7,
  comfortable: 2.1,
  polite: 2.1,
  helpful: 2.1,
  reliable: 2.2,
  accurate: 1.8,
  quick: 1.5,
  affordable: 1.8,
  reasonable: 1.4,
  'hassle-free': 2.0,
  peaceful: 1.8,
  great: 2.3,
  impressed: 2.1,
  love: 2.6,
  easy: 1.7,
  relaxing: 1.9,
  solved: 1.6,
  fresh: 1.5,
  good: 1.8,
  safe: 2.0,
  early: 1.2,
};

const PHRASES: Array<[RegExp, string]> = [
  [/\bon[- ]time\b/gi, 'punctual'],
  [/\bpacked like sardines\b/gi, 'overcrowded'],
  [/\bno (?:space|room|seats?)\b/gi, 'overcrowded'],
  [/\brip[- ]off\b/gi, 'rip-off'],
  [/\blate[- ]night\b/gi, 'night'],
  [/\bnobody (?:helped|cares|came)\b/gi, 'unhelpful'],
  [/\b(?:can'?t|cannot|couldn'?t) stand\b/gi, 'hate'],
  [/\b(?:not|wasn'?t|isn'?t|never|doesn'?t|didn'?t) work(?:ing|s|ed)?\b/gi, 'broken'],
  [/\bno (?:announcements?|updates?|information)\b/gi, 'uninformed'],
  [/\bhassle[- ]free\b/gi, 'hassle-free'],
];

const TOPIC_RULES: Array<[IssueCategory, RegExp]> = [
  [
    'Delay & punctuality',
    /delay|\blate\b|\bwait|punctual|on[- ]time|schedule|cancel|breakdown|stuck|stranded|frequen/gi,
  ],
  [
    'Safety & security',
    /unsafe|\bsafe|safety|rash|reckless|danger|harass|theft|stolen|accident|lighting|\bdark/gi,
  ],
  [
    'Overcrowding',
    /crowd|packed|sardines|no (?:space|room|seats?)|\bseats?\b|standing|rush hour|congest/gi,
  ],
  [
    'Cleanliness',
    /clean|dirt|filth|smell|garbage|\bbins?\b|litter|toilet|stink|spotless|hygien/gi,
  ],
  [
    'Staff behaviour',
    /driver|conductor|staff|guard|rude|polite|helpful|behaviou?r|attitude/gi,
  ],
  [
    'Fares & ticketing',
    /\bfares?\b|ticket|price|\bpass\b|smart ?card|top-?up|rip-?off|refund|charge|expensive|afford/gi,
  ],
  [
    'Comfort & facilities',
    /\bAC\b|air[- ]?con|seat|comfort|ride|smooth|bumpy|noise|stuffy|ventilat|facilit|escalator|\blift\b/gi,
  ],
  [
    'App & information',
    /\bapp\b|tracking|announce|information|update|display|website|signage|real-?time|timetable/gi,
  ],
];

const NEGATIONS = new Set([
  'not',
  'no',
  'never',
  "wasn't",
  'wasnt',
  "isn't",
  'isnt',
  "didn't",
  'didnt',
  "doesn't",
  'doesnt',
  "can't",
  'cant',
  'barely',
  'hardly',
  'without',
]);

const INTENSIFIERS = new Set([
  'very',
  'extremely',
  'super',
  'really',
  'totally',
  'completely',
  'utterly',
  'so',
  'incredibly',
  'absolutely',
]);

function normaliseText(text: string): string {
  let out = text;
  for (const [pattern, replacement] of PHRASES) {
    out = out.replace(pattern, replacement);
  }
  return out;
}

export function classifyTopic(text: string): IssueCategory {
  let bestTopic: IssueCategory = 'General';
  let bestCount = 0;

  for (const [topic, regex] of TOPIC_RULES) {
    const matches = text.match(regex);
    const count = matches ? matches.length : 0;
    if (count > bestCount) {
      bestCount = count;
      bestTopic = topic;
    }
  }

  return bestTopic;
}

export interface SentimentAnalysisResult {
  sentiment: SentimentLabel;
  score: number;
  confidence: number;
  aspect: IssueCategory;
  matchedTerms: string[];
}

export function analyzeTransitFeedback(text: string): SentimentAnalysisResult {
  const normalized = normaliseText(text).toLowerCase();
  const rawTokens = normalized.match(/[a-z'-]+/g) ?? [];

  let sumValence = 0;
  let matchCount = 0;
  const matchedTerms: string[] = [];

  for (let i = 0; i < rawTokens.length; i++) {
    const token = rawTokens[i].replace(/^'+|'+$/g, '');
    const valence = DOMAIN_LEXICON[token];
    if (valence === undefined || valence === 0) continue;

    let adjusted = valence;
    const prev1 = i > 0 ? rawTokens[i - 1] : '';
    const prev2 = i > 1 ? rawTokens[i - 2] : '';

    const negated = NEGATIONS.has(prev1) || NEGATIONS.has(prev2);
    const intensified = INTENSIFIERS.has(prev1);

    if (negated) {
      adjusted = -adjusted * 0.74;
    } else if (intensified) {
      adjusted = adjusted * 1.29;
    }

    sumValence += adjusted;
    matchCount += 1;
    matchedTerms.push(negated ? `${prev1} ${token}`.trim() : token);
  }

  // Exclamation boost (VADER-style)
  const exclamations = Math.min(4, (text.match(/!/g) ?? []).length);
  if (exclamations > 0 && sumValence !== 0) {
    sumValence += Math.sign(sumValence) * exclamations * 0.292;
  }

  // Emoji check matching data_generator.py
  if (text.includes('😊') || text.includes('👍')) {
    sumValence += 1.8;
    matchCount += 1;
  }
  if (text.includes('😡') || text.includes('😤')) {
    sumValence -= 2.2;
    matchCount += 1;
  }

  // VADER normalization: x / sqrt(x^2 + alpha), alpha = 15
  const compound = sumValence === 0 ? 0 : sumValence / Math.sqrt(sumValence * sumValence + 15);
  const score = Number(Math.max(-1, Math.min(1, compound)).toFixed(4));

  let sentiment: SentimentLabel = 'neutral';
  if (score >= 0.05) sentiment = 'positive';
  else if (score <= -0.05) sentiment = 'negative';

  const confidence = Number(
    Math.min(0.99, Math.max(0.62, 0.65 + Math.abs(score) * 0.3 + matchCount * 0.02)).toFixed(2)
  );

  return {
    sentiment,
    score,
    confidence,
    aspect: classifyTopic(text),
    matchedTerms: Array.from(new Set(matchedTerms)).slice(0, 5),
  };
}
