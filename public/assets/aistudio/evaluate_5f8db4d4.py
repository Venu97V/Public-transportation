"""Measure how well the sentiment model agrees with known labels.

  python evaluate.py                                        # synthetic data (optimistic!)
  python evaluate.py --csv labelled.csv --text-col text --label-col airline_sentiment

For the project report, evaluate on a real labelled dataset: the synthetic
messages were written from the same vocabulary the model knows, so their
accuracy is an upper bound, not a realistic figure.
"""
import argparse

import pandas as pd

from data_generator import FeedbackGenerator
from sentiment import analyze

CLASSES = ["positive", "neutral", "negative"]


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--csv")
    ap.add_argument("--text-col", default="text")
    ap.add_argument("--label-col", default="label")
    ap.add_argument("--n", type=int, default=2000, help="synthetic messages to generate")
    args = ap.parse_args()

    if args.csv:
        df = pd.read_csv(args.csv)
        texts, truth = df[args.text_col].astype(str), df[args.label_col].str.lower().str.strip()
    else:
        gen = FeedbackGenerator(incidents=False, seed=1)
        pairs = [gen.next() for _ in range(args.n)]
        texts = pd.Series([r["text"] for r, _ in pairs])
        truth = pd.Series([label for _, label in pairs])

    pred = texts.map(lambda t: analyze(t)["sentiment"])
    print(f"samples: {len(texts)}   accuracy: {(pred == truth).mean():.1%}\n")

    print("confusion matrix (rows = actual, columns = predicted)")
    print(pd.crosstab(truth, pred).reindex(index=CLASSES, columns=CLASSES, fill_value=0), "\n")

    print(f"{'class':<10}{'precision':>10}{'recall':>9}{'f1':>7}")
    for c in CLASSES:
        tp = ((pred == c) & (truth == c)).sum()
        precision = tp / max((pred == c).sum(), 1)
        recall = tp / max((truth == c).sum(), 1)
        f1 = 2 * precision * recall / max(precision + recall, 1e-9)
        print(f"{c:<10}{precision:>10.2f}{recall:>9.2f}{f1:>7.2f}")


if __name__ == "__main__":
    main()
