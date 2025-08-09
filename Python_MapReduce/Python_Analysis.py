#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import pandas as pd
from pathlib import Path
import re

# ========================
# Config (edit as needed)
# ========================
PART_PATH   = "part-00000"          # e.g. /mnt/data/part-00000
TITLES_PATH = "movie_titles.csv"    # e.g. /mnt/data/movie_titles.csv
OUT_PATH    = "movies_summary.csv"
MIN_REVIEWS = 100
TOP_N       = 3

# IMPORTANT: your reducer now outputs: movie_id, avg, POS, NEG, NEU, total
# If it flips back to POS, NEU, NEG later, change COUNT_ORDER below.
COUNT_ORDER = "pos_neg_neu"  # or "pos_neu_neg"

# ========================
# Helpers
# ========================
def _clean_num(x):
    """Strip stray labels/brackets, keep only digits/sign/decimal, coerce to number."""
    if pd.isna(x):
        return pd.NA
    s = str(x)
    s = s.replace("[", "").replace("]", "")
    for tok in ("Average:", "avg:", "Pos:", "pos:", "Neu:", "neu:",
                "Neg:", "neg:", "Total:", "total:", "\t"):
        s = s.replace(tok, "")
    s = re.sub(r"[^0-9.\-]", "", s)  # keep digits, sign, dot
    return pd.to_numeric(s, errors="coerce")

def load_part(path: str) -> pd.DataFrame:
    """
    Read 6 columns from part-00000:
      movie_id, avg_rating, c3, c4, c5, total
    Map c3..c5 to POS/NEG/NEU according to COUNT_ORDER.
    """
    df = pd.read_csv(
        path,
        header=None,
        names=["movie_id", "avg_rating", "c3", "c4", "c5", "total"],
        engine="python",
        sep=r"[,\t]+",   # comma or tab
        dtype=str
    )
    # Clean/convert each column (avoid deprecated applymap)
    for col in ["movie_id", "avg_rating", "c3", "c4", "c5", "total"]:
        df[col] = df[col].map(_clean_num)

    # Drop rows we couldn't parse
    df = df.dropna(subset=["movie_id", "avg_rating", "c3", "c4", "c5", "total"]).copy()

    # Map counts based on declared order
    if COUNT_ORDER == "pos_neg_neu":
        df["pos"] = df["c3"].astype(int)
        df["neg"] = df["c4"].astype(int)
        df["neu"] = df["c5"].astype(int)
    elif COUNT_ORDER == "pos_neu_neg":
        df["pos"] = df["c3"].astype(int)
        df["neu"] = df["c4"].astype(int)
        df["neg"] = df["c5"].astype(int)
    else:
        raise ValueError("COUNT_ORDER must be 'pos_neg_neu' or 'pos_neu_neg'.")

    df["movie_id"]   = df["movie_id"].astype(int)
    df["avg_rating"] = df["avg_rating"].astype(float)
    df["total"]      = df["total"].astype(int)

    # Fix inconsistent totals (just in case)
    comp_total = df["pos"] + df["neg"] + df["neu"]
    df.loc[df["total"] != comp_total, "total"] = comp_total

    # Percentages (neutrals included in denominator)
    df["positive_percentage"] = (df["pos"] / df["total"]) * 100
    df["negative_percentage"] = (df["neg"] / df["total"]) * 100

    return df[["movie_id", "avg_rating", "pos", "neg", "neu", "total",
               "positive_percentage", "negative_percentage"]]

def load_titles(path: str) -> pd.DataFrame:
    """Netflix titles file: movie_id,year,title (title may contain commas)."""
    rows = []
    with open(path, "r", encoding="ISO-8859-1", errors="ignore") as f:
        for line in f:
            line = line.rstrip("\n")
            if not line:
                continue
            parts = line.split(",", 2)
            if len(parts) < 3:
                continue
            mid_raw, year_raw, title = parts
            try:
                mid = int((mid_raw.strip().lstrip("0") or "0"))
            except Exception:
                continue
            year = pd.to_numeric(year_raw.strip(), errors="coerce")
            rows.append({"movie_id": mid, "year": year, "title": title.strip()})
    return pd.DataFrame(rows)

def board(df: pd.DataFrame, category: str, sort_cols, ascending) -> pd.DataFrame:
    b = df.sort_values(by=sort_cols, ascending=ascending).head(TOP_N).copy()
    b.insert(0, "category", category)
    return b

# ========================
# Main
# ========================
def main():
    if not Path(PART_PATH).exists():
        raise FileNotFoundError(f"MapReduce output not found: {PART_PATH}")
    if not Path(TITLES_PATH).exists():
        raise FileNotFoundError(f"Titles file not found: {TITLES_PATH}")

    mr     = load_part(PART_PATH)
    titles = load_titles(TITLES_PATH)
    df     = mr.merge(titles, on="movie_id", how="left")

    # Apply ≥100 filter for all boards
    df100 = df[df["total"] >= MIN_REVIEWS].copy()

    # Four leaderboards (tie-break: larger total, then smaller movie_id)
    b_pos      = board(df100, "Top 3 Highest Positive %",
                       ["positive_percentage", "total", "movie_id"], [False, False, True])
    b_low_avg  = board(df100, "Top 3 Lowest Avg Ratings",
                       ["avg_rating", "total", "movie_id"], [True,  False, True])
    b_high_avg = board(df100, "Top 3 Highest Avg Ratings",
                       ["avg_rating", "total", "movie_id"], [False, False, True])
    b_neg      = board(df100, "Top 3 Highest Negative %",
                       ["negative_percentage", "total", "movie_id"], [False, False, True])

    export_df = pd.concat([b_pos, b_low_avg, b_high_avg, b_neg], ignore_index=True)

    # Round for readability
    export_df["avg_rating"]           = export_df["avg_rating"].round(2)
    export_df["positive_percentage"]  = export_df["positive_percentage"].round(2)
    export_df["negative_percentage"]  = export_df["negative_percentage"].round(2)

    # Keep only the columns you want in the CSV
    export_df = export_df[["category", "title", "avg_rating",
                           "positive_percentage", "negative_percentage"]]

    # Save a CSV that opens nicely in Excel
    export_df.to_csv(OUT_PATH, index=False, encoding="utf-8-sig")

    print(f"Saved: {OUT_PATH}")
    print(export_df.to_string(index=False))

if __name__ == "__main__":
    main()
