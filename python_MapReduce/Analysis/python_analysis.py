#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import pandas as pd
from pathlib import Path
import re

# ------------------------
# Configuration parameters
# ------------------------
PART_PATH   = "part-00000"          # reducer output file (CSV/TSV mixed)
TITLES_PATH = "movie_titles.csv"    # titles file: movie_id,year,title
OUT_PATH    = "movies_summary.csv"  # export path
MIN_REVIEWS = 100                   # minimum reviews for leaderboards
TOP_N       = 3                     # number of items per board

# The reducer outputs: movie_id, avg, POS, NEG, NEU, total.
# If the order of the three counts changes, adjust COUNT_ORDER.
COUNT_ORDER = "pos_neg_neu"         # or "pos_neu_neg"

# ------------------------
# Helpers
# ------------------------
def _clean_num(x):
    """Keep digits/sign/decimal only; convert to number (NaN on failure)."""
    if pd.isna(x):
        return pd.NA
    s = str(x)
    s = s.replace("[", "").replace("]", "")
    for tok in ("Average:", "avg:", "Pos:", "pos:", "Neu:", "neu:",
                "Neg:", "neg:", "Total:", "total:", "\t"):
        s = s.replace(tok, "")
    s = re.sub(r"[^0-9.\-]", "", s)  # strip non-numeric chars
    return pd.to_numeric(s, errors="coerce")

def load_part(path: str) -> pd.DataFrame:
    """
    Read reducer output with 6 columns:
      movie_id, avg_rating, c3, c4, c5, total
    Map c3..c5 into pos/neg/neu according to COUNT_ORDER.
    """
    df = pd.read_csv(
        path,
        header=None,
        names=["movie_id", "avg_rating", "c3", "c4", "c5", "total"],
        engine="python",
        sep=r"[,\t]+",   # accept comma or tab
        dtype=str
    )
    # Clean/convert columns
    for col in ["movie_id", "avg_rating", "c3", "c4", "c5", "total"]:
        df[col] = df[col].map(_clean_num)

    # Remove rows that failed to parse
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

    # Final typed columns
    df["movie_id"]   = df["movie_id"].astype(int)
    df["avg_rating"] = df["avg_rating"].astype(float)
    df["total"]      = df["total"].astype(int)

    # Ensure total equals pos+neg+neu
    comp_total = df["pos"] + df["neg"] + df["neu"]
    df.loc[df["total"] != comp_total, "total"] = comp_total

    # Percentages (denominator includes neutrals)
    df["positive_percentage"] = (df["pos"] / df["total"]) * 100
    df["negative_percentage"] = (df["neg"] / df["total"]) * 100

    return df[["movie_id", "avg_rating", "pos", "neg", "neu", "total",
               "positive_percentage", "negative_percentage"]]

def load_titles(path: str) -> pd.DataFrame:
    """Parse titles file where the title can contain commas: split only twice."""
    rows = []
    with open(path, "r", encoding="ISO-8859-1", errors="ignore") as f:
        for line in f:
            line = line.rstrip("\n")
            if not line:
                continue
            parts = line.split(",", 2)  # movie_id,year,title
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
    """Return TOP_N rows for a category using the provided sort order."""
    b = df.sort_values(by=sort_cols, ascending=ascending).head(TOP_N).copy()
    b.insert(0, "category", category)
    return b

# ------------------------
# Main
# ------------------------
def main():
    # Validate inputs exist
    if not Path(PART_PATH).exists():
        raise FileNotFoundError(f"MapReduce output not found: {PART_PATH}")
    if not Path(TITLES_PATH).exists():
        raise FileNotFoundError(f"Titles file not found: {TITLES_PATH}")

    # Load MR results and titles, then join on movie_id
    mr     = load_part(PART_PATH)
    titles = load_titles(TITLES_PATH)
    df     = mr.merge(titles, on="movie_id", how="left")

    # Keep movies with at least MIN_REVIEWS
    df100 = df[df["total"] >= MIN_REVIEWS].copy()

    # Leaderboards with deterministic tie-breaks (total desc, movie_id asc)
    b_pos      = board(df100, "Top 3 Highest Positive %",
                       ["positive_percentage", "total", "movie_id"], [False, False, True])
    b_low_avg  = board(df100, "Top 3 Lowest Avg Ratings",
                       ["avg_rating", "total", "movie_id"], [True,  False, True])
    b_high_avg = board(df100, "Top 3 Highest Avg Ratings",
                       ["avg_rating", "total", "movie_id"], [False, False, True])
    b_neg      = board(df100, "Top 3 Highest Negative %",
                       ["negative_percentage", "total", "movie_id"], [False, False, True])

    # Concatenate boards for export
    export_df = pd.concat([b_pos, b_low_avg, b_high_avg, b_neg], ignore_index=True)

    # Round numeric columns for readability
    export_df["avg_rating"]           = export_df["avg_rating"].round(2)
    export_df["positive_percentage"]  = export_df["positive_percentage"].round(2)
    export_df["negative_percentage"]  = export_df["negative_percentage"].round(2)

    # Final column order for the CSV
    export_df = export_df[["category", "title", "avg_rating",
                           "positive_percentage", "negative_percentage"]]

    # Write CSV with BOM so Excel displays UTF-8 correctly
    export_df.to_csv(OUT_PATH, index=False, encoding="utf-8-sig")

    print(f"Saved: {OUT_PATH}")
    print(export_df.to_string(index=False))

if __name__ == "__main__":
    main()
