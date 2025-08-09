#!/usr/bin/env python3
import sys, csv
from collections import defaultdict

# Mapper: aggregate per movie locally to reduce shuffle.
# stats[key] -> [sum_of_ratings, positive_count, negative_count, neutral_count, total_count]
stats = defaultdict(lambda: [0, 0, 0, 0, 0])

reader = csv.reader(sys.stdin)  # read CSV rows from Hadoop Streaming stdin
for row in reader:
    if not row or len(row) < 3:
        continue
    # Skip header safely if present: customer_id,movie_id,rating
    if row[0].lower() == "customer_id" or row[1].lower() == "movie_id" or row[2].lower() == "rating":
        continue
    try:
        mid_num = int(row[1].strip())     # parse movie_id
        r = int(row[2])                   # parse rating
    except Exception:
        continue

    key = f"{mid_num:05d}"                # zero-pad so Hadoop's text sort preserves numeric order

    # Update local aggregates
    s, p, n, u, c = stats[key]
    s += r
    p += 1 if r in (4, 5) else 0          # 4–5 = positive
    n += 1 if r in (1, 2) else 0          # 1–2 = negative
    u += 1 if r == 3 else 0               # 3 = neutral
    c += 1
    stats[key] = [s, p, n, u, c]

# Emit one line per movie_id: key \t sum pos neg neu count
for key, (s, p, n, u, c) in stats.items():
    print(f"{key}\t{s}\t{p}\t{n}\t{u}\t{c}", flush=True)
