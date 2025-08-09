#!/usr/bin/env python3
import sys, csv
from collections import defaultdict

# stats[key] = [sum_ratings, pos, neg, neu, count]
stats = defaultdict(lambda: [0, 0, 0, 0, 0])

reader = csv.reader(sys.stdin)
for row in reader:
    if not row or len(row) < 3:
        continue
    # split-safe header skip: customer_id,movie_id,rating
    if row[0].lower() == "customer_id" or row[1].lower() == "movie_id" or row[2].lower() == "rating":
        continue
    try:
        mid_num = int(row[1].strip())     # numeric movie_id
        r = int(row[2])
    except Exception:
        continue

    key = f"{mid_num:05d}"                # <— zero-pad so text sort == numeric sort

    s, p, n, u, c = stats[key]
    s += r
    p += 1 if r in (4, 5) else 0
    n += 1 if r in (1, 2) else 0
    u += 1 if r == 3 else 0
    c += 1
    stats[key] = [s, p, n, u, c]

for key, (s, p, n, u, c) in stats.items():
    print(f"{key}\t{s}\t{p}\t{n}\t{u}\t{c}", flush=True)
