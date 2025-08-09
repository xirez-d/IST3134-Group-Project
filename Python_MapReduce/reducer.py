#!/usr/bin/env python3
import sys

current = None      # store as int
sum_r = 0
pos = neg = neu = cnt = 0

def emit(k, s, p, n, u, c):
    if k is None: return
    avg = (s / c) if c else 0.0
    print(f"{k},{avg:.2f},{p},{n},{u},{c}", flush=True)

for line in sys.stdin:
    line = line.strip()
    if not line:
        continue
    parts = line.split("\t")
    if len(parts) != 6:
        continue

    try:
        key_int = int(parts[0])   # <— drop zero padding
        s = int(parts[1]); p = int(parts[2]); n = int(parts[3]); u = int(parts[4]); c = int(parts[5])
    except ValueError:
        continue

    if key_int == current:
        sum_r += s; pos += p; neg += n; neu += u; cnt += c
    else:
        emit(current, sum_r, pos, neg, neu, cnt)
        current = key_int
        sum_r, pos, neg, neu, cnt = s, p, n, u, c

emit(current, sum_r, pos, neg, neu, cnt)
