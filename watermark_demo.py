"""Replays the 10-reading example from the Dataflow Model paper (Akidau et al., VLDB 2015, Figure 5)
through two ways of handling late data, so the trade-off is visible on a slide.

  1. Drop late data (what Spark Structured Streaming does, and what our SmokeStream pipeline does):
     a window is final once the watermark passes its end, and anything that arrives after that is dropped.
     Like Spark 3.4+, late rows in micro-batch k are judged against the watermark of batch k-1
     (the rule our simulator in smokestream/common/simulate.py reproduces).
  2. Re-fire on late data (the paper's default: Repeat(AtWatermark()) in accumulating mode):
     a window fires when the watermark passes, and fires again with a corrected value when a late reading shows up.

Event and processing times are read off Figure 5 by eye, so they are approximate, but every window sum
matches the paper (14, 22, 3, 12; total 51).

Run: python3 watermark_demo.py
"""

WINDOW = 2.0   # fixed 2-minute windows, as in the paper's Figures 10-13
BATCH = 1.0    # 1-minute micro-batches in processing time

# (value, event time, processing time), minutes after 12:00
READINGS = [
    (5, 0.50, 5.70),
    (7, 2.50, 6.30),
    (3, 3.60, 6.60),
    (4, 3.75, 6.75),
    (3, 4.40, 6.80),
    (8, 3.20, 7.20),
    (3, 6.80, 7.50),
    (9, 1.40, 8.30),   # the late reading in the paper
    (8, 7.60, 8.80),
    (1, 7.80, 9.00),
]


def clock(minutes):
    return f"12:{int(minutes):02d}"


def window_of(event_time):
    start = int(event_time // WINDOW) * WINDOW
    return (start, start + WINDOW)


def batches():
    """Group readings into 1-minute micro-batches by processing time."""
    out = {}
    for value, ev, proc in READINGS:
        out.setdefault(int(proc // BATCH), []).append((value, ev))
    return [(k, out[k]) for k in sorted(out)]


def drop_late(allowed_delay):
    """Spark-style: watermark = max event time seen - allowed delay; late rows are dropped."""
    sums, emitted, dropped = {}, {}, []
    max_event, prev_wm = float("-inf"), float("-inf")
    for k, rows in batches():
        for value, ev in rows:
            w = window_of(ev)
            if w[1] <= prev_wm:            # window already closed by the previous batch's watermark
                dropped.append(value)
                continue
            sums[w] = sums.get(w, 0) + value
            max_event = max(max_event, ev)
        wm = max_event - allowed_delay
        batch_end = (k + 1) * BATCH
        for w in sorted(sums):
            if w not in emitted and wm >= w[1]:
                emitted[w] = (batch_end, sums[w])
        prev_wm = wm
    return sums, emitted, dropped


def refire_on_late():
    """Dataflow-style: fire when the watermark passes (no extra delay), then re-fire for each late reading."""
    sums, panes = {}, []
    max_event, wm = float("-inf"), float("-inf")
    for k, rows in batches():
        batch_end = (k + 1) * BATCH
        touched = set()
        for value, ev in rows:
            w = window_of(ev)
            sums[w] = sums.get(w, 0) + value
            max_event = max(max_event, ev)
            touched.add(w)
        new_wm = max_event
        for w in sorted(sums):
            on_time = wm < w[1] <= new_wm
            late = w[1] <= wm and w in touched
            if on_time or late:
                panes.append((batch_end, w, sums[w], "late update" if late else "on time"))
        wm = new_wm
    return panes


def main():
    total = sum(v for v, _, _ in READINGS)
    print(f"Paper's example: {len(READINGS)} readings, true total = {total}\n")

    print("1) Drop late data (Spark / SmokeStream)")
    print(f"{'delay':>6} | {'counted':>7} | {'dropped':>7} | first window final at | windows still open")
    for d in [0, 1, 2, 5]:
        sums, emitted, dropped = drop_late(d)
        counted = sum(sums.values())
        first = min(emitted.values())[0] if emitted else None
        still_open = sum(1 for w in sums if w not in emitted)
        print(f"{d:>4} m | {counted:>4}/{total} | {str(dropped):>7} | "
              f"{clock(first) if first else '-':>21} | {still_open}")

    print("\n2) Re-fire on late data (Dataflow, accumulating)")
    panes = refire_on_late()
    for t, w, s, kind in panes:
        print(f"  {clock(t)}  window [{clock(w[0])}, {clock(w[1])})  sum = {s:>2}  ({kind})")
    fired = {w for _, w, _, _ in panes}
    for w in sorted({window_of(ev) for _, ev, _ in READINGS} - fired):
        print(f"  still open: window [{clock(w[0])}, {clock(w[1])}) - the watermark has not passed its end yet")
    print("\nDropping is cheap and final but loses data unless you wait longer;")
    print("re-firing is correct and early but every consumer must handle updated answers.")


if __name__ == "__main__":
    main()
