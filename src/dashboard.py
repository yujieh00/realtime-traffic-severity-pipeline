"""
Real-time dashboard: Kafka consumer + live matplotlib plots.

Subscribes to the three result topics the Spark job publishes and redraws
three panels every second:

    1. High-severity accidents (7-10) over time  - line chart
    2. Cumulative accidents by predicted severity - bar chart
    3. Accident locations coloured by severity    - bubble map

Run this last, in its own terminal, while the producer and the streaming job
are both running:
    python src/dashboard.py               # interactive window
    python src/dashboard.py --headless    # save PNG snapshots (for Docker/CI)

In --headless mode there is no GUI: the figure is re-rendered to
output/dashboard.png every refresh, so it works inside a container.
"""

import os
import sys
from collections import defaultdict
from json import loads

import matplotlib

# Pick a backend before importing pyplot: Agg (no display) when headless.
HEADLESS = "--headless" in sys.argv
if HEADLESS:
    matplotlib.use("Agg")

import matplotlib.pyplot as plt  # noqa: E402
from kafka import KafkaConsumer  # noqa: E402

from config import (  # noqa: E402
    KAFKA_BOOTSTRAP,
    OUTPUT_ROOT,
    TOPIC_DISTRICT_COUNTS,
    TOPIC_PREDICTIONS_MAP,
    TOPIC_SEVERITY_COUNTS,
)

DASHBOARD_PNG = os.path.join(OUTPUT_ROOT, "dashboard.png")

# Rolling state built up from the incoming messages.
high_by_window = defaultdict(int)     # window_end -> high-severity count
severity_totals = defaultdict(int)    # severity 1-10 -> cumulative count
map_points = []                       # list of (lon, lat, severity)


def short_time(ts):
    s = str(ts).replace("T", " ")
    return s.split(" ", 1)[1][:8] if " " in s else s[:8]


def clamp_severity(value):
    try:
        return max(1, min(10, int(round(float(value)))))
    except (TypeError, ValueError):
        return 1


def connect_consumer():
    return KafkaConsumer(
        TOPIC_SEVERITY_COUNTS, TOPIC_DISTRICT_COUNTS, TOPIC_PREDICTIONS_MAP,
        bootstrap_servers=[KAFKA_BOOTSTRAP],
        auto_offset_reset="latest",
        value_deserializer=lambda m: loads(m.decode("utf-8")),
        consumer_timeout_ms=1000,
    )


def draw(fig, axes):
    ax1, ax2, ax3 = axes
    for ax in axes:
        ax.clear()

    # 1) High-severity accidents over time.
    windows = sorted(high_by_window)[-20:]
    ax1.plot([short_time(w) for w in windows],
             [high_by_window[w] for w in windows],
             marker="o", color="crimson")
    ax1.set_title("High-severity accidents (7-10)\nper 30s window")
    ax1.set_xlabel("Window end")
    ax1.set_ylabel("Count")
    ax1.tick_params(axis="x", rotation=45)

    # 2) Cumulative accidents by severity.
    sevs = list(range(1, 11))
    ax2.bar(sevs, [severity_totals.get(s, 0) for s in sevs], color="steelblue")
    ax2.set_title("Cumulative accidents by\npredicted severity")
    ax2.set_xlabel("Predicted severity")
    ax2.set_ylabel("Total")
    ax2.set_xticks(sevs)

    # 3) Accident locations coloured by severity.
    if map_points:
        lons, lats, sev = zip(*map_points[-500:], strict=False)
        sc = ax3.scatter(lons, lats, c=sev, cmap="YlOrRd", vmin=1, vmax=10,
                         s=25, alpha=0.7, edgecolors="none")
        if not getattr(draw, "_cbar", None):
            draw._cbar = fig.colorbar(sc, ax=ax3, label="severity")
    ax3.set_title("Accident locations\n(colour = predicted severity)")
    ax3.set_xlabel("Longitude")
    ax3.set_ylabel("Latitude")

    fig.suptitle("Real-time Accident Severity Dashboard")
    fig.tight_layout()

    if HEADLESS:
        os.makedirs(OUTPUT_ROOT, exist_ok=True)
        fig.savefig(DASHBOARD_PNG, dpi=90)
    else:
        plt.pause(0.01)


def main():
    consumer = connect_consumer()
    if not HEADLESS:
        plt.ion()
    fig, axes = plt.subplots(1, 3, figsize=(17, 5))
    if HEADLESS:
        print(f"Headless mode: writing snapshots to {DASHBOARD_PNG}. Ctrl-C to stop.")
    else:
        print("Listening for messages... close the window or Ctrl-C to stop.")

    try:
        while True:
            for msg in consumer:
                d = msg.value
                if msg.topic == TOPIC_DISTRICT_COUNTS:
                    we = d.get("window_end")
                    if we is not None:
                        high_by_window[we] += int(d.get("high_7_10", 0) or 0)
                elif msg.topic == TOPIC_SEVERITY_COUNTS:
                    sev = clamp_severity(d.get("pred_severity"))
                    severity_totals[sev] += int(d.get("num_accidents", 0) or 0)
                elif msg.topic == TOPIC_PREDICTIONS_MAP:
                    lat, lon = d.get("latitude"), d.get("longitude")
                    if lat is not None and lon is not None:
                        map_points.append((lon, lat, clamp_severity(d.get("pred_severity"))))
            draw(fig, axes)
    except KeyboardInterrupt:
        print("\nStopped by user.")
    finally:
        consumer.close()
        if not HEADLESS:
            plt.ioff()


if __name__ == "__main__":
    main()
