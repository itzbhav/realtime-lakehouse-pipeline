"""
bpm_producer.py - simulates a quick-commerce order fulfillment process
(Superstore / Blinkit-Instamart style dark-store dispatch) and publishes
one event per process stage to Kafka topic `order_fulfillment_events`.

Process (happy path):
  ORDER_RECEIVED -> PICKER_ASSIGNED -> [ITEM_SUBSTITUTED]* -> ITEMS_BAGGED
    -> RIDER_ASSIGNED -> DISPATCHED_FROM_DARKSTORE -> DOORSTEP_DELIVERED

Each case (order) is independently, and *deliberately*, given a chance to
carry one or more process anomalies so bpm_process_engine.py has real
things to detect:
  - SLA breach       : one stage takes far longer than its target
  - duplicate task    : the same activity for the same case_id sent twice
  - out-of-sequence   : two adjacent stage events swapped in send order
  - substitution      : an ITEM_SUBSTITUTED exception event is inserted
  - drop-off          : the case stops partway through and never delivers
"""

import argparse
import json
import random
import time
from datetime import datetime, timedelta, timezone

from kafka import KafkaProducer

STAGES = [
    "ORDER_RECEIVED",
    "PICKER_ASSIGNED",
    "ITEMS_BAGGED",
    "RIDER_ASSIGNED",
    "DISPATCHED_FROM_DARKSTORE",
    "DOORSTEP_DELIVERED",
]

STAGE_SLA_MINUTES = {
    ("ORDER_RECEIVED", "PICKER_ASSIGNED"): 1,
    ("PICKER_ASSIGNED", "ITEMS_BAGGED"): 3,
    ("ITEMS_BAGGED", "RIDER_ASSIGNED"): 2,
    ("RIDER_ASSIGNED", "DISPATCHED_FROM_DARKSTORE"): 2,
    ("DISPATCHED_FROM_DARKSTORE", "DOORSTEP_DELIVERED"): 7,
}
TARGET_TOTAL_MINUTES = 15
PICKING_SLA_MINUTES = 3

PICKERS = [f"Picker_Emp_{i}" for i in range(101, 121)]
RIDERS = [f"Rider_Emp_{i}" for i in range(201, 231)]
LOCATIONS = [
    "DarkStore_Coimbatore_North",
    "DarkStore_Bangalore_East",
    "DarkStore_Chennai_Central",
    "DarkStore_Hyderabad_West",
    "DarkStore_Pune_South",
]
ITEMS = [
    "Milk 1L", "Bread", "Eggs (6)", "Bananas", "Rice 5kg",
    "Detergent", "Butter 500g", "Toothpaste", "Tea Powder", "Biscuits",
]

ANOMALY_RATES = {
    "sla_breach": 0.15,
    "duplicate": 0.08,
    "out_of_order": 0.08,
    "substitution": 0.10,
    "dropoff": 0.05,
}


def fmt_ts(dt: datetime) -> str:
    return dt.strftime("%Y-%m-%dT%H:%M:%SZ")


def make_event(case_id, activity, ts, resource, location, metadata):
    return {
        "case_id": case_id,
        "activity": activity,
        "timestamp": fmt_ts(ts),
        "resource": resource,
        "location": location,
        "metadata": metadata,
    }


def simulate_case(case_id, base_ts):
    """Build the full list of (activity, ts, resource, location, metadata) events for one order."""
    location = random.choice(LOCATIONS)
    picker = random.choice(PICKERS)
    rider = random.choice(RIDERS)
    item_count = random.randint(1, 15)
    common_meta = {
        "item_count": item_count,
        "target_total_minutes": TARGET_TOTAL_MINUTES,
        "picking_sla_minutes": PICKING_SLA_MINUTES,
    }

    flags = {name: random.random() < rate for name, rate in ANOMALY_RATES.items()}
    breach_stage = random.choice(list(STAGE_SLA_MINUTES.keys())) if flags["sla_breach"] else None

    events = [("ORDER_RECEIVED", base_ts, "Customer_App", location, dict(common_meta))]
    sim_ts = base_ts
    prev_activity = "ORDER_RECEIVED"
    dropoff_at = random.choice(["ITEMS_BAGGED", "RIDER_ASSIGNED"]) if flags["dropoff"] else None

    for stage in STAGES[1:]:
        sla = STAGE_SLA_MINUTES[(prev_activity, stage)]
        gap = sla + random.uniform(-0.3, -0.02)  # clean cases finish comfortably inside the SLA
        if flags["sla_breach"] and (prev_activity, stage) == breach_stage:
            gap = sla + random.uniform(1.5, 4.0)
        sim_ts = sim_ts + timedelta(minutes=gap)

        if stage in ("PICKER_ASSIGNED", "ITEMS_BAGGED"):
            resource = picker
        elif stage in ("RIDER_ASSIGNED", "DISPATCHED_FROM_DARKSTORE"):
            resource = rider
        else:
            resource = "System"

        if flags["substitution"] and stage == "ITEMS_BAGGED":
            sub_ts = sim_ts - timedelta(minutes=random.uniform(0.3, 0.8))
            sub_meta = dict(common_meta)
            sub_meta["substituted_item"] = random.choice(ITEMS)
            sub_meta["reason"] = "out_of_stock"
            events.append(("ITEM_SUBSTITUTED", sub_ts, picker, location, sub_meta))

        events.append((stage, sim_ts, resource, location, dict(common_meta)))
        prev_activity = stage

        if dropoff_at == stage:
            break

    if flags["duplicate"] and len(events) > 2:
        dup_activity, dup_ts, dup_res, dup_loc, dup_meta = random.choice(events[1:])
        events.append((dup_activity, dup_ts + timedelta(seconds=random.uniform(5, 30)), dup_res, dup_loc, dup_meta))

    if flags["out_of_order"] and len(events) > 3:
        # swap only the *timestamps* of two adjacent main-stage events (never touching
        # ITEM_SUBSTITUTED), so the later stage's activity ends up timestamped before the
        # earlier one -- a genuine process-order violation, not just a send-order quirk
        # that gets silently undone once the engine sorts events by event_ts.
        candidates = [
            idx for idx in range(1, len(events) - 1)
            if events[idx][0] != "ITEM_SUBSTITUTED" and events[idx + 1][0] != "ITEM_SUBSTITUTED"
        ]
        if candidates:
            i = random.choice(candidates)
            a_act, a_ts, a_res, a_loc, a_meta = events[i]
            b_act, b_ts, b_res, b_loc, b_meta = events[i + 1]
            events[i] = (a_act, b_ts, a_res, a_loc, a_meta)
            events[i + 1] = (b_act, a_ts, b_res, b_loc, b_meta)

    return events, flags


def main():
    parser = argparse.ArgumentParser(description="Quick-commerce order fulfillment BPM event simulator")
    parser.add_argument("--bootstrap", default="localhost:9092", help="Kafka bootstrap servers")
    parser.add_argument("--topic", default="order_fulfillment_events")
    parser.add_argument("--cases", type=int, default=30, help="number of orders to simulate")
    parser.add_argument("--case-interval", type=float, default=1.0, help="real seconds between starting new cases")
    parser.add_argument("--event-delay", type=float, default=0.2, help="real seconds between events within one case")
    args = parser.parse_args()

    producer = KafkaProducer(
        bootstrap_servers=args.bootstrap,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
    )

    print(f"Simulating {args.cases} orders -> topic '{args.topic}'. Ctrl+C to stop.")
    anomaly_counts = {name: 0 for name in ANOMALY_RATES}

    try:
        for n in range(1, args.cases + 1):
            case_id = f"ORD-SUPER-{n:06d}"
            base_ts = datetime.now(timezone.utc)
            events, flags = simulate_case(case_id, base_ts)

            for name, hit in flags.items():
                if hit:
                    anomaly_counts[name] += 1
            tags = [name.upper() for name, hit in flags.items() if hit]
            tag_str = f"[{','.join(tags)}]" if tags else "[clean]"
            print(f"{tag_str} {case_id}: {len(events)} events, last={events[-1][0]}")

            for activity, ts, resource, location, meta in events:
                producer.send(args.topic, value=make_event(case_id, activity, ts, resource, location, meta))
                time.sleep(args.event_delay)

            time.sleep(max(0.0, args.case_interval - len(events) * args.event_delay))
    except KeyboardInterrupt:
        print("\nStopping producer.")
    finally:
        producer.flush()
        producer.close()
        print(f"Done. Anomaly counts out of {args.cases} cases: {anomaly_counts}")


if __name__ == "__main__":
    main()
