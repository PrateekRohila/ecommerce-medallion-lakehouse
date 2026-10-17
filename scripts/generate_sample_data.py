"""Generate 3 days of realistic landing-zone files, including the messy cases a
real pipeline must survive:

  * duplicate records (producer retries)
  * malformed JSON, bad timestamps, negative quantities, unsupported currency
  * order status updates (PLACED -> SHIPPED -> DELIVERED / CANCELLED)
  * a STALE status update that arrives after a newer one (must be ignored)
  * a late-arriving order for an earlier day (old partition must be rebuilt)
  * customer attribute changes, a no-op change, and a late-arriving change
    that rewrites customer history (SCD2)

Usage:  python scripts/generate_sample_data.py --base-path ./data
"""
from __future__ import annotations

import argparse
import csv
import json
import random
import shutil
from datetime import datetime, timedelta
from pathlib import Path

random.seed(42)
D1, D2, D3 = "2026-10-14", "2026-10-15", "2026-10-16"
FX = {"INR": 1.0, "USD": 83.50, "EUR": 90.20}
COUNTRY_CCY = {"IN": "INR", "DE": "EUR", "US": "USD"}

PRODUCTS = [
    ("P001", "Noise-Cancelling Headphones", "electronics", "12999.00"),
    ("P002", "Mechanical Keyboard", "electronics", "6499.00"),
    ("P003", "4K Monitor 27in", "electronics", "28999.00"),
    ("P004", "Cotton Kurta", "apparel", "1499.00"),
    ("P005", "Running Shoes", "apparel", "4999.00"),
    ("P006", "Cast Iron Skillet", "home", "2199.00"),
    ("P007", "Air Purifier", "home", "9999.00"),
    ("P008", "Designing Data-Intensive Applications", "books", "3299.00"),
    ("P009", "Spark: The Definitive Guide", "books", "2899.00"),
    ("P010", "Vitamin C Serum", "beauty", "899.00"),
]

CUSTOMERS = {  # id: (name, email, city, country, tier)
    "C001": ("aarav sharma", "aarav.sharma@example.com", "delhi", "IN", "SILVER"),
    "C002": ("isha verma", "isha.verma@example.com", "bengaluru", "IN", "GOLD"),
    "C003": ("lukas becker", "lukas.becker@example.de", "munich", "DE", "BRONZE"),
    "C004": ("emma schmidt", "emma.schmidt@example.de", "berlin", "DE", "SILVER"),
    "C005": ("john miller", "john.miller@example.com", "austin", "US", "PLATINUM"),
    "C006": ("priya nair", "priya.nair@example.com", "kochi", "IN", "BRONZE"),
    "C007": ("rohan gupta", "rohan.gupta@example.com", "pune", "IN", "GOLD"),
    "C008": ("sarah lee", "sarah.lee@example.com", "seattle", "US", "SILVER"),
}


def ts(value: datetime) -> str:
    return value.strftime("%Y-%m-%dT%H:%M:%S")


def order(order_id, customer_id, order_ts, status="PLACED", updated=None, product=None, qty=None):
    pid, _, _, price = product or random.choice(PRODUCTS)
    ccy = COUNTRY_CCY[CUSTOMERS[customer_id][3]] if customer_id in CUSTOMERS else "INR"
    unit_price = round(float(price) / FX[ccy], 2)
    return {
        "order_id": order_id, "customer_id": customer_id, "product_id": pid,
        "quantity": qty if qty is not None else random.randint(1, 3),
        "unit_price": f"{unit_price:.2f}", "currency": ccy, "status": status,
        "order_ts": ts(order_ts), "updated_at": ts(updated or order_ts + timedelta(minutes=2)),
    }


def write_jsonl(path: Path, rows: list) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as fh:
        for row in rows:
            fh.write((row if isinstance(row, str) else json.dumps(row)) + "\n")


def write_csv(path: Path, header: list[str], rows: list[list]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8") as fh:
        writer = csv.writer(fh)
        writer.writerow(header)
        writer.writerows(rows)


def main(base: Path) -> None:
    landing = base / "landing"
    if base.exists():
        shutil.rmtree(base)
    cust_header = ["customer_id", "full_name", "email", "city", "country", "tier", "updated_at"]
    ids = list(CUSTOMERS)
    placed: dict[str, dict] = {}

    # ----------------------------------------------------------- Day 1
    write_csv(landing / "products" / f"ingest_date={D1}" / "products.csv",
              ["product_id", "product_name", "category", "list_price"], [list(p) for p in PRODUCTS])
    cust_rows = [[cid, *attrs, "2026-10-13T10:00:00"] for cid, attrs in CUSTOMERS.items()]
    cust_rows.append(["C009", "bad email", "not-an-email", "delhi", "IN", "GOLD", "2026-10-13T10:00:00"])
    write_csv(landing / "customers" / f"ingest_date={D1}" / "customers.csv", cust_header, cust_rows)

    day1 = [order("O0001", "C001", datetime(2026, 10, 14, 15, 0), product=PRODUCTS[0], qty=1)]
    for i in range(2, 31):
        o = order(f"O{i:04d}", random.choice(ids), datetime(2026, 10, 14, 8, 0) + timedelta(minutes=29 * i))
        day1.append(o)
    placed.update({o["order_id"]: o for o in day1})
    day1 += [day1[3], day1[7], day1[11]]                                   # producer retries
    day1.append('{"order_id": "O9001", "customer_id": "C002", "quantity": ')  # truncated JSON
    day1.append(order("O9002", "C004", datetime(2026, 10, 14, 11, 0), qty=-2))
    bad_ccy = order("O9003", "C002", datetime(2026, 10, 14, 12, 0))
    bad_ccy["currency"] = "GBP"
    day1.append(bad_ccy)
    write_jsonl(landing / "orders" / f"ingest_date={D1}" / "orders_part-0001.json", day1)

    # ----------------------------------------------------------- Day 2
    write_csv(landing / "customers" / f"ingest_date={D2}" / "customers.csv", cust_header, [
        ["C001", "aarav sharma", "aarav.sharma@example.com", "gurugram", "IN", "GOLD", "2026-10-15T09:00:00"],
        ["C003", "lukas becker", "lukas.becker@example.de", "hamburg", "DE", "BRONZE", "2026-10-15T09:30:00"],
        ["C005", *CUSTOMERS["C005"], "2026-10-15T10:00:00"],               # no-op change
    ])
    day2 = []
    for i in range(1, 16):
        o = dict(placed[f"O{i:04d}"])
        o["status"] = "CANCELLED" if i in (4, 9) else "SHIPPED"
        o["updated_at"] = ts(datetime(2026, 10, 15, 6, 0) + timedelta(minutes=i))
        day2.append(o)
        placed[o["order_id"]] = o
    for i in range(31, 51):
        o = order(f"O{i:04d}", random.choice(ids), datetime(2026, 10, 15, 8, 0) + timedelta(minutes=31 * (i - 30)))
        day2.append(o)
        placed[o["order_id"]] = o
    day2.append(order("O0051", "C007", datetime(2026, 10, 13, 22, 45)))   # late-arriving order
    no_cust = order("O9004", "C002", datetime(2026, 10, 15, 13, 0))
    no_cust["customer_id"] = None
    day2.append(no_cust)
    bad_ts = order("O9005", "C006", datetime(2026, 10, 15, 14, 0))
    bad_ts["order_ts"] = "15/10/2026 14:00"
    day2.append(bad_ts)
    write_jsonl(landing / "orders" / f"ingest_date={D2}" / "orders_part-0001.json", day2)

    # ----------------------------------------------------------- Day 3
    write_csv(landing / "customers" / f"ingest_date={D3}" / "customers.csv", cust_header, [
        # Late-arriving: the GOLD upgrade actually happened on 14 Oct at noon
        ["C001", "aarav sharma", "aarav.sharma@example.com", "delhi", "IN", "GOLD", "2026-10-14T12:00:00"],
        ["C008", "sarah lee", "sarah.lee@example.com", "seattle", "US", "GOLD", "2026-10-16T08:00:00"],
    ])
    day3 = []
    for i in range(1, 9):
        if i in (4,):
            continue
        o = dict(placed[f"O{i:04d}"])
        o["status"] = "DELIVERED"
        o["updated_at"] = ts(datetime(2026, 10, 16, 7, 0) + timedelta(minutes=i))
        day3.append(o)
    stale = dict(placed["O0010"])                                          # older than SHIPPED update
    stale["status"] = "PLACED"
    stale["updated_at"] = ts(datetime(2026, 10, 14, 23, 0))
    day3.append(stale)
    for i in range(52, 71):
        day3.append(order(f"O{i:04d}", random.choice(ids), datetime(2026, 10, 16, 8, 0) + timedelta(minutes=27 * (i - 51))))
    write_jsonl(landing / "orders" / f"ingest_date={D3}" / "orders_part-0001.json", day3)

    print(f"Sample landing data written to {landing.resolve()} for {D1}, {D2}, {D3}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--base-path", default="./data")
    main(Path(parser.parse_args().base_path))
