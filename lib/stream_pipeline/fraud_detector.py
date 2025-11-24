from google.cloud import pubsub_v1
import json
from datetime import datetime
import psycopg2
from psycopg2.extras import Json

project_id = "jcdeah-006"
subscription_id = "akmal-fraud-detection-topic-sub"

subscriber = pubsub_v1.SubscriberClient()
subscription_path = subscriber.subscription_path(project_id, subscription_id)

# conn = psycopg2.connect(host="localhost", database="streaming_db", user="postgres", password="yourpassword")
# cur = conn.cursor()

# cur.execute("""CREATE TABLE IF NOT EXISTS orders_enhanced (
#     id SERIAL PRIMARY KEY,
#     order_id VARCHAR(50) UNIQUE,
#     data JSONB,
#     status VARCHAR(20),
#     fraud_reasons TEXT[],
#     detected_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
# );""")
# conn.commit()

def is_fraud(order):
    reasons = []
    country = order["country"]
    qty = order["quantity"]
    amount = order["amount_numeric"]
    dt = datetime.strptime(order["created_date"], "%Y-%m-%dT%H:%M:%S")
    hour = dt.hour
    payment = order["payment"]
    device = order["device"]

    # Rule 1: Transaksi dari luar Indonesia
    if country != "ID":
        reasons.append("outside_indonesia")

    # Rule 2: Kartu luar negeri tapi transaksi di Indonesia
    if payment["method"] in ["credit_card", "debit_card"] and payment["card_country"] != "ID" and country == "ID":
        reasons.append("foreign_card_in_id")

    # Rule 3: Quantity besar (>100) di jam 00-04
    if qty > 100 and 0 <= hour <= 4:
        reasons.append("high_qty_midnight")

    # Rule 4: Amount > 100 juta di jam 00-04
    if amount > 100_000_000 and 0 <= hour <= 4:
        reasons.append("large_amount_midnight")

    # Rule 5: Pakai VPN / Proxy
    if device["is_vpn"] or device["is_proxy"]:
        reasons.append("vpn_or_proxy")

    return len(reasons) > 0, reasons

def callback(message):
    try:
        data = message.data.decode("utf-8")
        order = json.loads(data)
        order_id = order["order_id"]

        fraud_flag, reasons = is_fraud(order)
        status = "frauds" if fraud_flag else "genuine"

        order["status"] = status
        order["fraud_reasons"] = reasons if fraud_flag else ""
        # cur.execute("""
        #     INSERT INTO orders_enhanced (order_id, data, status, fraud_reasons)
        #     VALUES (%s, %s, %s, %s)
        #     ON CONFLICT (order_id) DO NOTHING
        # """, (order_id, Json(order), status, reasons if fraud_flag else None))
        # conn.commit()

        color = "\033[91m" if fraud_flag else "\033[92m"
        reason_str = ", ".join(reasons) if reasons else "genuine"
        print(f"{color}[{status.upper():7}] {order_id} | {reason_str}\033[0m")

        message.ack()
    except Exception as e:
        print("error:", e)
        message.nack()

print("Detecting Fraud...")
subscriber.subscribe(subscription_path, callback=callback).result()