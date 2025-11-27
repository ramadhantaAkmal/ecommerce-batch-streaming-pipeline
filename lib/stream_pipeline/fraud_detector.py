import json
import psycopg2
from google.cloud import pubsub_v1
from datetime import datetime
from psycopg2.extras import Json
from const.config import DB_CONFIG, PROJECT_ID, SUBSCRIPTION_ID
from utils.db_utils import connect_to_db,load_to_db

subscriber = pubsub_v1.SubscriberClient()
subscription_path = subscriber.subscription_path(PROJECT_ID, SUBSCRIPTION_ID)

def is_fraud(order):
    reasons = []
    country = order["country"]
    qty = order["quantity"]
    amount = order["amount_numeric"]
    payment = order["payment"]
    device = order["device"]

    # Rule 1: Transaksi dari luar Indonesia
    if country != "ID":
        reasons.append("outside_indonesia")

    # Rule 2: Kartu luar negeri tapi transaksi di Indonesia
    if payment["method"] in ["credit_card", "debit_card"] and payment["card_country"] != "ID" and country == "ID":
        reasons.append("foreign_card_in_id")

    # Rule 3: Quantity besar (>100) dan amount lebih dari 100jt
    if qty > 5 and amount > 100_000_000:
        reasons.append("high_qty_on_exp_products")

    # Rule 4: Pakai VPN / Proxy
    if device["is_vpn"] or device["is_proxy"]:
        reasons.append("vpn_or_proxy")

    return len(reasons) > 0, reasons

def callback(message):
    try:
        conn = connect_to_db(DB_CONFIG)
        data = message.data.decode("utf-8")
        order = json.loads(data)
        order_id = order["order_id"]

        fraud_flag, reasons = is_fraud(order)
        status = "frauds" if fraud_flag else "genuine"

        order["status"] = status
        order["fraud_reasons"] = reasons if fraud_flag else ""

        color = "\033[91m" if fraud_flag else "\033[92m"
        reason_str = ", ".join(reasons) if reasons else "genuine"
        print(f"{color}[{status.upper():7}] {order_id} | {reason_str}\033[0m")
        
        load_to_db(
        """
            INSERT INTO orders (order_id, user_id, product_id, quantity, amount, country_code, payment_method, status, fraud_reasons)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
        """,(order_id,
             order["user_id"],
             order["product_id"],
             order["quantity"],
             order["amount"], 
             order["country"], 
             order["payment"]["method"],
             status,
             reasons),conn
        )
        
        message.ack()
        conn.close()
    except Exception as e:
        print("error:", e)
        message.nack()

print("Detecting Fraud...")
subscriber.subscribe(subscription_path, callback=callback).result()