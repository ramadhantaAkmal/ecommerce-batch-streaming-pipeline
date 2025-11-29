import json
import time
import random
import uuid

from datetime import datetime, timedelta
from faker import Faker
from google.cloud import pubsub_v1
from const.const import PAYMENT_METHODS, EWALLET_PROVIDERS, CARD_BRANDS
from const.config import DB_CONFIG, PROJECT_ID, TOPIC_ID
from utils.db_utils import connect_to_db,fetch_record

fake = Faker('id_ID')

publisher = pubsub_v1.PublisherClient()
topic_path = publisher.topic_path(PROJECT_ID, TOPIC_ID)

def generate_payment():
    method = random.choices(
        PAYMENT_METHODS,
        weights=[50, 20, 25, 5], k=1)[0]

    if method == "credit_card" or method == "debit_card":
        is_foreign_card = random.random() < 0.15  # 15% kartu luar
        brand = random.choice(CARD_BRANDS)
        card_country = "ID" if not is_foreign_card else random.choice(["US","SG","MY","GB","AU"])
        
        return {
            "method": method,
            "card_brand": brand,
            "card_country": card_country,
            "ewallet_provider": None,
            "bank_code": None,
            "billing_address": fake.address()
        }
    elif method == "ewallet":
        return {
            "method": "ewallet",
            "card_brand": None, 
            "card_country": None,
            "ewallet_provider": random.choice(EWALLET_PROVIDERS),
            "bank_code": None,
            "billing_address": None
        }
    else:  # virtual_account / transfer
        return {
            "method": method,
            "card_brand": None, 
            "card_country": None,
            "ewallet_provider": None,
            "bank_code": random.choice(["BCA", "BNI", "BRI", "Mandiri", "CIMB"]),
            "billing_address": None
        }

def generate_order(product: tuple, user: tuple):
    product_id, product_name, price = product
    user_id, created_at = user
    platform = random.choices(["Android","IOS","Browser"], weights=[50,20,30], k=1)
    quantity = random.choices([1,2,3,5,10,50,100,200], weights=[60,20,10,5,3,1,0.8,0.2], k=1)[0]
    amount = price * quantity
    country = random.choice(["ID","SG","MY","US","GB","RU","CN","BR","NG","AE"] + ["ID"]*15)  # 60% dari ID
    created_dt = created_at + (datetime.now() - created_at) * random.random()

    order = {
        "order_id": uuid.uuid4().hex[:5].upper(),
        "user_id": user_id,
        "product_id": product_id,
        "product_name": product_name,
        "quantity": quantity,
        "amount": f"Rp.{amount:,}".replace(",", "."),
        "amount_numeric": int(amount),
        "country": country,
        "created_date": created_dt.strftime("%Y-%m-%dT%H:%M:%S"),
        "payment": generate_payment(),
        "platform": platform[0],
        "ip_address": fake.ipv4(),
        "is_vpn": random.random() < 0.07,
        "is_proxy": random.random() < 0.04
    }
    return order

conn = connect_to_db(DB_CONFIG)
print("Generate Orders...")
try:
    i = 1
    while i<=3:
        product_data = fetch_record("SELECT product_id, product_name, price FROM products ORDER BY RANDOM() LIMIT 1", conn)
        user_data = fetch_record("SELECT user_id,created_at FROM users ORDER BY RANDOM() LIMIT 1", conn)
        order = generate_order(product_data, user_data)
        # data = json.dumps(order, ensure_ascii=False).encode("utf-8")
        # future = publisher.publish(topic_path, data)
        # future.result()
        # print(f"[SENT] {order['order_id']} | {order['amount']:>16} | {order['country']} | "
        #       f"{order['payment']['method']:12}")
        print(order)
        time.sleep(random.uniform(0.7, 2.8))
        i+=1
    conn.close()
except KeyboardInterrupt:
    conn.close()
    print("\nGenerator Stopped")



