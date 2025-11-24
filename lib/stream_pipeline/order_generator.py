from google.cloud import pubsub_v1
import json
import time
import random
from datetime import datetime, timedelta
from faker import Faker
import uuid

fake = Faker('id_ID')

project_id = "jcdeah-006"
topic_id = "akmal-fraud-detection-topic"

publisher = pubsub_v1.PublisherClient()
topic_path = publisher.topic_path(project_id, topic_id)

# Produk premium (harga tinggi = rawan fraud)
products = [
    {"id": "P1001", "name": "iPhone 16 Pro Max", "price": 23_999_000},
    {"id": "P1002", "name": "MacBook Pro M3 Max 64GB", "price": 72_999_000},
    {"id": "P1003", "name": "Samsung Z Fold 6", "price": 28_999_000},
    {"id": "P1004", "name": "PS5 Pro Bundle", "price": 15_999_000},
    {"id": "P1005", "name": "RTX 4090 Gaming Laptop", "price": 85_999_000},
    {"id": "P1006", "name": "Rolex Submariner", "price": 195_000_000},
]

payment_methods = ["credit_card", "debit_card", "ewallet", "virtual_account"]
ewallet_providers = ["GoPay", "OVO", "Dana", "ShopeePay"]
card_brands = ["Visa", "Mastercard", "American Express", "JCB"]

# BIN card number
foreign_bins = ["481111", "400000", "510510", "378282", "353011"]  # US, SG, MY, dll
id_bins = ["460000", "520000", "436000", "548888"]  # BCA, Mandiri, BNI, dll

def generate_payment():
    method = random.choices(
        payment_methods,
        weights=[50, 20, 25, 5], k=1)[0]

    if method == "credit_card" or method == "debit_card":
        is_foreign_card = random.random() < 0.15  # 15% kartu luar
        bin6 = random.choice(foreign_bins) if is_foreign_card else random.choice(id_bins)
        brand = random.choice(card_brands)
        card_country = "ID" if not is_foreign_card else random.choice(["US","SG","MY","GB","AU"])
        
        return {
            "method": method,
            "card_bin": bin6,
            "card_last4": f"{random.randint(0,9999):04d}",
            "card_brand": brand,
            "card_country": card_country,
            "ewallet_provider": None,
            "bank_code": None,
            "billing_address": {
                "city": fake.city(),
                "postal_code": fake.postcode(),
                "country": "ID"
            }
        }
    elif method == "ewallet":
        return {
            "method": "ewallet",
            "card_bin": None, "card_last4": None, "card_brand": None, "card_country": None,
            "ewallet_provider": random.choice(ewallet_providers),
            "bank_code": None,
            "billing_address": None
        }
    else:  # virtual_account / transfer
        return {
            "method": method,
            "card_bin": None, "card_last4": None, "card_brand": None, "card_country": None,
            "ewallet_provider": None,
            "bank_code": random.choice(["BCA", "BNI", "BRI", "Mandiri", "CIMB"]),
            "billing_address": None
        }

def generate_order():
    product = random.choice(products)
    quantity = random.choices([1,2,3,5,10,50,100,200], weights=[60,20,10,5,3,1,0.8,0.2], k=1)[0]
    amount = product["price"] * quantity
    country = random.choice(["ID","SG","MY","US","GB","RU","CN","BR","NG","AE"] + ["ID"]*15)  # 60% dari ID

    # Jam rawan fraud
    if random.random() < 0.04:
        hour = random.randint(0, 4)
    else:
        hour = random.randint(0, 23)

    created_dt = datetime.now() - timedelta(minutes=random.randint(0, 2880))
    created_dt = created_dt.replace(hour=hour, minute=random.randint(0,59), second=random.randint(0,59))

    order = {
        "order_id": f"ORD-{uuid.uuid4().hex[:10].upper()}",
        "user_id": f"U{random.randint(10000,999999)}",
        "product_id": product["id"],
        "product_name": product["name"],
        "quantity": quantity,
        "amount": f"Rp.{amount:,}".replace(",", "."),
        "amount_numeric": amount,
        "country": country,
        "created_date": created_dt.strftime("%Y-%m-%dT%H:%M:%S"),
        "payment": generate_payment(),
        "device": {
            "ip_address": fake.ipv4(),
            "user_agent": fake.user_agent(),
            "is_vpn": random.random() < 0.07,
            "is_proxy": random.random() < 0.04
        }
    }
    return order

print("Generate Orders...")
try:
    while True:
        order = generate_order()
        data = json.dumps(order, ensure_ascii=False).encode("utf-8")
        future = publisher.publish(topic_path, data)
        future.result()
        print(f"[SENT] {order['order_id']} | {order['amount']:>16} | {order['country']} | "
              f"{order['payment']['method']:12} | {order['created_date'][-8:]}")
        time.sleep(random.uniform(0.7, 2.8))
except KeyboardInterrupt:
    print("\nGenerator Stopped")