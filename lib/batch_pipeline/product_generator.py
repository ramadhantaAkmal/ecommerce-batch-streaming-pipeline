from faker import Faker
from faker.providers import BaseProvider
from lib.batch_pipeline.utils.db_utils import connect_to_db,load_to_db
from lib.batch_pipeline.const.exceptions import DBConnectionError
from lib.batch_pipeline.const.const import PRICE_RANGES,ADJECTIVES,BRANDS_STYLE,CATEGORIES,MATERIALS,LUXURY_CATEGORIES
from lib.batch_pipeline.const.config import DB_CONFIG
import random

fake = Faker()

# Custom Provider for Premium Product Names
def premium_product_name():
        """
            Generate a realistic product names.
            The name patterns are generated randomly
        """
        # Patterns for premium product names
        patterns = [
            lambda: f"{random.choice(ADJECTIVES)} {random.choice(CATEGORIES)}",
            lambda: f"{random.choice(ADJECTIVES)} {random.choice(MATERIALS)} {random.choice(CATEGORIES)}",
            lambda: f"{random.choice(BRANDS_STYLE)} {random.choice(ADJECTIVES)}",
            lambda: f"{random.choice(ADJECTIVES)} {random.choice(CATEGORIES)} by {fake.last_name()}",
            lambda: f"{random.choice(MATERIALS)} {random.choice(CATEGORIES)}",
            lambda: f"{random.choice(ADJECTIVES)} {random.choice(CATEGORIES)} No. {random.randint(1, 99)}",
            lambda: f"{fake.company()} {random.choice(['Couture', 'Privée', 'Reserve', 'Legacy'])}",
            lambda: f"Édition {random.choice(ADJECTIVES)} – {random.choice(CATEGORIES)}",
            lambda: f"{random.choice(ADJECTIVES)} {random.choice(MATERIALS)} Edition",
            lambda: f"{random.choice(BRANDS_STYLE)} {random.choice(['de', 'du', 'des'])} {fake.city()}",
        ]

        return random.choice(patterns)()

def premium_product_category():
    """
        Generate a realistic luxury categories.
        Randomly pick category types and category name
    """
    #Randomly pick category types
    category_type = random.choice(list(LUXURY_CATEGORIES.keys()))
    
    #Randomly pick category names
    rand_index = random.randint(0, len(LUXURY_CATEGORIES[category_type])-1)
    category_name = LUXURY_CATEGORIES[category_type][rand_index]
    
    return category_type, category_name    
    
def luxury_price(category):
        """
        Generate a realistic luxury price.
        If category_hint contains a keyword from PRICE_RANGES, it uses that range.
        Otherwise picks a random luxury category.
        """
        for key in PRICE_RANGES:
            if key in category:
                category = key
                break

        price_data = PRICE_RANGES[category]

        # 80% chance to be in the "common" realistic range, 20% chance extreme high-end
        if random.random() < 0.80 and "common" in price_data:
            min_p, max_p = price_data["common"]
        else:
            min_p = price_data["min"]
            max_p = price_data["max"]

        # Generate a random price using a triangular distribution.
        # The mode is set to 75% of the max value, making the result skew toward higher prices.
        price = int(random.triangular(min_p, max_p, max_p * 0.75))
        
        # Round to realistic luxury pricing patterns
        if price >= 100_000:
            price = round(price, -4)   # nearest 10,000
        elif price >= 10_000:
            price = round(price, -3)   # nearest 1,000
        elif price >= 1_000:
            price = round(price / 50) * 50   # nearest 50
        else:
            price = round(price / 10) * 10

        return price

def generate_product(ts, **kwargs):
    """
        The main function for generate product data
    """
    conn = connect_to_db(DB_CONFIG)
    if conn is None:
        raise DBConnectionError("Postgress Connection Failed")
    else:
        print("Generate Product...")
        product_name = premium_product_name()
        category_type, category_name = premium_product_category()
        price = luxury_price(category_name)
        load_to_db(
            """
                INSERT INTO products (product_name, category, price, created_at)
                VALUES (%s, %s, %s, %s)
            """,(product_name, category_type, price, ts),conn
        )
        print("Successfully generated product and load into db...")
    conn.close()