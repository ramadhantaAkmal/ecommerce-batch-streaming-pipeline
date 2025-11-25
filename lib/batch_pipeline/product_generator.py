from faker import Faker
from faker.providers import BaseProvider
import random

fake = Faker()

# Custom Provider for Premium Product Names
def premium_product_name():
        # Luxury adjectives
        adjectives = [
            "Elite", "Signature", "Imperial", "Royal", "Prestige", "Heritage",
            "Luxe", "Opulent", "Exquisite", "Refined", "Divine", "Celestial",
            "Eternal", "Velvet", "Obsidian", "Aurum", "Platinum", "Diamond",
            "Sovereign", "Regal", "Noble", "Grand", "Premier", "Infinite",
            "Apex", "Zenith", "Vantage", "Pinnacle", "Ascendant", "Elysian"
        ]

        # High-end materials & themes
        materials = [
            "Cashmere", "Silk", "Leather", "Marble", "Ebony", "Ivory",
            "Sapphire", "Emerald", "Onyx", "Titanium", "Carbon", "Gold",
            "Rosewood", "Mahogany", "Alcantara", "Crystal", "Pearl", "Quartz"
        ]

        # Product categories (luxury style)
        categories = [
            "Couture", "Reserve", "Collection",
            "Atelier", "Maison", "Heritage", "Legacy", "Edition", "Noir",
            "Blanche", "Voyage", "Essence", "Absolu", "Intense", "Prive",
            "Lumiere", "Infini", "Eclat", "Sublime", "Exceptionnelle", "Rare"
        ]

        # Luxury brand-inspired prefixes/suffixes
        brands_style = [
            "La Maison", "Atelier", "Cuir", "Joallier", "Horloger",
            "Cuvée", "Domaine", "Château", "Vintage", "Millésime", "Privée",
            "Exclusif", "Iconique", "Legend", "Mythique", "Édition Limitée"
        ]

        # Patterns for premium product names
        patterns = [
            lambda: f"{random.choice(adjectives)} {random.choice(categories)}",
            lambda: f"{random.choice(adjectives)} {random.choice(materials)} {random.choice(categories)}",
            lambda: f"{random.choice(brands_style)} {random.choice(adjectives)}",
            lambda: f"{random.choice(adjectives)} {random.choice(categories)} by {fake.last_name()}",
            lambda: f"{random.choice(materials)} {random.choice(categories)}",
            lambda: f"{random.choice(adjectives)} {random.choice(categories)} No. {random.randint(1, 99)}",
            lambda: f"{fake.company()} {random.choice(['Couture', 'Privée', 'Reserve', 'Legacy'])}",
            lambda: f"Édition {random.choice(adjectives)} – {random.choice(categories)}",
            lambda: f"{random.choice(adjectives)} {random.choice(materials)} Edition",
            lambda: f"{random.choice(brands_style)} {random.choice(['de', 'du', 'des'])} {fake.city()}",
        ]

        return random.choice(patterns)()

def premium_product_category():
    # Product categories
    luxury_categories = {
        "Watches": ["watch", "chronograph", "tourbillon"],
        "Fragrances": ["eau de parfum", "parfum", "extrait"],
        "Handbags & Leather Goods": ["handbag", "birkin", "kelly", "tote"],
        "Jewelry": ["ring", "necklace", "earrings"],
        "Footwear": ["sneaker", "loafer"],
        "Jackets & Coats": ["jacket", "coat"]
    }
    category_type = random.choice(list(luxury_categories.keys()))
    
    rand_index = random.randint(0, len(luxury_categories[category_type])-1)
    category_name = luxury_categories[category_type][rand_index]
    
    return category_type, category_name
    

PRICE_RANGES = {
        # Watches
        "watch":            {"min": 4_500,   "max": 350_000,  "common": (8_000, 85_000)},
        "chronograph":      {"min": 6_500,   "max": 450_000,  "common": (12_000, 120_000)},
        "tourbillon":       {"min": 85_000,  "max": 2_500_000,"common": (150_000, 800_000)},

        # Fragrances
        "eau de parfum":    {"min": 180,     "max": 1_200,    "common": (250, 680)},
        "parfum":           {"min": 280,     "max": 2_800,    "common": (380, 980)},
        "extrait":          {"min": 450,     "max": 4_500,    "common": (680, 1_800)},

        # Handbags & Leather Goods
        "handbag":          {"min": 2_200,   "max": 85_000,   "common": (3_800, 28_000)},
        "birkin":           {"min": 12_000,  "max": 500_000,  "common": (28_000, 120_000)},
        "kelly":            {"min": 9_500,   "max": 380_000,  "common": (18_000, 85_000)},
        "tote":             {"min": 1_800,   "max": 42_000,   "common": (2_900, 18_000)},

        # Jewelry
        "ring":             {"min": 3_800,   "max": 1_200_000,"common": (8_500, 125_000)},
        "necklace":         {"min": 5_200,   "max": 2_800_000,"common": (12_000, 280_000)},
        "earrings":         {"min": 2_900,   "max": 950_000,  "common": (6_800, 98_000)},

        # Footwear
        "sneaker":          {"min": 680,     "max": 18_000,   "common": (890, 4_200)},
        "loafer":           {"min": 780,     "max": 6_800,    "common": (1_100, 3_200)},
        
        # Jackets & coats
        "jacket":           {"min": 2_800,   "max": 48_000,   "common": (4_200, 18_000)},
        "coat":             {"min": 3_900,   "max": 92_000,   "common": (6_500, 32_000)},
}

    
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

        # Skew towards higher end using a power-law-like distribution
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


# ——— EXAMPLE USAGE ———
print("Luxury Product Names with Realistic Prices\n" + "="*50)
for _ in range(25):
    product_name = premium_product_name()
    category_type, category_name = premium_product_category()
    price = luxury_price(category_name)
    print(f"{product_name:<35} {category_type:<25} {category_name:<25} ${price:,.0f}")