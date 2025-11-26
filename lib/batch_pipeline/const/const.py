PRICE_RANGES = {
    # Watches
    "watch": {"min": 4_500_000, "max": 350_000_000, "common": (8_000_000, 85_000_000)},
    "chronograph": {"min": 6_500_000, "max": 450_000_000, "common": (12_000_000, 120_000_000)},
    "tourbillon": {"min": 85_000_000, "max": 2_500_000_000, "common": (150_000_000, 800_000_000)},
    # Fragrances
    "eau de parfum": {"min": 180_000, "max": 1_200_000, "common": (250_000, 680_000)},
    "parfum": {"min": 280_000, "max": 2_800_000, "common": (380_000, 980_000)},
    "extrait": {"min": 450_000, "max": 4_500_000, "common": (680_000, 1_800_000)},
    # Handbags & Leather Goods
    "handbag": {"min": 2_200_000, "max": 85_000_000, "common": (3_800_000, 28_000_000)},
    "birkin": {"min": 12_000_000, "max": 500_000_000, "common": (28_000_000, 120_000_000)},
    "kelly": {"min": 9_500_000, "max": 380_000_000, "common": (18_000_000, 85_000_000)},
    "tote": {"min": 1_800_000, "max": 42_000_000, "common": (2_900_000, 18_000_000)},
    # Jewelry
    "ring": {"min": 3_800_000, "max": 1_200_000_000, "common": (8_500_000, 125_000_000)},
    "necklace": {"min": 5_200_000, "max": 2_800_000_000, "common": (12_000_000, 280_000_000)},
    "earrings": {"min": 2_900_000, "max": 950_000_000, "common": (6_800_000, 98_000_000)},
    # Footwear
    "sneaker": {"min": 680_000, "max": 18_000_000, "common": (890_000, 4_200_000)},
    "loafer": {"min": 780_000, "max": 6_800_000, "common": (1_100_000, 3_200_000)},
    # Jackets & coats
    "jacket": {"min": 2_800_000, "max": 48_000_000, "common": (4_200_000, 18_000_000)},
    "coat": {"min": 3_900_000, "max": 92_000_000, "common": (6_500_000, 32_000_000)},
}

DB_CONFIG = {
    "host":"localhost",
    "port":5434,
    "name":"data_source",
    "user":"root",
    "password":"password"
}