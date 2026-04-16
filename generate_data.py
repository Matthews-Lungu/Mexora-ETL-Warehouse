"""
Mexora Data Generator
=====================
Generates all 4 raw data files with intentional quality issues
as described in the project brief.

Design assumptions documented inline.
"""

import pandas as pd
import numpy as np
import json
import random
from datetime import date, datetime, timedelta

random.seed(42)
np.random.seed(42)

# ─────────────────────────────────────────────
# FILE 4 — regions_maroc.csv  (clean reference)
# Generated first because other files reference it
# ─────────────────────────────────────────────

REGIONS_DATA = [
    # code_ville, nom_ville_standard, province, region_admin, zone_geo, population, code_postal
    ("TNG", "Tanger",       "Tanger-Assilah",         "Tanger-Tétouan-Al Hoceïma", "Nord",     1065058, 90000),
    ("TET", "Tétouan",      "Tétouan",                "Tanger-Tétouan-Al Hoceïma", "Nord",      380787, 93000),
    ("CAS", "Casablanca",   "Casablanca",             "Casablanca-Settat",         "Centre",   3752000, 20000),
    ("RBA", "Rabat",        "Rabat",                  "Rabat-Salé-Kénitra",        "Centre",    577827, 10000),
    ("FES", "Fès",          "Fès",                    "Fès-Meknès",                "Centre-Nord",1112072,30000),
    ("MKN", "Meknès",       "Meknès",                 "Fès-Meknès",                "Centre-Nord", 632079,50000),
    ("MRK", "Marrakech",    "Marrakech",              "Marrakech-Safi",            "Centre-Sud", 928850,40000),
    ("AGA", "Agadir",       "Agadir-Ida Ou Tanane",   "Souss-Massa",               "Sud",       421844,80000),
    ("OUJ", "Oujda",        "Oujda-Angad",            "Oriental",                  "Est",       494252,60000),
    ("KEN", "Kénitra",      "Kénitra",                "Rabat-Salé-Kénitra",        "Centre",    431282,14000),
    ("SAL", "Salé",         "Salé",                   "Rabat-Salé-Kénitra",        "Centre",    890403,11000),
    ("TEM", "Témara",       "Skhirate-Témara",        "Rabat-Salé-Kénitra",        "Centre",    313510,12000),
    ("NAD", "Nador",        "Nador",                  "Oriental",                  "Est",       161726,62000),
    ("BEN", "Béni Mellal",  "Béni Mellal",            "Béni Mellal-Khénifra",      "Centre",    192676,23000),
    ("ELJ", "El Jadida",    "El Jadida",              "Casablanca-Settat",         "Centre",    194934,24000),
    ("TAZ", "Taza",         "Taza",                   "Fès-Meknès",                "Centre-Nord", 148406,35000),
    ("SFI", "Safi",         "Safi",                   "Marrakech-Safi",            "Centre-Sud",308508,46000),
    ("KHO", "Khouribga",    "Khouribga",              "Béni Mellal-Khénifra",      "Centre",    196196,25000),
    ("SET", "Settat",       "Settat",                 "Casablanca-Settat",         "Centre",    142250,26000),
    ("LAR", "Larache",      "Larache",                "Tanger-Tétouan-Al Hoceïma", "Nord",      125008,92000),
    ("HOC", "Al Hoceïma",   "Al Hoceïma",             "Tanger-Tétouan-Al Hoceïma", "Nord",       56716,32000),
    ("CHF", "Chefchaouen",  "Chefchaouen",            "Tanger-Tétouan-Al Hoceïma", "Nord",       42786,91000),
    ("ESS", "Essaouira",    "Essaouira",              "Marrakech-Safi",            "Centre-Sud",  77966,44000),
    ("DKH", "Dakhla",       "Oued Ed-Dahab",          "Dakhla-Oued Ed-Dahab",      "Sud",        106277,73000),
    ("LAY", "Laâyoune",     "Laâyoune",               "Laâyoune-Sakia El Hamra",   "Sud",        217732,70000),
]

df_regions = pd.DataFrame(REGIONS_DATA, columns=[
    "code_ville","nom_ville_standard","province","region_admin","zone_geo","population","code_postal"
])
df_regions.to_csv("/home/claude/mexora_etl/data/regions_maroc.csv", index=False, encoding="utf-8")
print(f"[OK] regions_maroc.csv — {len(df_regions)} rows")

# ─────────────────────────────────────────────
# HELPER — dirty city variants (used in orders & clients)
# ─────────────────────────────────────────────

CITY_DIRTY_MAP = {
    "Tanger":      ["tanger", "TNG", "TANGER", "Tnja", "Tanger", "tanger "],
    "Casablanca":  ["casablanca", "CASA", "Casablanca", "casa", "CAS"],
    "Rabat":       ["rabat", "RABAT", "Rabat", "rbat"],
    "Fès":         ["fes", "FES", "Fès", "Fez"],
    "Marrakech":   ["marrakech", "MARRAKECH", "Marrakesh", "mrk"],
    "Agadir":      ["agadir", "AGADIR", "Agadir"],
    "Meknès":      ["meknes", "MEKNES", "Meknès"],
    "Kénitra":     ["kenitra", "KENITRA", "Kénitra"],
    "Oujda":       ["oujda", "OUJDA", "Oujda"],
    "Salé":        ["sale", "SALE", "Salé"],
    "Tétouan":     ["tetouan", "TETOUAN", "Tétouan"],
    "Nador":       ["nador", "NADOR", "Nador"],
    "Béni Mellal": ["beni mellal", "BENI MELLAL", "Beni Mellal"],
    "El Jadida":   ["el jadida", "EL JADIDA", "El Jadida"],
    "Safi":        ["safi", "SAFI", "Safi"],
    "Larache":     ["larache", "LARACHE", "Larache"],
}

CITY_POOL = list(CITY_DIRTY_MAP.keys())

def dirty_city(city):
    """Return a random dirty variant of a city name."""
    variants = CITY_DIRTY_MAP.get(city, [city])
    return random.choice(variants)

# ─────────────────────────────────────────────
# FILE 2 — products_mexora.json
# ─────────────────────────────────────────────

PRODUCTS_RAW = [
    # Electronics — Smartphones
    ("P001","iPhone 16 Pro 256Go",      "Electronique","Smartphones","Apple",    "Apple MENA",       12999.00,"USA",     "2024-09-20",True),
    ("P002","Samsung Galaxy S24 Ultra", "Electronique","Smartphones","Samsung",  "Samsung Maghreb",  9999.00, "Corée",   "2024-01-15",True),
    ("P003","Xiaomi 14 Pro",            "electronique","Smartphones","Xiaomi",   "Xiaomi MENA",      7499.00, "Chine",   "2024-03-10",True),
    ("P004","iPhone 14",                "ELECTRONIQUE","Smartphones","Apple",    "Apple MENA",       8499.00, "USA",     "2022-09-15",True),
    ("P005","Oppo Reno 11",             "Electronique","Smartphones","Oppo",     "Oppo MENA",        4999.00, "Chine",   "2024-01-05",True),
    # Electronics — Laptops
    ("P006","MacBook Air M3",           "Electronique","Ordinateurs","Apple",    "Apple MENA",       14999.00,"USA",     "2024-03-01",True),
    ("P007","Dell XPS 15",              "electronique","Ordinateurs","Dell",     "Dell Afrique",     11999.00,"USA",     "2023-06-01",True),
    ("P008","HP Pavilion 15",           "Electronique","Ordinateurs","HP",       "HP Maroc",         6499.00, "USA",     "2023-01-10",True),
    ("P009","Lenovo ThinkPad E14",      "Electronique","Ordinateurs","Lenovo",   "Lenovo Maghreb",   7999.00, "Chine",   "2023-09-20",True),
    ("P010","Asus VivoBook 15",         "ELECTRONIQUE","Ordinateurs","Asus",     "Asus MENA",        5499.00, "Taiwan",  "2023-04-15",True),
    # Electronics — TVs
    ("P011","Samsung QLED 55\"",        "Electronique","Télévisions","Samsung",  "Samsung Maghreb",  8999.00, "Corée",   "2023-11-01",True),
    ("P012","LG OLED 65\"",             "Electronique","Télévisions","LG",       "LG Maroc",         15999.00,"Corée",   "2024-01-20",True),
    ("P013","Hisense 50\" 4K",          "electronique","Télévisions","Hisense",  "Hisense MENA",     3999.00, "Chine",   "2023-07-01",True),
    # Electronics — Audio
    ("P014","AirPods Pro 2",            "Electronique","Audio","Apple",          "Apple MENA",       2499.00, "USA",     "2022-09-23",True),
    ("P015","Sony WH-1000XM5",          "Electronique","Audio","Sony",           "Sony Maroc",       3499.00, "Japon",   "2022-05-12",True),
    ("P016","JBL Charge 5",             "Electronique","Audio","JBL",            "JBL MENA",         1299.00, "USA",     "2021-06-01",False),  # inactive
    # Fashion — Men
    ("P017","Djellaba Homme Premium",   "Mode",        "Homme","Artisan Maroc",  "Textile Nord",     599.00,  "Maroc",   "2022-01-15",True),
    ("P018","Chemise Oxford Slim",      "mode",        "Homme","Zara",           "Zara Maroc",       349.00,  "Espagne", "2023-02-01",True),
    ("P019","Jean Slim H&M",            "MODE",        "Homme","H&M",            "H&M Maroc",        299.00,  "Suède",   "2023-03-10",True),
    ("P020","Sneakers Nike Air Max",    "Mode",        "Homme","Nike",           "Nike MENA",        1299.00, "USA",     "2024-02-20",True),
    ("P021","Costume Mariage Maroc",    "Mode",        "Homme","Artisan Fès",    "Textile Fès",      2499.00, "Maroc",   "2022-06-01",True),
    ("P022","Burnous Laine Naturelle",  "mode",        "Homme","Artisan Maroc",  "Textile Sud",      899.00,  "Maroc",   "2021-11-01",False),  # inactive
    # Fashion — Women
    ("P023","Caftan Mariage Brodé",     "Mode",        "Femme","Maison Zineb",   "Couture Rabat",    3500.00, "Maroc",   "2022-03-01",True),
    ("P024","Abaya Premium",            "MODE",        "Femme","Al Baraka",      "Textile Casablanca",799.00,"Émirats", "2023-01-15",True),
    ("P025","Robe Été Zara",            "mode",        "Femme","Zara",           "Zara Maroc",       449.00,  "Espagne", "2024-04-01",True),
    ("P026","Hijab Soie Premium",       "Mode",        "Femme","Artisan Maroc",  "Textile Nord",     199.00,  "Maroc",   "2022-07-01",True),
    ("P027","Sneakers New Balance 574",  "Mode",       "Femme","New Balance",    "NB MENA",          1099.00, "USA",     "2023-08-01",True),
    # Fashion — Kids
    ("P028","Pyjama Enfant Cotton",     "Mode",        "Enfants","Petit Bateau", "PB Maroc",         299.00,  "France",  "2023-05-01",True),
    ("P029","Chaussures École",         "Mode",        "Enfants","Clarks",       "Clarks Maroc",     499.00,  "UK",      "2023-08-15",True),
    # Food — Staples
    ("P030","Huile d'Olive Vierge 5L",  "Alimentation","Huiles","Volubilis",     "Agro Nord",        289.00,  "Maroc",   "2022-01-01",True),
    ("P031","Couscous Dur 5kg",         "alimentation","Céréales","Dari",        "Agro Centre",      85.00,   "Maroc",   "2022-01-01",True),
    ("P032","Miel Naturel Thym 1kg",    "ALIMENTATION","Épicerie Fine","Abeilles du Rif","Agro Nord", None,   "Maroc",   "2021-06-01",True),  # null price
    ("P033","Sardines Conserve 24pc",   "Alimentation","Conserves","Doha",       "Agro Agadir",      120.00,  "Maroc",   "2022-03-01",True),
    ("P034","Safran Pur Taliouine 10g", "Alimentation","Épices","Coop Taliouine","Agro Sud",         149.00,  "Maroc",   "2022-01-15",True),
    ("P035","Argan Alimentaire 250ml",  "Alimentation","Huiles","Tifawin",       "Agro Sud",         199.00,  "Maroc",   "2022-04-01",True),
    # Food — Ramadan Specials
    ("P036","Chebakia Ramadan 1kg",     "Alimentation","Pâtisserie","Halwiyat",  "Artisan Fès",      129.00,  "Maroc",   "2022-03-01",True),
    ("P037","Sellou Tradition 500g",    "alimentation","Pâtisserie","Artisan",   "Artisan Marrakech",89.00,   "Maroc",   "2022-03-10",True),
    ("P038","Harira Concentrée 6pc",    "Alimentation","Conserves","Mama",       "Agro Centre",      65.00,   "Maroc",   "2022-03-01",True),
    ("P039","Bougie Aromatique Ramadan","Alimentation","Décoration","Artisan",   "Artisan Nord",     None,    "Maroc",   "2022-03-15",True),  # null price
    ("P040","Dattes Medjool Premium",   "Alimentation","Fruits Secs","Deglet",   "Agro Sud",         189.00,  "Maroc",   "2022-01-01",True),
    # Food — Beverages
    ("P041","Thé Vert Gunpowder 500g",  "ALIMENTATION","Boissons","Atay",        "Agro Casablanca",  79.00,   "Maroc",   "2022-02-01",True),
    ("P042","Jus Orange Frais 1L",      "Alimentation","Boissons","Sidi Ali",    "Agro Centre",      25.00,   "Maroc",   "2022-03-01",True),
    # More electronics
    ("P043","Tablette Samsung Galaxy A9","Electronique","Tablettes","Samsung",   "Samsung Maghreb",  4999.00, "Corée",   "2023-10-01",True),
    ("P044","iPad 10ème génération",    "electronique","Tablettes","Apple",      "Apple MENA",       6499.00, "USA",     "2022-10-18",True),
    ("P045","Clé USB 128Go Samsung",    "Electronique","Accessoires","Samsung",  "Samsung Maghreb",  199.00,  "Corée",   "2022-06-01",True),
    ("P046","Câble USB-C Braided 2m",   "Electronique","Accessoires","Anker",    "Anker MENA",       149.00,  "USA",     "2023-01-01",True),
    ("P047","Chargeur Rapide 65W",      "Electronique","Accessoires","Baseus",   "Baseus Maroc",     249.00,  "Chine",   "2023-06-01",True),
    ("P048","Disque SSD Externe 1To",   "Electronique","Stockage","Samsung",     "Samsung Maghreb",  899.00,  "Corée",   "2023-02-01",True),
    # More fashion
    ("P049","Babouches Cuir Fès",       "Mode",        "Chaussures","Artisan Fès","Artisan Fès",     349.00,  "Maroc",   "2022-01-01",True),
    ("P050","Sac Maroquinerie Maroc",   "mode",        "Sacs","Artisan Marrakech","Artisan Marrakech",599.00, "Maroc",   "2022-01-01",True),
]

products_json = {
    "produits": [
        {
            "id_produit":    p[0],
            "nom":           p[1],
            "categorie":     p[2],
            "sous_categorie":p[3],
            "marque":        p[4],
            "fournisseur":   p[5],
            "prix_catalogue":p[6],
            "origine_pays":  p[7],
            "date_creation": p[8],
            "actif":         p[9],
        }
        for p in PRODUCTS_RAW
    ]
}

with open("/home/claude/mexora_etl/data/products_mexora.json", "w", encoding="utf-8") as f:
    json.dump(products_json, f, ensure_ascii=False, indent=2)

print(f"[OK] products_mexora.json — {len(PRODUCTS_RAW)} products")

# ─────────────────────────────────────────────
# FILE 3 — clients_mexora.csv
# ─────────────────────────────────────────────

FIRST_NAMES_M = ["Mohamed","Ahmed","Youssef","Hamid","Khalid","Omar","Hassan",
                 "Rachid","Samir","Amine","Tariq","Adil","Soufiane","Mehdi","Ilyas"]
FIRST_NAMES_F = ["Fatima","Khadija","Aicha","Zineb","Meryem","Nadia","Sara",
                 "Houda","Samira","Loubna","Hajar","Amina","Safae","Imane","Dounia"]
LAST_NAMES    = ["El Idrissi","Benali","Ouazzani","Chraibi","Tazi","Bensouda",
                 "Lahlou","Berrada","Filali","Alami","Guessous","Bennani",
                 "Kettani","Skali","Rhouni","Mourchid","Hajji","Bacha"]

ACQUISITION_CHANNELS = ["instagram","facebook","google_ads","bouche_a_oreille",
                         "email_marketing","influenceur","application_mobile","partenaire"]

def random_date(start_year, end_year):
    start = date(start_year, 1, 1)
    end   = date(end_year, 12, 31)
    return start + timedelta(days=random.randint(0, (end - start).days))

def random_email(prenom, nom):
    base = f"{prenom.lower().replace(' ','')}.{nom.lower().replace(' ','').replace('el ','')}"
    domains = ["gmail.com","yahoo.fr","hotmail.com","outlook.com","menara.ma","iam.net.ma"]
    return f"{base}{random.randint(1,99)}@{random.choice(domains)}"

clients = []
client_id = 1

# 2,800 clean clients
for i in range(2800):
    gender = random.choice(["m","f"])
    prenom = random.choice(FIRST_NAMES_M if gender == "m" else FIRST_NAMES_F)
    nom    = random.choice(LAST_NAMES)
    city   = random.choice(CITY_POOL[:12])  # concentrate in big cities
    dob    = random_date(1965, 2005)
    reg_date = random_date(2020, 2024)
    email  = random_email(prenom, nom)

    # Gender encoding: introduce variety (issue R2 in transform)
    gender_raw = random.choice([
        gender,                              # clean: m/f
        "1" if gender=="m" else "0",         # numeric
        "Male" if gender=="m" else "Female", # english
        "homme" if gender=="m" else "femme", # french
        gender.upper(),                      # M/F
    ])

    clients.append({
        "id_client":        f"C{client_id:05d}",
        "nom":              nom,
        "prenom":           prenom,
        "email":            email,
        "date_naissance":   dob.strftime("%Y-%m-%d"),
        "sexe":             gender_raw,
        "ville":            dirty_city(city),
        "telephone":        f"06{random.randint(10000000,99999999)}",
        "date_inscription": reg_date.strftime("%Y-%m-%d"),
        "canal_acquisition":random.choice(ACQUISITION_CHANNELS),
    })
    client_id += 1

# ── Intentional issue 1: duplicate emails (different id_client — migration error)
for _ in range(80):
    original = random.choice(clients[:500])
    city2    = random.choice(CITY_POOL[:12])
    clients.append({
        "id_client":        f"C{client_id:05d}",
        "nom":              original["nom"],
        "prenom":           original["prenom"],
        "email":            original["email"],          # SAME email → duplicate
        "date_naissance":   original["date_naissance"],
        "sexe":             original["sexe"],
        "ville":            dirty_city(city2),
        "telephone":        f"06{random.randint(10000000,99999999)}",
        "date_inscription": (
            datetime.strptime(original["date_inscription"],"%Y-%m-%d")
            + timedelta(days=random.randint(30,200))
        ).strftime("%Y-%m-%d"),
        "canal_acquisition": random.choice(ACQUISITION_CHANNELS),
    })
    client_id += 1

# ── Intentional issue 2: impossible birth dates (age < 0 or > 120)
for _ in range(40):
    gender = random.choice(["m","f"])
    prenom = random.choice(FIRST_NAMES_M if gender=="m" else FIRST_NAMES_F)
    nom    = random.choice(LAST_NAMES)
    # Future date (negative age) or ancient (> 120 yrs)
    bad_dob = random.choice([
        date(2026, random.randint(1,12), random.randint(1,28)),  # future
        date(1880, random.randint(1,12), random.randint(1,28)),  # > 120 yrs
    ])
    reg_date = random_date(2021, 2024)
    clients.append({
        "id_client":        f"C{client_id:05d}",
        "nom":              nom,
        "prenom":           prenom,
        "email":            random_email(prenom, nom),
        "date_naissance":   bad_dob.strftime("%Y-%m-%d"),
        "sexe":             gender,
        "ville":            dirty_city(random.choice(CITY_POOL[:12])),
        "telephone":        f"06{random.randint(10000000,99999999)}",
        "date_inscription": reg_date.strftime("%Y-%m-%d"),
        "canal_acquisition":random.choice(ACQUISITION_CHANNELS),
    })
    client_id += 1

# ── Intentional issue 3: malformed emails (no @, no domain)
for _ in range(60):
    gender = random.choice(["m","f"])
    prenom = random.choice(FIRST_NAMES_M if gender=="m" else FIRST_NAMES_F)
    nom    = random.choice(LAST_NAMES)
    bad_email = random.choice([
        f"{prenom}{nom}gmail.com",    # missing @
        f"{prenom}@",                 # no domain
        "noemail",                    # completely invalid
        f"{prenom}.{nom}@.com",       # no domain name
    ])
    reg_date = random_date(2021, 2024)
    clients.append({
        "id_client":        f"C{client_id:05d}",
        "nom":              nom,
        "prenom":           prenom,
        "email":            bad_email,
        "date_naissance":   random_date(1970,2000).strftime("%Y-%m-%d"),
        "sexe":             gender,
        "ville":            dirty_city(random.choice(CITY_POOL[:12])),
        "telephone":        f"06{random.randint(10000000,99999999)}",
        "date_inscription": reg_date.strftime("%Y-%m-%d"),
        "canal_acquisition":random.choice(ACQUISITION_CHANNELS),
    })
    client_id += 1

random.shuffle(clients)
df_clients = pd.DataFrame(clients)
df_clients.to_csv("/home/claude/mexora_etl/data/clients_mexora.csv", index=False, encoding="utf-8")
print(f"[OK] clients_mexora.csv — {len(df_clients)} rows")

# ─────────────────────────────────────────────
# FILE 1 — orders_mexora.csv  (50,000 rows)
# ─────────────────────────────────────────────

CLIENT_IDS  = [c["id_client"] for c in clients if not c["email"].endswith("@") ]
PRODUCT_IDS = [p[0] for p in PRODUCTS_RAW]
PRODUCT_PRICES = {p[0]: p[6] for p in PRODUCTS_RAW}

# Product category map for Ramadan boost logic
PRODUCT_CATS = {p[0]: p[2].lower() for p in PRODUCTS_RAW}
FOOD_PRODUCTS = [p[0] for p in PRODUCTS_RAW if "aliment" in p[2].lower()]
OTHER_PRODUCTS = [p[0] for p in PRODUCTS_RAW if "aliment" not in p[2].lower()]

# Ramadan periods (for demand boost)
RAMADAN = [
    (date(2022,4,2),  date(2022,5,1)),
    (date(2023,3,22), date(2023,4,20)),
    (date(2024,3,10), date(2024,4,9)),
]

def is_ramadan(d):
    for start, end in RAMADAN:
        if start <= d <= end:
            return True
    return False

STATUSES_CLEAN   = ["livré","annulé","en_cours","retourné"]
STATUSES_WEIGHTS = [0.72,   0.10,    0.12,       0.06]
STATUS_DIRTY_MAP = {
    "livré":    ["livré","livre","LIVRE","DONE","livré"],
    "annulé":   ["annulé","annule","KO","annulé"],
    "en_cours": ["en_cours","OK","en_cours"],
    "retourné": ["retourné","retourne","retourné"],
}

PAYMENT_MODES = ["carte_bancaire","cash_on_delivery","virement","wallet_mobile"]
DELIVERY_PERSONS = [f"L{i:03d}" for i in range(1,16)]

def random_date_fmt(d):
    """Return date in one of 3 mixed formats (issue R2)."""
    fmt = random.choice(["iso","us","text"])
    if fmt == "iso":   return d.strftime("%Y-%m-%d")
    if fmt == "us":    return d.strftime("%m/%d/%Y")
    if fmt == "text":  return d.strftime("%b %d %Y")

order_id_counter = 1
orders = []

# Base date range: 2022-01-01 to 2024-12-31
date_start = date(2022, 1, 1)
date_end   = date(2024, 12, 31)
total_days = (date_end - date_start).days

for i in range(50000):
    order_day = date_start + timedelta(days=random.randint(0, total_days))

    # Ramadan boost for food products
    if is_ramadan(order_day) and random.random() < 0.45:
        prod_id = random.choice(FOOD_PRODUCTS)
    else:
        prod_id = random.choice(PRODUCT_IDS)

    client_id_val = random.choice(CLIENT_IDS)
    catalogue_price = PRODUCT_PRICES.get(prod_id)

    # Base unit price with ± 5% variation; handle null catalogue prices
    if catalogue_price is None:
        unit_price = round(random.uniform(50, 300), 2)
    else:
        unit_price = round(catalogue_price * random.uniform(0.95, 1.05), 2)

    qty = random.randint(1, 5)
    status_clean = random.choices(STATUSES_CLEAN, STATUSES_WEIGHTS)[0]
    status_raw   = random.choice(STATUS_DIRTY_MAP[status_clean])

    # Delivery date: 1–7 days after order
    delivery_date = order_day + timedelta(days=random.randint(1, 7))

    # Delivery city = client's city (with dirty variant)
    city = random.choice(CITY_POOL[:12])
    delivery_city = dirty_city(city)

    # Delivery person: 7% missing
    if random.random() < 0.07:
        livreur = ""
    else:
        livreur = random.choice(DELIVERY_PERSONS)

    orders.append({
        "id_commande":    f"CMD{order_id_counter:07d}",
        "id_client":      client_id_val,
        "id_produit":     prod_id,
        "date_commande":  random_date_fmt(order_day),
        "quantite":       qty,
        "prix_unitaire":  unit_price,
        "statut":         status_raw,
        "ville_livraison":delivery_city,
        "mode_paiement":  random.choice(PAYMENT_MODES),
        "id_livreur":     livreur,
        "date_livraison": random_date_fmt(delivery_date),
    })
    order_id_counter += 1

# ── Intentional issue 1: ~3% duplicates on id_commande
n_dups = int(50000 * 0.03)
dup_indices = random.sample(range(50000), n_dups)
dup_orders  = []
for idx in dup_indices:
    dup = orders[idx].copy()
    # Slightly alter quantity or price to simulate re-submitted order
    dup["quantite"]      = random.randint(1,3)
    dup["prix_unitaire"] = round(dup["prix_unitaire"] * random.uniform(0.98,1.02), 2)
    dup_orders.append(dup)

orders.extend(dup_orders)

# ── Intentional issue 2: negative quantities (~0.5%)
for _ in range(250):
    idx = random.randint(0, len(orders)-1)
    orders[idx]["quantite"] = random.randint(-5, -1)

# ── Intentional issue 3: zero unit price / test orders (~0.5%)
for _ in range(200):
    idx = random.randint(0, len(orders)-1)
    orders[idx]["prix_unitaire"] = 0.00

# Shuffle to mix duplicates and dirty data
random.shuffle(orders)

df_orders = pd.DataFrame(orders)
df_orders.to_csv("/home/claude/mexora_etl/data/orders_mexora.csv", index=False, encoding="utf-8")

print(f"[OK] orders_mexora.csv — {len(df_orders)} rows (includes duplicates)")
print()
print("=" * 50)
print("ALL 4 DATA FILES GENERATED SUCCESSFULLY")
print("=" * 50)
print(f"  regions_maroc.csv  : {len(df_regions)} rows (clean)")
print(f"  products_mexora.json: {len(PRODUCTS_RAW)} products")
print(f"  clients_mexora.csv : {len(df_clients)} rows")
print(f"  orders_mexora.csv  : {len(df_orders)} rows")
