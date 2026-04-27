# Mexora ETL — Transformation Report

## Source files and intentional data quality issues:

**orders_mexora.csv** contains 51,500 rows, 3% duplicates, mixed date formats, dirty city names, non-standard statuses, negative quantities.

**clients_mexora.csv** contains 2,980 rows, duplicate emails, 5 gender encodings, invalid birth dates, malformed emails.

**products_mexora.json** contains 50 Nested JSON, inconsistent capitalisation, null prices, inactive products.

**regions_maroc.csv** contains 25 rows and is a clean official geographic reference.

---

## 1. Orders Transformations (`clean_commandes.py`)

### R1 — Duplicate Order Removal

**Business Rule:**
The source system allowed duplicate order IDs due to a synchronisation bug between the mobile app and the backend. Keeping duplicates would inflate revenue figures and order counts. We keep the last occurrence as it represents the most recent state of the order.

**Applied Code:**
```python
# Business rule: duplicate order IDs inflate revenue and order count metrics.
# The last occurrence is kept as it reflects the most recent order state.
df = df.drop_duplicates(subset=['id_commande'], keep='last')
```

**Result:**
1,500 duplicate rows removed (2.9%) — confirmed in pipeline log:

```
2026-04-23 14:31:05  INFO  transform.clean_commandes  [TRANSFORM] commandes : début avec 51,500 lignes
2026-04-23 14:31:05  INFO  transform.clean_commandes  [TRANSFORM] R1 doublons : 1,500 lignes supprimées (2.9%)
```

---

## R2 to R7

| Rule | Description | Business Reason | Lines Affected |
|---|---|---|---|
| R2 | Date standardisation | 3 mixed formats from different platforms | 0 invalid — all parsed |
| R3 | City harmonisation | Dirty variants (tanger, TNG, Tnja) → standard name via regions_maroc.csv | 0 unmapped |
| R4 | Status normalisation | 11 variants → 4 canonical (livré, annulé, en_cours, retourné) | All 49,567 rows |
| R5 | Invalid quantities | quantity ≤ 0 are data entry errors with no business value | −240 rows |
| R6 | Test orders | unit_price = 0 are internal test orders, not real sales | −193 rows |
| R7 | Missing drivers | Empty driver IDs filled with '-1' placeholder for dim_livreur | 3,554 filled |

### Code Applied

**R2 — Date Standardisation**
```python
# Business rule: orders originated from 3 platforms each using a different date format. All dates must be unified to YYYY-MM-DD for time-based analysis.
df['date_commande'] = pd.to_datetime(
    df['date_commande'], format='mixed',
    dayfirst=False, errors='coerce'
)
```

**R3 — City Harmonisation**
```python
# Business rule: city names entered manually across platforms contain abbreviations and spelling variants that prevent accurate regional analysis.
# Standardised using the official regions_maroc.csv reference file.
# Example: "tanger", "TNG", "TANGER", "Tnja" → "Tanger"
df['ville_livraison'] = df['ville_livraison'].map(city_map).fillna('Non renseignée')
```

**R4 — Status Normalisation**
```python
# Business rule: 11 status variants from different systems represent only 4 business states. Normalisation enables consistent return rate and delivery performance analysis.
STATUS_MAP = {
    'DONE': 'livré', 'livre': 'livré', 'LIVRE': 'livré',
    'KO': 'annulé', 'annule': 'annulé',
    'OK': 'en_cours',
}
df['statut'] = df['statut'].replace(STATUS_MAP)
```

**R5 — Invalid Quantities**
```python
# Business rule: negative or zero quantities are data entry errors with no valid business interpretation and must be excluded.
df = df[df['quantite'].astype(float) > 0]
```

**R6 — Test Orders**
```python
# Business rule: orders with unit price = 0 are internal test orders placed by the engineering team and must not distort revenue figures.
df = df[df['prix_unitaire'].astype(float) > 0]
```

**R7 — Missing Delivery Drivers**
```python
# Business rule: missing driver IDs prevent join to dim_livreur.
# Replaced with '-1' placeholder which maps to the 'Livreur Inconnu' record in dim_livreur, preserving referential integrity.
df['id_livreur'] = df['id_livreur'].replace('', '-1')
```

### Pipeline Log Output
```
2026-04-22 21:48:58  INFO  [TRANSFORM] R2 dates (date_commande) : 0 dates invalides → NaT
2026-04-22 21:49:02  INFO  [TRANSFORM] R2 dates (date_livraison) : 0 dates invalides → NaT
2026-04-22 21:49:02  INFO  [TRANSFORM] R2 dates supprimées : 0 lignes (date_commande invalide)
2026-04-22 21:49:02  INFO  [TRANSFORM] R3 villes        : 0 villes non trouvées → 'Non renseignée'
2026-04-22 21:49:02  INFO  [TRANSFORM] R5 quantités     : 240 lignes supprimées (quantité ≤ 0)
2026-04-22 21:49:02  INFO  [TRANSFORM] R6 prix          : 193 commandes test supprimées (prix = 0)
2026-04-22 21:49:02  INFO  [TRANSFORM] R7 livreurs      : 3554 valeurs manquantes → '-1'
2026-04-22 21:49:02  INFO  [TRANSFORM] commandes : TERMINÉ  51,500 → 49,567 lignes (1,933 supprimées, 3.8%)
```

---

## 2. Clients Transformations (`clean_clients.py`)

### R1 to R5 Summary

| Rule | Description | Impact | Technical Detail |
|---|---|---|---|
| R1 | Email deduplication | −112 duplicates removed | Sort by inscription date, keep='last' |
| R2 | Gender standardisation | 5 variants → 3 values | m/1/Male/homme/M → m/f/inconnu |
| R3 | Age validation | 40 invalidated | Age < 16 or > 100 years → NaT |
| R4 | Email validation | 38 nullified | Regex `^[a-zA-Z0-9.%+\-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$` |
| R5 | Customer segmentation | All 2,868 clients segmented | 12-month delivered revenue: Gold ≥15K / Silver ≥5K / Bronze <5K |

### Code Applied

**R1 — Email Deduplication**
```python
# Business rule: duplicate emails indicate the same customer registered multiple times. Keeping the most recent registration preserves the latest contact information and avoids double-counting clients.
df = df.sort_values('date_inscription')
df = df.drop_duplicates(subset=['email'], keep='last')
```

**R2 — Gender Standardisation**
```python
# Business rule: gender was encoded differently across registration platforms.
# Standardised to m/f/inconnu for consistent demographic analysis.
GENDER_MAP = {
    'm': 'm', 'M': 'm', 'Male': 'm', 'homme': 'm', '1': 'm',
    'f': 'f', 'F': 'f', 'Female': 'f', 'femme': 'f', '0': 'f',
}
df['sexe'] = df['sexe'].map(GENDER_MAP).fillna('inconnu')
```

**R3 — Age Validation**
```python
# Business rule: birth dates producing ages below 16 or above 100 are biologically impossible and indicate data entry errors.
# Invalidated to NaT rather than removed to preserve the client record.
df['date_naissance'] = pd.to_datetime(df['date_naissance'], errors='coerce')
age = (pd.Timestamp.today() - df['date_naissance']).dt.days / 365.25
df.loc[(age < 16) | (age > 100), 'date_naissance'] = pd.NaT
```

**R4 — Email Validation**
```python
# Business rule: malformed emails cannot be used for marketing campaigns.
# Set to NULL rather than removing the client — the client is still valid.
import re
EMAIL_REGEX = r'^[a-zA-Z0-9.%+\-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$'
invalid_mask = ~df['email'].str.match(EMAIL_REGEX, na=False)
df.loc[invalid_mask, 'email'] = None
```

**R5 — Customer Segmentation**
```python
# Business rule: Mexora segments clients by 12-month delivered revenue to enable targeted retention strategies per segment.
# Gold ≥ 15,000 MAD | Silver ≥ 5,000 MAD | Bronze < 5,000 MAD
def segmenter(ca):
    if ca >= 15_000: return 'Gold'
    if ca >= 5_000:  return 'Silver'
    return 'Bronze'

ca_par_client['segment_client'] = ca_par_client['ca_12m'].apply(segmenter)
```

### Pipeline Log Output
```
2026-04-22 21:49:02  INFO  [TRANSFORM] R1 doublons email  : 112 doublons supprimés
2026-04-22 21:49:02  INFO  [TRANSFORM] R2 sexe            : 0 valeurs non reconnues → 'inconnu'
2026-04-22 21:49:02  INFO  [TRANSFORM] R3 date_naissance  : 40 âges invalides → NaT
2026-04-22 21:49:02  INFO  [TRANSFORM] R4 emails          : 38 emails invalides → NULL
2026-04-22 21:49:02  INFO  [TRANSFORM] clients   : TERMINÉ  2,980 → 2,868 lignes (112 supprimées)
2026-04-22 21:49:02  INFO  [TRANSFORM] R5 segmentation    : {'Gold': 1961, 'Bronze': 493, 'Silver': 414}
```

---

## 3. Products Transformations (`clean_produits.py`)

### R1 to R3 Summary

| Rule | Description | Impact | Technical Detail |
|---|---|---|---|
| R1 | Category capitalisation normalisation | 9 variants → 3 canonical values | "electronique", "ELECTRONIQUE" → "Electronique" |
| R2 | Inactive product flagging | 2 products flagged | Preserved for SCD Type 2 history, est_actif = FALSE |
| R3 | Null catalogue price imputation | 2 prices filled | Sub-category median, fallback to category median |

### Code Applied

**R1 — Category Normalisation**
```python
# Business rule: category names were entered manually with inconsistent capitalisation across catalogue management tools. Normalised to 3 canonical values for accurate product category analysis.
CATEGORY_MAP = {
    'electronique': 'Electronique', 'ELECTRONIQUE': 'Electronique',
    'électronique': 'Electronique', 'mode': 'Mode', 'MODE': 'Mode',
    'alimentation': 'Alimentation', 'ALIMENTATION': 'Alimentation',
}
df['categorie'] = df['categorie'].map(CATEGORY_MAP).fillna(df['categorie'])
```

**R2 — Inactive Product Flagging**
```python
#Business rule: inactive products may still appear in historical orders.Preserved with est_actif = FALSE for SCD Type 2 historical tracking rather than deleted, ensuring past sales remain correctly attributed.
df['est_actif'] = df['actif'].astype(bool)
```

**R3 — Null Price Imputation**
```python
#Business rule: products without a catalogue price cannot be used for revenue calculations. Filled with sub-category median as the closest market approximation. Fallback to category median if needed.
df['prix_catalogue'] = df.groupby('sous_categorie')['prix_catalogue']\
    .transform(lambda x: x.fillna(x.median()))
df['prix_catalogue'] = df.groupby('categorie')['prix_catalogue']\
    .transform(lambda x: x.fillna(x.median()))
```

### Pipeline Log Output
```
2026-04-22 21:49:02  INFO  [TRANSFORM] R1 normalisation (categorie) : 9 variantes → 3 valeurs uniques
2026-04-22 21:49:02  INFO  [TRANSFORM] R2 produits inactifs : 2 produits inactifs conservés pour historique SCD Type 2
2026-04-22 21:49:02  INFO  [TRANSFORM] R3 prix nuls : 2 prix null → médiane sous-catégorie/catégorie
2026-04-22 21:49:02  INFO  [TRANSFORM] produits  : TERMINÉ  50 lignes (aucune suppression)
```

---

## 4. Full Summary

| Source | Input Rows | Output Rows | Removed | Rate |
|---|---|---|---|---|
| orders_mexora.csv | 51,500 | 49,567 | 1,933 | 3.8% |
| clients_mexora.csv | 2,980 | 2,868 | 112 | 3.8% |
| products_mexora.json | 50 | 50 | 0 | 0% |
| regions_maroc.csv | 25 | 25 | 0 | 0% |

**Total rows loaded into PostgreSQL: 49,567**