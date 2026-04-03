# 04_extract.py
# Roles : filtrer le perimetre Elysee + selectionner les colonnes utiles
# AUCUNE transformation ici — tout est delegue a 05_transform.py
# pip install pandas

import os
import sys
import pandas as pd

if sys.stdout.encoding != "utf-8":
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")

# ── Configuration ────────────────────────────────────────────
INPUT_PATH      = "data/raw/tabular/listings.csv"
OUTPUT_PATH     = "data/processed/filtered_elysee.csv"
FILTRE_QUARTIER = "Elysee"
COL_QUARTIER    = "neighbourhood_cleansed"

COLS_TO_KEEP = [
    "id",
    "neighbourhood_cleansed",
    "latitude",
    "longitude",
    # Hypothese A - concentration economique
    "calculated_host_listings_count",
    "price",
    "property_type",
    "room_type",
    "availability_365",
    # Hypothese B - deshumanisation de l accueil
    "host_response_time",
    "host_response_rate",
    "host_is_superhost",
    "host_identity_verified",
    # Contexte
    "accommodates",
    "bedrooms",
    "beds",
    "minimum_nights",
    "instant_bookable",
    # Avis
    "number_of_reviews",
    "review_scores_rating",
    "review_scores_cleanliness",
    "review_scores_communication",
    "review_scores_location",
    "reviews_per_month",
]

# ─────────────────────────────────────────────────────────────


def main():
    os.makedirs(os.path.dirname(OUTPUT_PATH), exist_ok=True)

    # Chargement brut — aucune conversion
    df = pd.read_csv(INPUT_PATH, low_memory=False)
    print(f"Charge : {len(df):,} lignes, {len(df.columns)} colonnes")

    # Filtre geographique (robuste aux accents)
    col_norm = df[COL_QUARTIER].str.strip().str.lower() \
                               .str.normalize("NFD") \
                               .str.encode("ascii", errors="ignore") \
                               .str.decode("ascii")
    filtre_norm = FILTRE_QUARTIER.lower()
    df = df[col_norm == filtre_norm].copy()
    print(f"Filtre '{FILTRE_QUARTIER}' -> {len(df):,} annonces")

    # Selection des colonnes
    cols_presentes = [c for c in COLS_TO_KEEP if c in df.columns]
    cols_absentes  = [c for c in COLS_TO_KEEP if c not in df.columns]
    if cols_absentes:
        print(f"/!\\ Colonnes absentes (ignorees) : {cols_absentes}")

    df = df[cols_presentes]

    # Sauvegarde brute — valeurs originales intactes
    df.to_csv(OUTPUT_PATH, index=False, encoding="utf-8")
    print(f"Exporte -> {OUTPUT_PATH}")
    print(f"Shape : {df.shape[0]:,} lignes x {df.shape[1]} colonnes")


if __name__ == "__main__":
    main()