# 04_extract.py
# pip install pandas
#
# Role : selectionner uniquement les colonnes utiles aux 3 hypotheses
# et exporter le sous-ensemble Elysee dans data/processed/

import os
import sys
import pandas as pd

import sys
sys.stdout.reconfigure(encoding="utf-8", errors="replace")
# ...votre code...
# Force UTF-8 sur Windows (evite les UnicodeEncodeError en cp1252)
if sys.stdout.encoding != "utf-8":
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")

# ── Configuration ────────────────────────────────────────────
INPUT_PATH      = "data/raw/tabular/listings.csv"
OUTPUT_PATH     = "data/processed/filtered_elysee.csv"
FILTRE_QUARTIER = "Élysée"
COL_QUARTIER    = "neighbourhood_cleansed"

# ── Colonnes à conserver (par hypothèse) ────────────────────
COLS_TO_KEEP = [

    # --- Identifiant (clé de jointure avec images et textes)
    "id",

    # --- Localisation
    "neighbourhood_cleansed",       # périmètre de filtrage
    "latitude",
    "longitude",

    # --- Hypothèse A : concentration économique ─────────────
    # Détecte si l'hôte gère plusieurs biens (profil industriel)
    "calculated_host_listings_count",
    # Prix, type de bien et disponibilité : marqueurs d'usage professionnel
    "price",
    "property_type",
    "room_type",
    "availability_365",

    # --- Hypothèse B : déshumanisation de l'accueil ─────────
    # Réactivité de l'hôte : les agences répondent vite et toujours
    "host_response_time",
    "host_response_rate",
    "host_is_superhost",            # label Airbnb de qualité d'accueil
    "host_identity_verified",       # indicateur de confiance

    # --- Contexte commun aux deux hypothèses ─────────────────
    "accommodates",                 # capacité d'accueil
    "bedrooms",
    "beds",
    "minimum_nights",               # séjour court = usage touristique
    "instant_bookable",             # réservation sans contact humain

    # --- Notes et avis (validation croisée avec les .txt NLP)
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

    # Étape 1 — Chargement
    df = pd.read_csv(INPUT_PATH, low_memory=False)
    print(f"Chargé : {len(df):,} lignes, {len(df.columns)} colonnes")

    # Étape 2 — Filtre géographique
    masque = (
        df[COL_QUARTIER].str.strip().str.lower()
        == FILTRE_QUARTIER.strip().lower()
    )
    df = df[masque].copy()
    print(f"Filtre '{FILTRE_QUARTIER}' -> {len(df):,} annonces")

    # Étape 3 — Sélection des colonnes
    # On ne garde que celles qui existent réellement dans le CSV
    cols_presentes = [c for c in COLS_TO_KEEP if c in df.columns]
    cols_absentes  = [c for c in COLS_TO_KEEP if c not in df.columns]

    if cols_absentes:
        print(f"/!\\ Colonnes absentes du CSV (ignorees) : {cols_absentes}")

    df = df[cols_presentes]
    print(f"Colonnes retenues : {len(cols_presentes)} / {len(COLS_TO_KEEP)}")

    # Étape 4 — Nettoyage minimal
    # price est souvent stocké en string "$1,234.00" → float
    if "price" in df.columns:
        df["price"] = (
            df["price"]
            .astype(str)
            .str.replace(r"[\$,]", "", regex=True)
            .pipe(pd.to_numeric, errors="coerce")
        )

    # host_response_rate "97%" → float 0.97
    if "host_response_rate" in df.columns:
        df["host_response_rate"] = (
            df["host_response_rate"]
            .astype(str)
            .str.replace("%", "", regex=False)
            .pipe(pd.to_numeric, errors="coerce")
            .div(100)
        )

    # Étape 5 — Sauvegarde (idempotente : écrase l'ancienne version)
    df.to_csv(OUTPUT_PATH, index=False, encoding="utf-8")
    print(f"Exporte -> {OUTPUT_PATH}")
    print(f"Shape finale : {df.shape[0]:,} lignes x {df.shape[1]} colonnes")


if __name__ == "__main__":
    main()