# 02_ingestion_textes.py
# pip install pandas tqdm

import os
import re
import sys
import argparse
import pandas as pd
from tqdm import tqdm

# ── Configuration ────────────────────────────────────────────
LISTINGS_PATH   = "data/raw/tabular/listings.csv"
REVIEWS_PATH    = "data/raw/tabular/reviews.csv"
OUTPUT_DIR      = "data/raw/texts"
FILTRE_QUARTIER = "Élysée"

COL_ID          = "id"
COL_QUARTIER    = "neighbourhood_cleansed"
COL_LISTING_ID  = "listing_id"
COL_COMMENT     = "comments"
# ─────────────────────────────────────────────────────────────


def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--overwrite",
        action="store_true",
        help="Régénère les fichiers .txt déjà existants",
    )
    return parser.parse_args()


def nettoyer(texte):
    """Supprime les balises HTML, normalise les espaces. Conserve l'UTF-8."""
    texte = re.sub(r"<[^>]+>", " ", str(texte))   # balises HTML
    texte = re.sub(r"\s+", " ", texte)             # espaces multiples
    return texte.strip()


def main():
    args = parse_args()
    os.makedirs(OUTPUT_DIR, exist_ok=True)

    # ── Étape 1 : IDs du périmètre Élysée ───────────────────
    df_listings = pd.read_csv(LISTINGS_PATH, low_memory=False)
    masque = (
        df_listings[COL_QUARTIER].str.strip().str.lower()
        == FILTRE_QUARTIER.strip().lower()
    )
    ids_elysee = set(df_listings.loc[masque, COL_ID].astype(str).str.strip())
    print(f"{len(ids_elysee):,} listings dans le périmètre '{FILTRE_QUARTIER}'")

    # ── Étape 2 : Charger reviews et filtrer ─────────────────
    df_reviews = pd.read_csv(
        REVIEWS_PATH,
        usecols=[COL_LISTING_ID, COL_COMMENT],
        low_memory=False,
    )
    df_reviews[COL_LISTING_ID] = df_reviews[COL_LISTING_ID].astype(str).str.strip()
    df_reviews = df_reviews[df_reviews[COL_LISTING_ID].isin(ids_elysee)]
    print(f"{len(df_reviews):,} avis retenus\n")

    # ── Étape 3 : Regroupement par listing_id ────────────────
    groupes = df_reviews.groupby(COL_LISTING_ID)

    ok, skip, erreur = 0, 0, 0

    for listing_id, groupe in tqdm(groupes, desc="Textes", unit="listing"):
        chemin = os.path.join(OUTPUT_DIR, f"{listing_id}.txt")

        # Idempotence
        if os.path.isfile(chemin) and not args.overwrite:
            skip += 1
            continue

        try:
            # Nettoyage et filtrage des commentaires vides
            avis = []
            for texte in groupe[COL_COMMENT]:
                if pd.isna(texte):
                    continue
                propre = nettoyer(texte)
                if propre:
                    avis.append(propre)

            if not avis:
                continue

            # Écriture avec en-tête + liste à puces
            contenu = f"Commentaires pour l'annonce {listing_id}:\n\n"
            contenu += "\n".join(f"• {a}" for a in avis)

            with open(chemin, "w", encoding="utf-8") as f:
                f.write(contenu)

            ok += 1

        except Exception as e:
            print(f"  Erreur — ID {listing_id} : {e}")
            erreur += 1

    print(f"\nOK : {ok}  |  Skip : {skip}  |  Erreurs : {erreur}")
    print(f"Textes dans : {OUTPUT_DIR}")


if __name__ == "__main__":
    main()