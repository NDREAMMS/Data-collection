# 03_sanity_check.py
# pip install pandas

import os
import pandas as pd

# ── Configuration ────────────────────────────────────────────
LISTINGS_PATH   = "data/raw/tabular/listings.csv"
REVIEWS_PATH    = "data/raw/tabular/reviews.csv"
IMAGES_DIR      = "data/raw/images"
TEXTS_DIR       = "data/raw/texts"
FILTRE_QUARTIER = "Élysée"

COL_ID          = "id"
COL_QUARTIER    = "neighbourhood_cleansed"
COL_LISTING_ID  = "listing_id"
COL_COMMENT     = "comments"
# ─────────────────────────────────────────────────────────────


def sep(titre=""):
    if titre:
        print(f"\n{'─' * 50}")
        print(f"  {titre}")
        print(f"{'─' * 50}")
    else:
        print("─" * 50)


def main():

    # ── 1. Référence : IDs Élysée ────────────────────────────
    df_listings = pd.read_csv(LISTINGS_PATH, low_memory=False)
    masque = (
        df_listings[COL_QUARTIER].str.strip().str.lower()
        == FILTRE_QUARTIER.strip().lower()
    )
    ids_ref = set(df_listings.loc[masque, COL_ID].astype(str).str.strip())

    # IDs ayant au moins un commentaire (référence textes)
    df_reviews = pd.read_csv(
        REVIEWS_PATH,
        usecols=[COL_LISTING_ID, COL_COMMENT],
        low_memory=False,
    )
    df_reviews[COL_LISTING_ID] = df_reviews[COL_LISTING_ID].astype(str).str.strip()
    df_reviews = df_reviews[df_reviews[COL_LISTING_ID].isin(ids_ref)]
    df_reviews = df_reviews[df_reviews[COL_COMMENT].notna()]
    ids_avec_avis = set(df_reviews[COL_LISTING_ID].unique())

    # ── 2. Inventaire physique ───────────────────────────────
    jpgs = {f[:-4] for f in os.listdir(IMAGES_DIR) if f.endswith(".jpg")} \
           if os.path.isdir(IMAGES_DIR) else set()

    txts = set()
    if os.path.isdir(TEXTS_DIR):
        for f in os.listdir(TEXTS_DIR):
            if f.endswith(".txt"):
                chemin = os.path.join(TEXTS_DIR, f)
                if os.path.getsize(chemin) > 0:   # ignorer les fichiers vides
                    txts.add(f[:-4])

    # ── 3. Calculs ───────────────────────────────────────────
    # Images
    img_attendues  = len(ids_ref)
    img_presentes  = len(jpgs & ids_ref)          # présentes ET dans le périmètre
    img_manquantes = ids_ref - jpgs
    img_orphelines = jpgs - ids_ref               # jpg hors périmètre

    # Textes
    txt_attendus   = len(ids_avec_avis)
    txt_presents   = len(txts & ids_avec_avis)
    txt_manquants  = ids_avec_avis - txts
    txt_orphelins  = txts - ids_avec_avis

    # Cohérence croisée
    img_sans_txt   = (jpgs & ids_ref) - txts      # image OK mais pas de texte
    txt_sans_img   = (txts & ids_avec_avis) - jpgs # texte OK mais pas d'image

    # Taux
    taux_img = img_presentes / img_attendues * 100 if img_attendues else 0
    taux_txt = txt_presents  / txt_attendus  * 100 if txt_attendus  else 0

    # ── 4. Rapport ───────────────────────────────────────────
    print()
    print("=" * 50)
    print("  SANITY CHECK — Data Lake ImmoVision360")
    print("=" * 50)
    print(f"  Périmètre : {FILTRE_QUARTIER} — {len(ids_ref):,} annonces de référence")
    sep()

    sep("IMAGES")
    print(f"  Attendues     : {img_attendues:,}")
    print(f"  Présentes     : {img_presentes:,}")
    print(f"  Manquantes    : {len(img_manquantes):,}")
    print(f"  Hors périmètre: {len(img_orphelines):,}")
    print(f"  Complétion    : {taux_img:.1f}%")
    if img_manquantes:
        apercu = sorted(img_manquantes)[:5]
        print(f"  5 premiers IDs sans .jpg : {apercu}")

    sep("TEXTES")
    print(f"  Attendus      : {txt_attendus:,}  (listings avec au moins 1 avis)")
    print(f"  Présents      : {txt_presents:,}")
    print(f"  Manquants     : {len(txt_manquants):,}")
    print(f"  Hors périmètre: {len(txt_orphelins):,}")
    print(f"  Complétion    : {taux_txt:.1f}%")
    if txt_manquants:
        apercu = sorted(txt_manquants)[:5]
        print(f"  5 premiers IDs sans .txt : {apercu}")

    sep("COHÉRENCE CROISÉE")
    print(f"  Image OK, texte manquant : {len(img_sans_txt):,}")
    print(f"  Texte OK, image manquante: {len(txt_sans_img):,}")
    if img_sans_txt:
        print(f"  Exemples (img sans txt)  : {sorted(img_sans_txt)[:5]}")
    if txt_sans_img:
        print(f"  Exemples (txt sans img)  : {sorted(txt_sans_img)[:5]}")

    sep()
    go_img = taux_img >= 70
    go_txt = taux_txt >= 60
    verdict = "GO" if (go_img and go_txt) else "NO-GO"
    print(f"  Verdict : {verdict}")
    if not go_img:
        print("  → Relancer 01_ingestion_images.py")
    if not go_txt:
        print("  → Relancer 02_ingestion_textes.py")
    print("=" * 50)
    print()


if __name__ == "__main__":
    main()