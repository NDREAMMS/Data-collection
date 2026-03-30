# 05_transform.py
# pip install pandas google-genai Pillow python-dotenv tqdm

import os
import sys
import time
import pandas as pd
from pathlib import Path
from tqdm import tqdm
from dotenv import load_dotenv
import PIL.Image

# Nouveau SDK Gemini (remplace google-generativeai deprecie)
from google import genai
from google.genai import types

# ── Cle API ─────────────────────────────────────────────────
load_dotenv()
GEMINI_API_KEY = os.getenv("GEMINI_API_KEY", "")

# ── Chemins ─────────────────────────────────────────────────
INPUT_CSV  = Path("data/processed/filtered_elysee.csv")
OUTPUT_CSV = Path("data/processed/transformed_elysee.csv")
IMAGES_DIR = Path("data/raw/images")
TEXTS_DIR  = Path("data/raw/texts")

# ── Parametres ──────────────────────────────────────────────
SLEEP_BETWEEN = 1.0    # pause entre chaque ligne (quota API)
PRICE_MIN     = 10
PRICE_MAX     = 1500

# ── Prompts ─────────────────────────────────────────────────
PROMPT_IMAGE = (
    "Analyse cette image de logement Airbnb et reponds UNIQUEMENT "
    "par l'une de ces valeurs exactes :\n"
    "- Appartement industrialise\n"
    "- Appartement personnel\n"
    "- Autre\n\n"
    "Criteres :\n"
    "- Appartement industrialise : deco minimaliste type catalogue, "
    "style hotel, impersonnel, froid\n"
    "- Appartement personnel : objets du quotidien, livres, deco "
    "heteroclite, chaleureux\n"
    "- Autre : image floue, exterieur, plan, ou sans interieur visible"
)

PROMPT_TEXTE_TPL = (
    "Voici des commentaires Airbnb agregés pour un meme logement.\n"
    "Reponds UNIQUEMENT par l'une de ces valeurs exactes :\n"
    "- Hotelise\n"
    "- Voisinage naturel\n"
    "- Indetermine\n\n"
    "Criteres :\n"
    "- Hotelise : boite a cles, consignes PDF, peu de contact humain\n"
    "- Voisinage naturel : rencontre avec l hote, conseils de quartier, "
    "echanges humains\n"
    "- Indetermine : commentaires trop courts ou sans info pertinente\n\n"
    "Commentaires :\n{texte}"
)

MAP_STANDARDIZATION = {
    "appartement industrialise": 2,
    "appartement personnel"    : 0,
    "autre"                    : 1,
}
MAP_NEIGHBORHOOD = {
    "hotelise"         : 2,
    "voisinage naturel": 0,
    "indetermine"      : 1,
}

# ─────────────────────────────────────────────────────────────


def nettoyer(df: pd.DataFrame) -> pd.DataFrame:
    print("Nettoyage...")

    # -- price : convertir si encore en string "$120.00", puis filtrer
    if "price" in df.columns:
        # Conversion robuste (gere les cas deja float ET les strings)
        df["price"] = (
            df["price"]
            .astype(str)
            .str.replace(r"[\$,\s]", "", regex=True)
            .replace("nan", float("nan"))
            .pipe(pd.to_numeric, errors="coerce")
        )
        n_nan = df["price"].isna().sum()
        df = df.dropna(subset=["price"])
        df = df[(df["price"] >= PRICE_MIN) & (df["price"] <= PRICE_MAX)]
        print(f"  price : {n_nan} NaN supprimes, "
              f"outliers hors [{PRICE_MIN}-{PRICE_MAX}] supprimes, "
              f"{len(df)} lignes restantes")

    # -- host_response_rate : convertir si string "97%", puis mediane
    if "host_response_rate" in df.columns:
        df["host_response_rate"] = (
            df["host_response_rate"]
            .astype(str)
            .str.replace("%", "", regex=False)
            .replace("nan", float("nan"))
            .pipe(pd.to_numeric, errors="coerce")
        )
        # Si les valeurs sont entre 0 et 100, normaliser en 0-1
        if df["host_response_rate"].max() > 1:
            df["host_response_rate"] = df["host_response_rate"] / 100
        mediane = df["host_response_rate"].median()
        n = df["host_response_rate"].isna().sum()
        df["host_response_rate"] = df["host_response_rate"].fillna(
            mediane if not pd.isna(mediane) else 0
        )
        print(f"  host_response_rate : {n} NaN -> {mediane:.2f}")

    # -- review_scores_* : mediane
    for col in [c for c in df.columns if c.startswith("review_scores_")]:
        mediane = df[col].median()
        n = df[col].isna().sum()
        df[col] = df[col].fillna(mediane if not pd.isna(mediane) else 0)
        if n:
            print(f"  {col} : {n} NaN -> mediane ({mediane:.2f})")

    # -- reviews_per_month : NaN = pas encore d avis -> 0
    if "reviews_per_month" in df.columns:
        n = df["reviews_per_month"].isna().sum()
        df["reviews_per_month"] = df["reviews_per_month"].fillna(0)
        if n:
            print(f"  reviews_per_month : {n} NaN -> 0")

    # -- bedrooms / beds : mediane
    for col in ["bedrooms", "beds", "accommodates"]:
        if col in df.columns:
            med = df[col].median()
            df[col] = df[col].fillna(med if not pd.isna(med) else 1)

    # -- Booleens t/f -> True/False
    for col in ["host_is_superhost", "host_identity_verified", "instant_bookable"]:
        if col in df.columns:
            df[col] = df[col].map({"t": True, "f": False,
                                   True: True, False: False})

    print(f"  Shape apres nettoyage : {df.shape[0]:,} lignes x {df.shape[1]} colonnes")
    return df.reset_index(drop=True)


def appel_gemini(client, contenu: list) -> str | None:
    """Appelle Gemini 2.0 Flash. Retourne le texte ou None si erreur."""
    try:
        response = client.models.generate_content(
            model="gemini-2.0-flash",
            contents=contenu,
        )
        return response.text.strip()
    except Exception as e:
        print(f"    API erreur : {e}")
        return None


def analyser_image(client, listing_id: str):
    chemin = IMAGES_DIR / f"{listing_id}.jpg"
    if not chemin.exists():
        return None, None
    try:
        img     = PIL.Image.open(chemin)
        reponse = appel_gemini(client, [PROMPT_IMAGE, img])
        if reponse is None:
            return None, None
        score = MAP_STANDARDIZATION.get(reponse.lower().strip(), 1)
        return reponse, score
    except Exception as e:
        print(f"    Image {listing_id} : {e}")
        return None, None


def analyser_texte(client, listing_id: str):
    chemin = TEXTS_DIR / f"{listing_id}.txt"
    if not chemin.exists():
        return None, None
    try:
        texte   = chemin.read_text(encoding="utf-8")[:4000]
        prompt  = PROMPT_TEXTE_TPL.format(texte=texte)
        reponse = appel_gemini(client, [prompt])
        if reponse is None:
            return None, None
        score = MAP_NEIGHBORHOOD.get(reponse.lower().strip(), 1)
        return reponse, score
    except Exception as e:
        print(f"    Texte {listing_id} : {e}")
        return None, None


def main():
    if not GEMINI_API_KEY:
        print("[ERREUR] GEMINI_API_KEY manquante dans le fichier .env")
        sys.exit(1)

    client = genai.Client(api_key=GEMINI_API_KEY)
    OUTPUT_CSV.parent.mkdir(parents=True, exist_ok=True)

    # Chargement
    df = pd.read_csv(INPUT_CSV, low_memory=False)
    print(f"Charge : {len(df):,} lignes depuis {INPUT_CSV}")

    # Nettoyage
    df = nettoyer(df)

    if len(df) == 0:
        print("[ERREUR] DataFrame vide apres nettoyage.")
        print("Verifiez que filtered_elysee.csv contient bien une colonne 'price'")
        print("avec des valeurs numeriques ou des strings du type '$120.00'.")
        sys.exit(1)

    # Colonnes IA
    COL_STD_CAT   = "Standardization_Category"
    COL_STD_SCORE = "Standardization_Score"
    COL_NEI_CAT   = "Neighborhood_Category"
    COL_NEI_SCORE = "Neighborhood_Impact"

    # Idempotence : recuperer les scores deja calcules
    if OUTPUT_CSV.exists():
        try:
            df_old = pd.read_csv(
                OUTPUT_CSV,
                usecols=["id", COL_STD_SCORE, COL_NEI_SCORE],
                dtype={"id": str},
                low_memory=False,
            )
            df["id"] = df["id"].astype(str)
            df = df.merge(df_old, on="id", how="left")
            n_img = df[COL_STD_SCORE].notna().sum()
            n_txt = df[COL_NEI_SCORE].notna().sum()
            print(f"Idempotence : {n_img} img + {n_txt} txt deja calcules -> skip")
        except Exception:
            df["id"]         = df["id"].astype(str)
            df[COL_STD_CAT]  = None
            df[COL_STD_SCORE]= pd.NA
            df[COL_NEI_CAT]  = None
            df[COL_NEI_SCORE]= pd.NA
    else:
        df["id"]         = df["id"].astype(str)
        df[COL_STD_CAT]  = None
        df[COL_STD_SCORE]= pd.NA
        df[COL_NEI_CAT]  = None
        df[COL_NEI_SCORE]= pd.NA

    # Boucle enrichissement IA
    print(f"\nEnrichissement IA ({len(df):,} lignes) - Ctrl+C pour interrompre\n")

    try:
        for i, row in tqdm(df.iterrows(), total=len(df), unit="listing"):
            lid     = str(row["id"]).strip()
            modifie = False

            if pd.isna(row.get(COL_STD_SCORE)):
                cat, score = analyser_image(client, lid)
                df.at[i, COL_STD_CAT]   = cat
                df.at[i, COL_STD_SCORE] = score
                modifie = True

            if pd.isna(row.get(COL_NEI_SCORE)):
                cat, score = analyser_texte(client, lid)
                df.at[i, COL_NEI_CAT]   = cat
                df.at[i, COL_NEI_SCORE] = score
                modifie = True

            if modifie:
                time.sleep(SLEEP_BETWEEN)

    except KeyboardInterrupt:
        print("\nInterruption - sauvegarde en cours...")

    # Sauvegarde
    df.to_csv(OUTPUT_CSV, index=False, encoding="utf-8")
    print(f"\nSauvegarde -> {OUTPUT_CSV}")
    print(f"Standardization_Score : {df[COL_STD_SCORE].notna().sum()}/{len(df)}")
    print(f"Neighborhood_Impact   : {df[COL_NEI_SCORE].notna().sum()}/{len(df)}")
    print(f"\nDistribution Standardization_Score :")
    print(df[COL_STD_SCORE].value_counts().to_string())
    print(f"\nDistribution Neighborhood_Impact :")
    print(df[COL_NEI_SCORE].value_counts().to_string())


if __name__ == "__main__":
    main()