# 05_transform.py
# Roles : nettoyage, encodage categoriel, enrichissement IA Gemini
# Lit  : data/processed/filtered_elysee.csv  (brut, sorti de 04_extract)
# Ecrit: data/processed/transformed_elysee.csv
# pip install pandas google-genai Pillow python-dotenv tqdm

import os
import sys
import time
import re
import random
import pandas as pd
from pathlib import Path
from tqdm import tqdm
from dotenv import load_dotenv
import PIL.Image
from google import genai

if sys.stdout.encoding != "utf-8":
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")

# ── Cle API ─────────────────────────────────────────────────
load_dotenv()
GEMINI_API_KEY = os.getenv("GEMINI_API_KEY", "")

# ── Chemins ─────────────────────────────────────────────────
INPUT_CSV  = Path("data/processed/filtered_elysee.csv")
OUTPUT_CSV = Path("data/processed/transformed_elysee.csv")
IMAGES_DIR = Path("data/raw/images")
TEXTS_DIR  = Path("data/raw/texts")

# ── Parametres ──────────────────────────────────────────────
SLEEP_BETWEEN = float(os.getenv("SLEEP_BETWEEN", "1.0"))
GEMINI_MODEL = os.getenv("GEMINI_MODEL", "gemini-2.0-flash")
GEMINI_MAX_RETRIES = int(os.getenv("GEMINI_MAX_RETRIES", "6"))
GEMINI_MAX_SLEEP = float(os.getenv("GEMINI_MAX_SLEEP", "120"))
GEMINI_CONSECUTIVE_ERRORS_ABORT = int(os.getenv("GEMINI_CONSECUTIVE_ERRORS_ABORT", "25"))
CHECKPOINT_EVERY = int(os.getenv("CHECKPOINT_EVERY", "25"))
CHECKPOINT_EVERY_SECONDS = float(os.getenv("CHECKPOINT_EVERY_SECONDS", "120"))

# ── Tables d encodage categoriel ────────────────────────────

MAP_RESPONSE_TIME = {
    "within an hour"    : 4,
    "within a few hours": 3,
    "within a day"      : 2,
    "a few days or more": 1,
}

MAP_ROOM_TYPE = {
    "entire home/apt": 3,
    "private room"   : 2,
    "hotel room"     : 1,
    "shared room"    : 0,
}

def encoder_property_type(val: str) -> int:
    val = str(val).lower()
    if any(k in val for k in ["hotel", "apart-hotel", "serviced"]):
        return 3
    if any(k in val for k in ["entire", "whole", "house", "apartment",
                               "condo", "loft", "villa", "chalet"]):
        return 2
    if any(k in val for k in ["room", "bed and breakfast", "bnb"]):
        return 1
    return 0

# ── Prompts Gemini ───────────────────────────────────────────

PROMPT_IMAGE = (
    "Analyse cette image de logement Airbnb et reponds UNIQUEMENT "
    "par l une de ces valeurs exactes :\n"
    "- Appartement industrialise\n"
    "- Appartement personnel\n"
    "- Autre\n\n"
    "Criteres :\n"
    "- Appartement industrialise : deco minimaliste type catalogue, "
    "style hotel, impersonnel, froid\n"
    "- Appartement personnel : objets du quotidien, livres, deco "
    "heteroclite, chaleureux\n"
    "- Autre : image floue, exterieur, plan, sans interieur visible"
)

PROMPT_TEXTE_TPL = (
    "Voici des commentaires Airbnb agregés pour un meme logement.\n"
    "Reponds UNIQUEMENT par l une de ces valeurs exactes :\n"
    "- Hotelise\n"
    "- Voisinage naturel\n"
    "- Indetermine\n\n"
    "Criteres :\n"
    "- Hotelise : boite a cles, consignes PDF, peu de contact humain\n"
    "- Voisinage naturel : rencontre avec l hote, conseils de quartier\n"
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


# ── Section 1 : Nettoyage & encodage ────────────────────────

def transformer(df: pd.DataFrame) -> pd.DataFrame:
    print("Transformations...")



    # -- host_response_rate : "97%" -> 0.97
    if "host_response_rate" in df.columns:
        df["host_response_rate"] = (
            df["host_response_rate"]
            .astype(str)
            .str.replace("%", "", regex=False)
            .replace({"nan": float("nan"), "": float("nan")})
            .pipe(pd.to_numeric, errors="coerce")
            .div(100)
        )
        med = df["host_response_rate"].median()
        n   = df["host_response_rate"].isna().sum()
        df["host_response_rate"] = df["host_response_rate"].fillna(
            med if not pd.isna(med) else 0
        )
        print(f"  host_response_rate : {n} NaN -> mediane ({med:.2f})")

    # -- host_response_time : texte -> 0-4
    if "host_response_time" in df.columns:
        df["host_response_time"] = (
            df["host_response_time"]
            .str.strip().str.lower()
            .map(MAP_RESPONSE_TIME)
            .fillna(0)
            .astype(int)
        )
        print(f"  host_response_time : {df['host_response_time'].value_counts().to_dict()}")

    # -- room_type : texte -> 0-3
    if "room_type" in df.columns:
        df["room_type"] = (
            df["room_type"]
            .str.strip().str.lower()
            .map(MAP_ROOM_TYPE)
            .fillna(0)
            .astype(int)
        )
        print(f"  room_type : {df['room_type'].value_counts().to_dict()}")

    # -- property_type : texte -> 0-3
    if "property_type" in df.columns:
        df["property_type"] = df["property_type"].apply(encoder_property_type)
        print(f"  property_type : {df['property_type'].value_counts().to_dict()}")

    # -- Booleens t/f -> 1/0
    for col in ["host_is_superhost", "host_identity_verified", "instant_bookable"]:
        if col in df.columns:
            df[col] = (
                df[col].map({"t": 1, "f": 0, True: 1, False: 0})
                .fillna(0).astype(int)
            )

    # -- review_scores_* : NaN -> mediane
    for col in [c for c in df.columns if c.startswith("review_scores_")]:
        med = df[col].median()
        n   = df[col].isna().sum()
        df[col] = df[col].fillna(med if not pd.isna(med) else 0)
        if n:
            print(f"  {col} : {n} NaN -> mediane ({med:.2f})")

    # -- reviews_per_month : NaN -> 0 (pas encore d avis)
    if "reviews_per_month" in df.columns:
        n = df["reviews_per_month"].isna().sum()
        df["reviews_per_month"] = df["reviews_per_month"].fillna(0)
        if n:
            print(f"  reviews_per_month : {n} NaN -> 0")

    # -- bedrooms / beds : NaN -> mediane
    for col in ["bedrooms", "beds", "accommodates"]:
        if col in df.columns:
            med = df[col].median()
            df[col] = df[col].fillna(med if not pd.isna(med) else 1)

    print(f"  Shape apres transformation : {df.shape[0]:,} x {df.shape[1]}")
    return df.reset_index(drop=True)


# ── Section 2 : Enrichissement IA ───────────────────────────

class GeminiHardError(RuntimeError):
    pass


def _parse_retry_delay_seconds(message: str) -> float | None:
    m = re.search(r"retryDelay':\\s*'([^']+)'", message)
    if m:
        raw = m.group(1).strip().lower()
        if raw.endswith("ms"):
            try:
                return float(raw[:-2]) / 1000.0
            except ValueError:
                return None
        if raw.endswith("s"):
            try:
                return float(raw[:-1])
            except ValueError:
                return None

    m = re.search(r"Please retry in\\s+([0-9.]+)\\s*(ms|s)\\.", message)
    if m:
        val = float(m.group(1))
        unit = m.group(2)
        return val / 1000.0 if unit == "ms" else val

    return None


def _is_quota_zero(message: str) -> bool:
    # Cas observe: "... limit: 0, model: gemini-2.0-flash"
    return "limit: 0" in message.lower()


def appel_gemini(client, contenu: list) -> str | None:
    last_error: Exception | None = None

    for attempt in range(GEMINI_MAX_RETRIES + 1):
        try:
            response = client.models.generate_content(
                model=GEMINI_MODEL,
                contents=contenu,
            )
            if not getattr(response, "text", None):
                return None
            return response.text.strip()

        except Exception as e:
            last_error = e
            msg = str(e)
            msg_lower = msg.lower()

            # Erreurs d'auth/cle: inutile de boucler
            if any(k in msg_lower for k in ["api key", "permission_denied", "unauthenticated", "invalid_argument"]):
                raise GeminiHardError(
                    "Gemini: erreur d'authentification/permission (verifiez GEMINI_API_KEY / projet)."
                ) from e

            # Rate limit / quota (429 RESOURCE_EXHAUSTED)
            if "429" in msg_lower or "resource_exhausted" in msg_lower or "quota exceeded" in msg_lower:
                if _is_quota_zero(msg):
                    raise GeminiHardError(
                        "Gemini: quota a 0 (limit: 0). Ce n'est pas recuperable par retry: "
                        "verifiez votre plan/billing et les quotas du projet Google AI."
                    ) from e

                if attempt >= GEMINI_MAX_RETRIES:
                    break

                delay = _parse_retry_delay_seconds(msg)
                if delay is None:
                    delay = min(2.0 * (2**attempt), GEMINI_MAX_SLEEP)
                delay = min(max(delay, 1.0), GEMINI_MAX_SLEEP)
                delay *= random.uniform(0.9, 1.1)
                print(f"    Rate limit Gemini (429) -> attente {delay:.1f}s (tentative {attempt + 1}/{GEMINI_MAX_RETRIES})")
                time.sleep(delay)
                continue

            print(f"    API erreur : {e}")
            return None

    print(f"    API erreur (apres retries) : {last_error}")
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
    except GeminiHardError:
        raise
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
    except GeminiHardError:
        raise
    except Exception as e:
        print(f"    Texte {listing_id} : {e}")
        return None, None


# ── Section 3 : Pipeline principal ──────────────────────────

def main():
    skip_gemini = os.getenv("SKIP_GEMINI_ENRICHMENT", "").strip().lower() in {"1", "true", "yes", "y"}
    enable_gemini = (not skip_gemini) and bool(GEMINI_API_KEY)

    if skip_gemini:
        print("[INFO] SKIP_GEMINI_ENRICHMENT=1 -> enrichissement IA desactive.")
    elif not GEMINI_API_KEY:
        print("[WARN] GEMINI_API_KEY manquante dans .env -> enrichissement IA desactive.")

    client = genai.Client(api_key=GEMINI_API_KEY) if enable_gemini else None
    OUTPUT_CSV.parent.mkdir(parents=True, exist_ok=True)

    # Chargement du CSV brut sorti de 04_extract
    df = pd.read_csv(INPUT_CSV, low_memory=False)
    print(f"Charge : {len(df):,} lignes depuis {INPUT_CSV}")

    # Toutes les transformations
    df = transformer(df)

    if len(df) == 0:
        print("[ERREUR] DataFrame vide apres transformation.")
        print("Regardez le diagnostic [diagnostic price] ci-dessus")
        print("pour comprendre le format brut de la colonne price.")
        sys.exit(1)

    # Colonnes IA
    COL_STD_CAT   = "Standardization_Category"
    COL_STD_SCORE = "Standardization_Score"
    COL_NEI_CAT   = "Neighborhood_Category"
    COL_NEI_SCORE = "Neighborhood_Impact"

    # Idempotence : recuperer les scores deja calcules
    if OUTPUT_CSV.exists():
        try:
            df_old = pd.read_csv(OUTPUT_CSV, dtype={"id": str}, low_memory=False)
            keep_cols = [
                c for c in ["id", COL_STD_CAT, COL_STD_SCORE, COL_NEI_CAT, COL_NEI_SCORE]
                if c in df_old.columns
            ]
            df_old = df_old[keep_cols]
            df["id"] = df["id"].astype(str)
            df = df.merge(df_old, on="id", how="left")

            for col in [COL_STD_CAT, COL_NEI_CAT]:
                if col not in df.columns:
                    df[col] = None
            for col in [COL_STD_SCORE, COL_NEI_SCORE]:
                if col not in df.columns:
                    df[col] = pd.NA

            n_img = df[COL_STD_SCORE].notna().sum()
            n_txt = df[COL_NEI_SCORE].notna().sum()
            print(f"Idempotence : {n_img} img + {n_txt} txt deja calcules -> skip")
        except Exception:
            df["id"]          = df["id"].astype(str)
            df[COL_STD_CAT]   = None
            df[COL_STD_SCORE] = pd.NA
            df[COL_NEI_CAT]   = None
            df[COL_NEI_SCORE] = pd.NA
    else:
        df["id"]          = df["id"].astype(str)
        df[COL_STD_CAT]   = None
        df[COL_STD_SCORE] = pd.NA
        df[COL_NEI_CAT]   = None
        df[COL_NEI_SCORE] = pd.NA

    # Boucle enrichissement IA
    if not enable_gemini:
        df.to_csv(OUTPUT_CSV, index=False, encoding="utf-8")
        print(f"\nSauvegarde -> {OUTPUT_CSV}")
        print("[INFO] Enrichissement IA non execute (Gemini desactive).")
        return

    print(f"\nEnrichissement IA ({len(df):,} lignes) - Ctrl+C pour interrompre\n")

    def save_checkpoint() -> None:
        tmp = OUTPUT_CSV.with_suffix(".csv.tmp")
        df.to_csv(tmp, index=False, encoding="utf-8")
        tmp.replace(OUTPUT_CSV)

    consecutive_gemini_errors = 0
    n_modifs_since_save = 0
    last_save_ts = time.time()

    try:
        for i, row in tqdm(df.iterrows(), total=len(df), unit="listing"):
            lid     = str(row["id"]).strip()
            modifie = False

            if pd.isna(row.get(COL_STD_SCORE)):
                try:
                    cat, score = analyser_image(client, lid)
                except GeminiHardError as e:
                    print(f"\n[ERREUR] {e}\nArret de l'enrichissement IA (sauvegarde des donnees deja calculees).")
                    break

                df.at[i, COL_STD_CAT] = cat
                df.at[i, COL_STD_SCORE] = score if score is not None else pd.NA

                if cat is None and score is None and (IMAGES_DIR / f"{lid}.jpg").exists():
                    consecutive_gemini_errors += 1
                else:
                    consecutive_gemini_errors = 0
                    modifie = True

            if pd.isna(row.get(COL_NEI_SCORE)):
                try:
                    cat, score = analyser_texte(client, lid)
                except GeminiHardError as e:
                    print(f"\n[ERREUR] {e}\nArret de l'enrichissement IA (sauvegarde des donnees deja calculees).")
                    break

                df.at[i, COL_NEI_CAT] = cat
                df.at[i, COL_NEI_SCORE] = score if score is not None else pd.NA

                if cat is None and score is None and (TEXTS_DIR / f"{lid}.txt").exists():
                    consecutive_gemini_errors += 1
                else:
                    consecutive_gemini_errors = 0
                    modifie = True

            if modifie:
                n_modifs_since_save += 1
                time.sleep(SLEEP_BETWEEN)

            now = time.time()
            if n_modifs_since_save and (n_modifs_since_save >= CHECKPOINT_EVERY or (now - last_save_ts) >= CHECKPOINT_EVERY_SECONDS):
                save_checkpoint()
                n_modifs_since_save = 0
                last_save_ts = now

            if consecutive_gemini_errors >= GEMINI_CONSECUTIVE_ERRORS_ABORT:
                print(
                    f"\n[ERREUR] Trop d'erreurs Gemini consecutives ({consecutive_gemini_errors}). "
                    "Arret et sauvegarde pour eviter de boucler inutilement."
                )
                break

    except KeyboardInterrupt:
        print("\nInterruption - sauvegarde en cours...")

    # Sauvegarde finale
    save_checkpoint()
    print(f"\nSauvegarde -> {OUTPUT_CSV}")
    print(f"Standardization_Score : {df[COL_STD_SCORE].notna().sum()}/{len(df)}")
    print(f"Neighborhood_Impact   : {df[COL_NEI_SCORE].notna().sum()}/{len(df)}")
    print(f"\nDistribution Standardization_Score :")
    print(df[COL_STD_SCORE].value_counts().to_string())
    print(f"\nDistribution Neighborhood_Impact :")
    print(df[COL_NEI_SCORE].value_counts().to_string())


if __name__ == "__main__":
    main()
