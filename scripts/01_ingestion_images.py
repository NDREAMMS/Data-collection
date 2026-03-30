# 01_ingestion_images.py
# pip install requests Pillow pandas tqdm

import os
import time
import random
import requests
import pandas as pd
from PIL import Image
from io import BytesIO
from tqdm import tqdm
from urllib.parse import urlparse
from urllib.robotparser import RobotFileParser

# ── Configuration ────────────────────────────────────────────
CSV_PATH        = "data/raw/tabular/listings.csv"
OUTPUT_DIR      = "data/raw/images"
FILTRE_QUARTIER = "Élysée"          # None = tout télécharger

IMAGE_WIDTH     = 320
IMAGE_HEIGHT    = 320
REQUEST_TIMEOUT = 10
SLEEP_MIN       = 0.5
SLEEP_MAX       = 1.5

COL_ID          = "id"
COL_URL         = "picture_url"
COL_QUARTIER    = "neighbourhood_cleansed"

USER_AGENT = (
    "AcademicResearchBot/1.0 "
    "(Projet academique non commercial; contact: email@univ.fr)"
)
# ─────────────────────────────────────────────────────────────

_robots_cache = {}

def robots_ok(url):
    domaine = urlparse(url).netloc
    if domaine not in _robots_cache:
        try:
            rp = RobotFileParser()
            rp.set_url(f"https://{domaine}/robots.txt")
            rp.read()
            _robots_cache[domaine] = rp.can_fetch(USER_AGENT, url)
        except Exception:
            _robots_cache[domaine] = True
    return _robots_cache[domaine]


def telecharger(listing_id, url):
    chemin = os.path.join(OUTPUT_DIR, f"{listing_id}.jpg")

    # Idempotence : deja telecharge -> on passe
    if os.path.isfile(chemin):
        return "skip"

    # Robots.txt
    if not robots_ok(url):
        print(f"  robots.txt refuse — ID {listing_id}")
        return "robots"

    try:
        r = requests.get(
            url,
            headers={"User-Agent": USER_AGENT},
            timeout=REQUEST_TIMEOUT,
        )
        if r.status_code == 429:
            time.sleep(int(r.headers.get("Retry-After", 30)))
            return "erreur"
        r.raise_for_status()

        img = Image.open(BytesIO(r.content)).convert("RGB")
        img = img.resize((IMAGE_WIDTH, IMAGE_HEIGHT), Image.LANCZOS)
        img.save(chemin, "JPEG", quality=85)
        return "ok"

    except requests.exceptions.Timeout:
        print(f"  Timeout — ID {listing_id}")
    except requests.exceptions.HTTPError as e:
        print(f"  HTTP {e.response.status_code} — ID {listing_id}")
    except requests.exceptions.ConnectionError:
        print(f"  Connexion KO — ID {listing_id}")
    except Exception as e:
        print(f"  Erreur — ID {listing_id} : {e}")
    return "erreur"


def main():
    os.makedirs(OUTPUT_DIR, exist_ok=True)

    df = pd.read_csv(CSV_PATH, low_memory=False)
    print(f"{len(df):,} lignes chargees")

    # Filtre quartier
    if FILTRE_QUARTIER and COL_QUARTIER in df.columns:
        df = df[df[COL_QUARTIER].str.strip().str.lower()
                == FILTRE_QUARTIER.strip().lower()]
        print(f"Filtre '{FILTRE_QUARTIER}' -> {len(df):,} lignes")

    # Nettoyage URLs
    df = df.dropna(subset=[COL_URL])
    df = df[df[COL_URL].str.startswith("http", na=False)]
    print(f"{len(df):,} images a traiter\n")

    ok, skip, erreur = 0, 0, 0

    for _, row in tqdm(df.iterrows(), total=len(df), unit="img"):
        statut = telecharger(str(row[COL_ID]).strip(), str(row[COL_URL]).strip())
        if statut == "ok":
            ok += 1
            time.sleep(random.uniform(SLEEP_MIN, SLEEP_MAX))
        elif statut == "skip":
            skip += 1
        else:
            erreur += 1

    print(f"\nOK : {ok}  |  Skip : {skip}  |  Erreurs : {erreur}")
    print(f"Images dans : {OUTPUT_DIR}")


if __name__ == "__main__":
    main()