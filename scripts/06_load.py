# 06_load.py
# Charge transformed_elysee.csv dans PostgreSQL (table elysee_listings_silver)
# pip install pandas sqlalchemy psycopg2-binary python-dotenv

import sys
import pandas as pd
from sqlalchemy import create_engine, text
from dotenv import load_dotenv
import os



# ── Cles de connexion via .env ───────────────────────────────
load_dotenv()
DB_USER     = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")
DB_HOST     = os.getenv("DB_HOST", "localhost")
DB_PORT     = os.getenv("DB_PORT", "5432")
DB_NAME     = os.getenv("DB_NAME", "immovision_db")

# ── Configuration ────────────────────────────────────────────
INPUT_CSV  = "data/processed/transformed_elysee.csv"
TABLE_NAME = "elysee_listings_silver"

# ─────────────────────────────────────────────────────────────


def main():

    # Verification des variables d environnement
    manquantes = [k for k, v in {
        "DB_USER": DB_USER, "DB_PASSWORD": DB_PASSWORD
    }.items() if not v]
    if manquantes:
        print(f"[ERREUR] Variables manquantes dans .env : {manquantes}")
        print("Ajoutez dans votre fichier .env :")
        print("  DB_USER=postgres")
        print("  DB_PASSWORD=votre_mot_de_passe")
        sys.exit(1)

    # Chargement du CSV transforme
    print(f"Lecture de {INPUT_CSV}...")
    df = pd.read_csv(INPUT_CSV, low_memory=False)
    print(f"  {len(df):,} lignes x {len(df.columns)} colonnes")

    # Connexion PostgreSQL
    url = f"postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
    print(f"Connexion -> {DB_HOST}:{DB_PORT}/{DB_NAME}...")
    try:
        engine = create_engine(url)
        # Test de connexion
        with engine.connect() as conn:
            conn.execute(text("SELECT 1"))
        print("  Connexion OK")
    except Exception as e:
        print(f"[ERREUR] Impossible de se connecter : {e}")
        print("Verifiez que PostgreSQL est lance et que vos identifiants sont corrects.")
        sys.exit(1)

    # Chargement dans PostgreSQL
    # if_exists='replace' : recrée la table à chaque exécution (idempotence)
    print(f"Chargement dans la table '{TABLE_NAME}'...")
    try:
        df.to_sql(
            TABLE_NAME,
            engine,
            if_exists="replace",   # recrée la table proprement
            index=False,
            chunksize=500,         # evite les timeouts sur grandes tables
        )
        print(f"  {len(df):,} lignes chargees avec succes")
    except Exception as e:
        print(f"[ERREUR] Echec du chargement : {e}")
        sys.exit(1)

    # Verification rapide
    with engine.connect() as conn:
        result = conn.execute(text(f"SELECT COUNT(*) FROM {TABLE_NAME}"))
        count  = result.scalar()
    print(f"  Verification : {count:,} lignes dans '{TABLE_NAME}'")
    print()
    print("ETL termine : Extract -> Transform -> Load")
    print(f"Table '{TABLE_NAME}' prete pour la Phase 3 (visualisation).")


if __name__ == "__main__":
    main()