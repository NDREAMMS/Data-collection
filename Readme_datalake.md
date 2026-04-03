# ImmoVision360 — Data Lake & Pipeline ETL

Analyse de l'impact des locations Airbnb courte durée dans le quartier de l'Élysée (Paris).  
Source des données : [Inside Airbnb](https://insideairbnb.com) — usage académique / non commercial.

---

## Prérequis

- Python 3.10+
- PostgreSQL 14+
- Un compte [Google AI Studio](https://aistudio.google.com) (clé Gemini)

```bash
pip install -r requirements.txt
```

---

## Variables d'environnement

Copiez `.env.example` en `.env` et renseignez vos valeurs :

```bash
cp .env.example .env
```

```
GEMINI_API_KEY=...
DB_USER=postgres
DB_PASSWORD=...
DB_HOST=localhost
DB_PORT=5432
DB_NAME=immovision_db
```

**Ne committez jamais `.env` sur GitHub.**

---

## Architecture

```
ImmoVision360_DataLake/
├── data/
│   ├── raw/
│   │   ├── tabular/        # listings.csv, reviews.csv
│   │   ├── images/         # <ID>.jpg  (320x320px)
│   │   └── texts/          # <ID>.txt  (avis agregés)
│   └── processed/
│       ├── filtered_elysee.csv     # sortie 04_extract
│       └── transformed_elysee.csv  # sortie 05_transform
├── scripts/
│   ├── 01_ingestion_images.py
│   ├── 02_ingestion_textes.py
│   ├── 03_sanity_check.py
│   ├── 04_extract.py
│   ├── 05_transform.py
│   └── 06_load.py
├── docs/
│   ├── README_DATALAKE.md
│   ├── README_EXTRACT.md
│   ├── README_TRANSFORM.md
│   ├── README_LOAD.md
│   └── screenshots/
├── .env.example
├── .gitignore
├── requirements.txt
└── README.md
```

---

## Ordre d'exécution

```bash
# Phase 1 — Ingestion
python scripts/01_ingestion_images.py   # telecharge les images
python scripts/02_ingestion_textes.py   # extrait les textes
python scripts/03_sanity_check.py       # controle qualite

# Phase 2 — ETL
python scripts/04_extract.py            # filtre Elysee + selection colonnes
python scripts/05_transform.py          # nettoyage + encodage + features IA
python scripts/06_load.py               # charge dans PostgreSQL
```

---

## Résultats attendus

| Etape | Fichier / Table produit |
|---|---|
| 04_extract | `data/processed/filtered_elysee.csv` |
| 05_transform | `data/processed/transformed_elysee.csv` |
| 06_load | Table `elysee_listings_silver` dans PostgreSQL |

---

## Hypothèses analysées

| | Hypothèse | Features clés |
|---|---|---|
| A | Concentration économique (multipropriétaires) | `calculated_host_listings_count`, `price`, `availability_365` |
| B | Déshumanisation de l'accueil | `host_response_time`, `host_response_rate`, `instant_bookable` |
| C | Standardisation visuelle | `Standardization_Score` (IA Vision), `Neighborhood_Impact` (NLP) |

---

*Attribution : Source des données — [Inside Airbnb](https://insideairbnb.com)*