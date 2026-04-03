# README — Load (`06_load.py`)

## Rôle

Charge `transformed_elysee.csv` dans PostgreSQL.
Produit la table `elysee_listings_silver` — Zone Silver du Data Warehouse.

## Prérequis

1. PostgreSQL installé et démarré
2. Base de données créée :
```sql
CREATE DATABASE immovision_db;
```
3. Variables dans `.env` :
```
DB_USER=postgres
DB_PASSWORD=votre_mot_de_passe
DB_HOST=localhost
DB_PORT=5432
DB_NAME=immovision_db
```
4. Dépendances :
```bash
pip install sqlalchemy psycopg2-binary
```

## Lancer

```bash
python scripts/06_load.py
```

## Idempotence

L'argument `if_exists="replace"` recrée la table à chaque exécution.
Le script peut être relancé sans risque de doublon.

## Vérification dans psql

```sql
-- Lister les tables
\dt

-- Compter les lignes
SELECT COUNT(*) FROM elysee_listings_silver;

-- Apercu
SELECT id, price, room_type, Standardization_Score, Neighborhood_Impact
FROM elysee_listings_silver
LIMIT 5;
```

## Schéma de la table

| Colonne | Type | Description |
|---|---|---|
| `id` | BIGINT | Identifiant unique du listing |
| `price` | TEXT | Prix par nuit (brut) |
| `room_type` | INT | 0-3 (encodé) |
| `property_type` | INT | 0-3 (encodé) |
| `host_response_time` | INT | 0-4 (encodé) |
| `host_response_rate` | FLOAT | 0.0 – 1.0 |
| `Standardization_Score` | INT | -1, 0, 1 (IA Vision) |
| `Neighborhood_Impact` | INT | -1, 0, 1 (NLP) |
| ... | ... | ... |

## Preuve d'exécution

> Ajoutez ici votre capture d'écran pgAdmin ou psql.

![Data Warehouse PostgreSQL](screenshots/postgres_data_warehouse.png)