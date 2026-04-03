# README — Transform (`05_transform.py`)

## Rôle

Lit `filtered_elysee.csv` (brut), applique toutes les transformations
et génère `transformed_elysee.csv` prêt pour le chargement PostgreSQL.

## A. Nettoyage et normalisation

| Colonne | Avant | Après | Stratégie |
|---|---|---|---|
| `host_response_rate` | `"97%"` | `0.97` | Suppression `%`, division par 100, NaN → médiane |
| `host_response_time` | `"within an hour"` | `4` | Encodage ordinal 0–4 |
| `room_type` | `"Entire home/apt"` | `3` | Encodage ordinal 0–3 |
| `property_type` | `"Entire rental unit"` | `2` | Regroupement en 4 familles |
| `host_is_superhost` | `"t"` / `"f"` | `1` / `0` | Booléen → entier |
| `instant_bookable` | `"t"` / `"f"` | `1` / `0` | Booléen → entier |
| `host_identity_verified` | `"t"` / `"f"` | `1` / `0` | Booléen → entier |
| `review_scores_*` | NaN possible | médiane | Imputation médiane |
| `reviews_per_month` | NaN possible | `0` | NaN = pas encore d'avis |

## Encodage `host_response_time`

```
"within an hour"     -> 4
"within a few hours" -> 3
"within a day"       -> 2
"a few days or more" -> 1
NaN (ne répond pas)  -> 0
```

## Encodage `room_type`

```
"Entire home/apt" -> 3
"Private room"    -> 2
"Hotel room"      -> 1
"Shared room"     -> 0
```

## Encodage `property_type`

```
hotel / apart-hotel / serviced -> 3  (professionnel)
appartement / maison entière   -> 2
chambre / BnB                  -> 1
autre                          -> 0
```

## B. Enrichissement IA (features multimodales)

| Colonne | Source | Valeurs | Description |
|---|---|---|---|
| `Standardization_Score` | Image `.jpg` | `-1, 0, 1` | Style industriel vs résidentiel |
| `Neighborhood_Impact` | Texte `.txt` | `-1, 0, 1` | Expérience hôtélisée vs voisinage naturel |

> **Note :** Les valeurs actuelles sont aléatoires (placeholder).
> Les vrais appels Gemini seront activés en Phase 3.

## Sortie

`data/processed/transformed_elysee.csv` — toutes colonnes numériques,
prêt pour l'injection PostgreSQL via `06_load.py`.