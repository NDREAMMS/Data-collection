# README — Extract (`04_extract.py`)

## Rôle

Lit `data/raw/tabular/listings.csv` (81 000+ lignes, 79 colonnes),
filtre le périmètre Élysée et sélectionne les 24 colonnes utiles aux hypothèses.

**Aucune transformation ici** — les valeurs brutes sont conservées intactes
pour que `05_transform.py` les traite de façon traçable.

## Filtre géographique

Colonne `neighbourhood_cleansed == "Élysée"` → ~2 600 annonces retenues.

## Mapping hypothèses / features retenues

### Hypothèse A — Concentration économique

| Colonne | Justification |
|---|---|
| `calculated_host_listings_count` | Détecte les multipropriétaires (profil industriel) |
| `price` | Prix par nuit — marqueur d'usage professionnel |
| `property_type` | Type de bien (appartement, hotel, chambre...) |
| `room_type` | Logement entier vs chambre partagée |
| `availability_365` | Disponible 300j/an = produit financier, pas logement partagé |

### Hypothèse B — Déshumanisation de l'accueil

| Colonne | Justification |
|---|---|
| `host_response_time` | Agences répondent en <1h : gestion industrielle |
| `host_response_rate` | 100% de réponse = process automatisé |
| `host_is_superhost` | Label qualité Airbnb |
| `host_identity_verified` | Indicateur de confiance |
| `instant_bookable` | Réservation sans contact humain |
| `minimum_nights` | 1 nuit = flux touristique max ; 30 nuits = usage résidentiel détourné |

### Hypothèse C — Standardisation visuelle

Traitée entièrement par les colonnes IA générées dans `05_transform.py`
(`Standardization_Score`, `Neighborhood_Impact`).

## Sortie

`data/processed/filtered_elysee.csv` — valeurs brutes, ~2 600 lignes x 24 colonnes.