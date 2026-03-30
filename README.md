# ImmoVision360 Data Lake – Rapport de Livraison

## 1. Titre et Contexte

**Création du Data Lake pour ImmoVision 360**  
Ce projet vise à constituer un Data Lake structuré pour l’analyse des annonces immobilières Airbnb du quartier Élysée. L’objectif est de centraliser, nettoyer et auditer les données tabulaires, images et textes pour des usages analytiques avancés.



## 2. Structure du Répertoire

```
data/
  raw/
    images/      # Images téléchargées des annonces
    tabular/     # Fichiers listings.csv et reviews.csv
    texts/       # Fichiers texte extraits des avis
myenv/           # Environnement Python virtuel
scripts/
  00_data.ipynb          # Notebook d’exploration initiale
  01_ingestion_images.py # Script d’ingestion des images
  02_ingestion_textes.py # Script d’ingestion des textes
  03_sanity_check.py     # Script d’audit et de contrôle qualité
```

---

## 3. Notice d’Exécution

1. **Créer l’environnement Python**  
   - `python -m venv myenv`  
   - Activer l’environnement :  
     - Windows : `myenv\Scripts\activate`
2. **Installer les dépendances**  
   - `pip install pandas tqdm requests Pillow`
3. **Ingestion des images**  
   - `python scripts/01_ingestion_images.py`
4. **Ingestion des textes**  
   - `python scripts/02_ingestion_textes.py`
5. **Audit du Data Lake**  
   - `python scripts/03_sanity_check.py`  
   - Les résultats détaillés s’affichent dans la console.

---

## 4. Audit des Données (Résultats du Sanity Check)

- **Périmètre analysé** : Quartier Élysée, 2 625 annonces de référence

### Images
- Attendues : 2 625
- Présentes : 2 496
- Manquantes : 129
- Taux de complétion : 95,1 %

### Textes
- Attendues : 1 965 (annonces avec au moins un avis)
- Présentes : 1 965
- Manquantes : 0
- Taux de complétion : 100 %

### Cohérence croisée
- Image OK, texte manquant : 624
- Texte OK, image manquante : 93

**Verdict : GO**

---

## 5. Analyse des Pertes

La différence entre le nombre d’annonces et le nombre d’images/textes effectivement ingérés s’explique par plusieurs facteurs techniques :
- **Liens morts ou expirés** : Certaines images ne sont plus accessibles sur les serveurs Airbnb.
- **Blocages anti-bot** : Des protections (robots.txt, captchas) empêchent le téléchargement automatisé de certaines ressources.
- **Erreurs réseau ou serveurs distants** : Timeout, erreurs HTTP, ou interruptions lors du scraping.
- **Données manquantes côté source** : Certaines annonces n’ont pas d’image ou d’avis disponibles dans les fichiers d’origine.

Ce taux de réussite élevé (>95%) est conforme aux standards d’ingénierie de la donnée pour des extractions web à grande échelle.
