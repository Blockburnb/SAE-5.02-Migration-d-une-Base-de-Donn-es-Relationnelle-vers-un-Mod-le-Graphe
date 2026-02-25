# ETL — étapes et scripts (expliqué pas à pas)

Ce document décrit, étape par étape, les scripts Python qui composent l'ETL du dépôt. Il précise les fichiers d'entrée et de sortie, l'ordre d'exécution, et les points d'attention.

## Prérequis

- Python 3.8+
- Packages : `pandas`, `openpyxl`, `xlrd` (installer : `pip install pandas openpyxl xlrd`)
- Se placer dans le dossier racine du dépôt (exemple PowerShell ci‑dessous).

Exemple (PowerShell) :

    cd "c:\Users\fbeck\Documents\SAE-5.02-Migration-d-une-Base-de-Donn-es-Relationnelle-vers-un-Mod-le-Graphe"


## Ordre d'exécution recommandé

1. `merge_and_clean_crimes.py`  -> produit `crimes_clean_2012_2021.csv`
2. `process_population.py`     -> produit `population_by_dept_year.csv`
3. `create_and_load_db.py`     -> lit les CSV et crée `crimes_database.db`


## 1) `merge_and_clean_crimes.py`

- Objectif : lire le fichier source Excel des statistiques de crimes (PN/GN), normaliser/transformer les tables "wide" en une table "longue" et produire un CSV uniforme utilisable en aval.
- Entrée : `crimes-et-delits-enregistres-par-les-services-de-gendarmerie-et-de-police-depuis-2012.xlsx` (fichier attendu dans le même dossier que le script).
- Sortie : `crimes_clean_2012_2021.csv` (nom verrouillé par le script).
- Commande : `python merge_and_clean_crimes.py`

Résumé interne :
- Lecture robuste des feuilles Excel (détection automatique des feuilles "Services PN/GN").
- Détection automatique de la ligne d'en-tête (1–3 niveaux) et gestion de colonnes MultiIndex.
- Transformation wide -> long via `pandas.melt`, extraction des métadonnées : `annee`, `service` (PN/GN), `departement`, `perimetre`, `CSP`, `code_index`, `libelle_index`, `nombre_faits`.
- Normalisation des nombres (`normalize_nombre`) pour convertir correctement les formats français (espaces, `.` milliers, `,` décimales) en entiers.
- Exporte le CSV encodé `utf-8-sig` et affiche un aperçu.

Points d'attention :
- Le script est verrouillé pour traiter exclusivement le fichier Excel nommé exactement. Si vous modifiez le nom du fichier, adaptez `target_name` dans le script.
- Fermer Excel si le fichier source est ouvert (éviter les fichiers temporaires `~$...`).


## 2) `process_population.py`

- Objectif : retraiter le fichier INSEE brut (`DS_ESTIMATION_POPULATION_data.csv`) pour obtenir une table `departement x annee` des populations.
- Entrée : `DS_ESTIMATION_POPULATION_data.csv` (fourni dans le dépôt)
- Sorties :
  - `DS_ESTIMATION_POPULATION_clean.csv` (intermédiaire lisible, si généré)
  - `population_by_dept_year.csv` (agrégation finale : colonnes `departement`, `annee`, `population`)
- Commande : `python process_population.py`

Résumé interne :
- Lecture en forçant le séparateur `;` et nettoyage des en-têtes (`clean_headers`).
- Filtrage des lignes de niveau département (`geo_object == 'DEP'`).
- Pivot/agrégation par `geo` (département) et `year` (année).
- Calcul de la population : priorité aux totaux `_T` sinon somme `M+F` ou autres colonnes disponibles.
- Écriture de `population_by_dept_year.csv` en `utf-8`.

Points d'attention :
- Vérifier la présence de colonnes attendues (`geo_object`, `TIME_PERIOD`/`year`, `OBS_VALUE`/`value`).


## 3) `create_and_load_db.py`

- Objectif : construire une base SQLite pivot (`crimes_database.db`) et charger les dimensions + la table de faits à partir des CSV produits précédemment.
- Entrées :
  - `crimes_clean_2012_2021.csv`
  - `population_by_dept_year.csv`
  - `schema_crimes.sql` (schéma SQL des tables)
- Sortie : `crimes_database.db` (fichier SQLite contenant les tables `DIM_*`, `FAITS_CRIMINELS`, `STAT_POPULATION`...)
- Commande : `python create_and_load_db.py`

Résumé interne :
- Supprime (optionnel) le fichier DB existant puis exécute le SQL de `schema_crimes.sql` pour créer les tables.
- Lit les CSV (`crimes_clean_2012_2021.csv` et `population_by_dept_year.csv`) avec types sécurisés.
- Prépare et insère :
  - `DIM_INFRACTIONS` à partir des couples `code_index`/`libelle_index`.
  - `DIM_DEPARTEMENTS` à partir des codes département unifiés.
  - `DIM_SERVICES` (normalisation des champs `service`, `CSP`, `perimetre`) — récupération des `id_service` générés.
  - `DIM_TEMPS` (liste d'années).
  - `FAITS_CRIMINELS` construit en joignant les identifiants (id_service, id_infraction, code_dept, annee) et inséré en masse.
  - `STAT_POPULATION` inséré après mise en forme et filtrage des années présentes.
- Ferme la connexion SQLite.

Points d'attention :
- Le script suppose la présence des colonnes clefs (`annee`, `nombre_faits`, `code_index`, `departement`, etc.).
- Les clés étrangères sont activées : assurez‑vous que les années et codes département insérés existent dans les dimensions avant d'insérer la table de faits.


## Fichiers clés (récapitulatif)

- Entrées brutes :
  - `crimes-et-delits-enregistres-par-les-services-de-gendarmerie-et-de-police-depuis-2012.xlsx`
  - `DS_ESTIMATION_POPULATION_data.csv`
  - `DS_ESTIMATION_POPULATION_metadata.csv` (métadonnées)
  - `schema_crimes.sql`

- Sorties intermédiaires / finales :
  - `crimes_clean_2012_2021.csv` (produit par `merge_and_clean_crimes.py`)
  - `population_by_dept_year.csv` (produit par `process_population.py`)
  - `crimes_database.db` (produit par `create_and_load_db.py`)


## Exécution complète (exemple PowerShell)

    cd "c:\Users\fbeck\Documents\SAE-5.02-Migration-d-une-Base-de-Donn-es-Relationnelle-vers-un-Mod-le-Graphe"
    python merge_and_clean_crimes.py
    python process_population.py
    python create_and_load_db.py


## Validation rapide / QA

- Vérifier que `crimes_clean_2012_2021.csv` contient les colonnes : `annee, service, departement, perimetre, CSP, code_index, libelle_index, nombre_faits`.
- Vérifier que `population_by_dept_year.csv` contient : `departement, annee, population` et que les codes département sont au format attendu (`'01'`, `'2A'`, ...).
- Ouvrir `crimes_database.db` (ex: DB Browser for SQLite) et vérifier les effectifs par table (`DIM_INFRACTIONS`, `DIM_DEPARTEMENTS`, `DIM_SERVICES`, `DIM_TEMPS`, `FAITS_CRIMINELS`, `STAT_POPULATION`).


## Remarques / améliorations possibles

- Rendre `merge_and_clean_crimes.py` paramétrable (chemins/nom fichier) via argparse au lieu du nom verrouillé.
- Ajouter `requirements.txt` et tests unitaires pour `normalize_nombre` et `aggregate_dept_year`.
- Logger (niveau INFO/DEBUG) au lieu d'imprimer via `print` pour faciliter le suivi.


---
Fichier généré automatiquement : `ETL_STEP_BY_STEP.md` — résumé succinct des scripts ETL présents dans le dépôt.
