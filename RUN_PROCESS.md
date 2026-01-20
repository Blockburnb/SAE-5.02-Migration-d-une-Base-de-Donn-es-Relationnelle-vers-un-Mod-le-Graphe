RUN_PROCESS
==========

But: expliquer comment lancer le traitement et décrire les fonctions principales du script `merge_and_clean_crimes.py`.

1. Objectif
---------
Ce dépôt contient un script pour fusionner et nettoyer les données (2012–2021) provenant du fichier Excel source `crimes-et-delits-enregistres-par-les-services-de-gendarmerie-et-de-police-depuis-2012.xlsx` et produire un CSV prêt pour insertion en base relationnelle : `crimes_clean_2012_2021.csv`.

2. Pré-requis
-----------
- Python 3.8+ installé.
- Packages Python : `pandas`, `openpyxl`, `xlrd`.

Commande d'installation (PowerShell) :

    pip install --upgrade pandas openpyxl xlrd

3. Fichiers importants
---------------------
- `merge_and_clean_crimes.py` : script principal (exécution non interactive). Il lit exclusivement le fichier source attendu et génère le CSV de sortie.
- `crimes-et-delits-enregistres-par-les-services-de-gendarmerie-et-de-police-depuis-2012.xlsx` : fichier source attendu (doit se trouver dans le même dossier que le script).
- `crimes_clean_2012_2021.csv` : fichier de sortie généré.

4. Exécution
-----------
Se placer dans le dossier du dépôt (PowerShell) :

    cd "c:\Users\fbeck\Documents\SAE-5.02-Migration-d-une-Base-de-Donn-es-Relationnelle-vers-un-Mod-le-Graphe"
    python merge_and_clean_crimes.py

Le script :
- traite uniquement `crimes-et-delits-enregistres-par-les-services-de-gendarmerie-et-de-police-depuis-2012.xlsx` ;
- produit systématiquement `crimes_clean_2012_2021.csv` (nom verrouillé) ;
- affiche en sortie un résumé et un aperçu (5 premières lignes PN puis 5 premières lignes GN).

5. Description des fonctions principales
--------------------------------------
- `read_csv_smart(path)`
  - Lecture robuste d'un fichier CSV ou Excel (essais d'encodages/séparateurs). Retourne un `pandas.DataFrame` lu.
  - NOTE : dans la configuration actuelle le script ne fait pas de fallback global — il traitera uniquement le fichier Excel source via `process_file`.

- `extract_meta_from_filename(filename)`
  - Extrait heuristiquement `annee` (ex : 2012) et `service` (`PN` ou `GN`) depuis un nom de fichier ou de feuille.

- `normalize_nombre(x)`
  - Nettoie et convertit une valeur textuelle en entier.
  - Gère espaces insécables, séparateurs milliers/décimales (`.` / `,`) et cas ambigus (ex : `3,0` -> 3).

- `process_file(path)`
  - Lit le fichier Excel, parcourt les feuilles de type "Services PN / GN", détecte automatiquement la ligne d'en-tête (1–3 niveaux), normalise la table (melt), extrait métadonnées et maps `departement` / `perimetre` pour chaque colonne, convertit `nombre_faits` et renvoie un DataFrame long prêt à concaténer.

- `main(input_dir, output_file)`
  - Dans la version verrouillée, vérifie que le fichier source attendu existe dans `input_dir`, appelle `process_file` et concatène les résultats en un seul DataFrame, remplace valeurs nulles, écrit le CSV de sortie et affiche un aperçu.

6. Remarques / troubleshooting
----------------------------
- Le script suppose que le fichier source est présent et lisible. Si Excel laisse un fichier temporaire (`~$...`) ouvert, la lecture peut échouer (PermissionDenied) — fermer Excel ou supprimer le fichier temporaire.
- Si vous souhaitez traiter d'autres fichiers ou rendre l'interface CLI paramétrable à nouveau, modifiez la section `if __name__ == '__main__'` et `main()`.
- Le CSV résultant est volumineux ; il est conseillé de ne pas committer le CSV généré dans Git si vous voulez garder le dépôt léger.

7. Étapes suivantes recommandées
------------------------------
- Ajouter un `requirements.txt` (versions figées) ;
- Ajouter des tests unitaires pour `normalize_nombre` et `process_file` (extraits) ;
- Ajouter des contrôles QA (cohérence codes départements, totaux par année/service).

Fin.
