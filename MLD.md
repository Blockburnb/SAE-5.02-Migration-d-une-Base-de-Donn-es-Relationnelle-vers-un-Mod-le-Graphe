# Modèle Logique de Données (MLD)

Ce document décrit le modèle logique correspondant au fichier `schema_crimes.sql`.

---

## Tables et colonnes (conformes au SQL)

### DIM_INFRACTIONS
- id_infraction (INTEGER) — Clé primaire
- libelle (VARCHAR(255), NOT NULL)

### DIM_DEPARTEMENTS
- code_dept (VARCHAR(3)) — Clé primaire (ex: '01', '2A', '974')

### DIM_SERVICES
- id_service (INTEGER, PRIMARY KEY AUTOINCREMENT)
- type_service (VARCHAR(10))
- nom_unite (VARCHAR(255))
- perimetre (VARCHAR(100))

### DIM_TEMPS
- annee (INTEGER) — Clé primaire

### FAITS_CRIMINELS
- id_fait (INTEGER, PRIMARY KEY AUTOINCREMENT)
- annee (INT, NOT NULL)
- nombre_faits (INT, DEFAULT 0)
- id_infraction (INT) — FK → DIM_INFRACTIONS(id_infraction)
- code_dept (VARCHAR(3)) — FK → DIM_DEPARTEMENTS(code_dept)
- id_service (INT) — FK → DIM_SERVICES(id_service)
- (FK annee → DIM_TEMPS(annee))

### STAT_POPULATION
- annee (INT)
- code_dept (VARCHAR(3))
- population (INT)
- PRIMARY KEY (annee, code_dept)
- FK code_dept → DIM_DEPARTEMENTS(code_dept)
- FK annee → DIM_TEMPS(annee)

---

## Remarques de correspondance
- Les noms de colonnes du MLD ont été alignés sur ceux du script SQL : `code_index` et `libelle_index` deviennent respectivement `id_infraction` et `libelle` dans `DIM_INFRACTIONS`.
- `DIM_DEPARTEMENTS` ne contient que `code_dept` comme PK (le `nom_dept` n'est pas présent dans le schéma SQL actuel).
- `DIM_SERVICES` utilise un `id_service` auto-incrémenté (AUTOINCREMENT) — le mapping entre les lignes source et cet ID doit être établi après insertion (lecture de la table).
- `STAT_POPULATION` a une clé primaire composite (`annee`, `code_dept`) conformément au SQL.

---

Si tu veux, je peux aussi :
- générer un diagramme ER à jour (image),
- vérifier que les fichiers CSV contiennent bien les colonnes renommées (ex: `code_index` / `libelle_index`), ou
- ajouter des contraintes d'unicité au SQL et aux imports pour éviter doublons.