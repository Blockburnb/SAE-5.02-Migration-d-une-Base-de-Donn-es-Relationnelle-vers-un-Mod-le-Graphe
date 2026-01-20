# Modèle Logique de Données (MLD)

Ce modèle respecte une architecture en **Schéma en Étoile**, optimisée pour l'analyse multidimensionnelle des données de criminalité.

**Légende de la notation :**

* **NOM_TABLE** : Nom de la relation.
* **Clé_Primaire** (en gras) : Identifiant unique de la ligne.
* *#Clé_Etrangère* (en italique précédé d'un #) : Référence vers une autre table.

---

### 1. Les Dimensions (Axes d'analyse)

Ces tables contiennent les données descriptives (le "Qui", "Où", "Quand").

* **DIM_INFRACTIONS** (**code_index**, libelle_index)
* **DIM_DEPARTEMENTS** (**code_dept**, nom_dept)
* **DIM_SERVICES** (**id_service**, nom_unite, type_service, perimetre)
* **DIM_TEMPS** (**annee**)

### 2. La Table de Faits (Cœur du modèle)

Cette table centrale contient les métriques et les liens vers les dimensions.

* **FAITS_CRIMINELS** (**id_fait**, nombre_faits, *#code_index*, *#id_service*, *#code_dept*, *#annee*)

### 3. Table Contextuelle

Données démographiques utilisées pour le calcul de ratios (ex: crimes pour 1000 habitants).

* **STAT_POPULATION** (**#code_dept**, **#annee**, population)
* *Note : La clé primaire est composite (Département + Année).*



---

### Dictionnaire des Données Simplifié

| Table | Attribut | Type | Description |
| --- | --- | --- | --- |
| **DIM_INFRACTIONS** | `code_index` | INT | Index officiel (1 à 107) du type de crime. |
|  | `libelle_index` | VARCHAR | Nom complet de l'infraction (ex: "Vols à main armée"). |
| **DIM_DEPARTEMENTS** | `code_dept` | VARCHAR(3) | Code officiel (ex: '01', '2A', '974'). |
| **DIM_SERVICES** | `id_service` | INT | Identifiant technique auto-incrémenté. |
|  | `nom_unite` | VARCHAR | Nom de la brigade ou du commissariat (CSP). |
|  | `type_service` | VARCHAR(2) | 'GN' (Gendarmerie) ou 'PN' (Police). |
| **FAITS_CRIMINELS** | `nombre_faits` | INT | Quantité d'infractions constatées. |
| **STAT_POPULATION** | `population` | INT | Nombre d'habitants pour l'année et le département donnés. |