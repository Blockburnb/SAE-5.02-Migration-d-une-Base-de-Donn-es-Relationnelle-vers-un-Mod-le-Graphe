# Rapport de Migration : Analyse Graphe des Crimes et Délits (2012-2021)

**Commanditaire :** Ministère de l’Intérieur (DGDSN)  
**Date :** 2024  
**Technologie :** Neo4j v1.5.9  
**Auteurs :** Franklin BECK, Leon VINCENT-VACLE, Lucas COROMPT

---

## 1. Inventaire des Ressources et Données Sources

L'étude repose sur l'exploitation des statistiques officielles de la délinquance enregistrée en France sur une décennie (2012-2021). Ce dataset constitue une base de connaissances massive permettant de retracer l'activité judiciaire des services de sécurité intérieure.

### 1.1 Analyse du Dataset et Méthodologie (Source : DCPJ)
D'après la documentation technique fournie par la Direction Centrale de la Police Judiciaire, les données répondent aux standards suivants :

* **Distinction territoriale PN / GN :**
    * **Police Nationale (PN) :** Intervient majoritairement dans les Circonscriptions de Sécurité Publique (CSP) couvrant les zones urbaines et les grandes agglomérations.
    * **Gendarmerie Nationale (GN) :** Intervient via les Compagnies de Gendarmerie Départementale (CGD) sur les zones rurales et périurbaines.
    * *Conséquence pour le projet :* Cette segmentation offre une opportunité d'analyse croisée entre typologie de territoire et types d'infractions.

### 1.2 Indicateurs Techniques Exploités
* **Dimensions temporelles :** 20 fichiers sources (10 pour la GN, 10 pour la PN) couvrant la période 2012-2021.
* **Entités géographiques :** Répartition par départements (codes et libellés).
* **Volume d'activité :** Nombre de faits constatés par index de criminalité.

### 1.3 Enrichissement
Pour transformer ce dataset tabulaire en un véritable outil d'intelligence territoriale, nous avons enrichi le modèle avec des sources de données externes :

1.  **Adjacences Géographiques (Topologie) :** Intégration des relations de voisinage entre départements. Cette donnée est cruciale pour le modèle graphe afin de détecter des phénomènes de propagation de la délinquance entre territoires limitrophes.
2.  **Données Démographiques (INSEE) :** Ajout de la population par département pour transformer les volumes bruts en **taux de criminalité** (nombre de faits pour 1000 habitants).
3.  **Indicateurs Socio-économiques :** Intégration facultative du revenu médian par zone pour permettre des analyses de corrélation entre contexte économique et types de délits enregistrés.

---

## 2. Processus ETL (Extract, Transform, Load)
Avant la migration vers Neo4j, une phase d'ETL est indispensable pour nettoyer et structurer la donnée.

### 2.1 Nettoyage et Normalisation
Nous utilisons un script Python (Pandas) pour :
1. **Fusionner** les 20 fichiers en un référentiel unique.
2. **Normaliser** les noms de colonnes (suppression des espaces et caractères spéciaux).
3. **Dédoublonner** les types d'infractions.


### 2.2 Chargement en Base Relationnelle (Pivot)
Avant le graphe, les données sont stockées dans un modèle relationnel classique (SQL) pour servir de point de comparaison.

---

## 3. Migration vers le Modèle Graphe (Neo4j)
La migration n'est pas une simple copie, c'est une **transformation structurelle**.

### 3.1 Conception du Schéma de Nœuds et Relations
Contrairement au SQL, nous modélisons les entités comme des objets interconnectés.

**Les Nœuds (Entités) :**
* `:TypeCrime` (Ex: Vols, Violences).
* `:Departement` (Ex: 75, 13).
* `:Service` (Police ou Gendarmerie).
* `:Annee` (2012...2021).

**Les Relations (Actions) :**
* `(Service)-[:ENREGISTRE {quantite: Int}]->(TypeCrime)`
* `(Service)-[:EST_RATTACHE_A]->(Departement)`
* `(Departement)-[:EST_LIMITROPHE_DE]->(Departement)`



### 3.2 Stratégie de Migration Technique
La migration est effectuée via la commande `LOAD CSV` de Neo4j ou un driver Python.

**Logique de migration :**
1. Création des contraintes d'unicité (ID crime, code département).
2. Importation des nœuds maîtres (Départements et Crimes).
3. Création des relations avec propriétés (le nombre de faits est stocké directement sur le lien entre le service et le crime).

```cypher
import os
import pandas as pd

INFILE = 'DS_ESTIMATION_POPULATION_data.csv'
AGG_OUT = 'population_by_dept_year.csv'

def read_raw(path):
    # lire en forçant le séparateur ';' et le quotechar '"'
    df = pd.read_csv(path, sep=';', quotechar='"', engine='python', dtype=str)
    return df


def clean_headers(df):
    # noms lisibles
    col_map = {
        'AGE': 'age_group',
        'EP_MEASURE': 'measure',
        'FREQ': 'freq',
        'GEO': 'geo',
        'GEO_OBJECT': 'geo_object',
        'REF_PERIOD': 'ref_period',
        'SEASONAL_ADJUST': 'seasonal_adjust',
        'SEX': 'sex',
        'UNIT_MEASURE': 'unit',
        'DECIMALS': 'decimals',
        'OBS_STATUS': 'obs_status',
        'OBS_STATUS_FR': 'obs_status_fr',
        'UNIT_MULT': 'unit_mult',
        'TIME_PERIOD': 'year',
        'OBS_VALUE': 'value'
    }
    # nettoyer espaces autour des noms de colonnes
    df = df.rename(columns=lambda c: c.strip())
    # appliquer mapping si possible
    for k, v in col_map.items():
        if k in df.columns:
            df = df.rename(columns={k: v})
    return df


def write_clean(df, out):
    df.to_csv(out, index=False, encoding='utf-8')
    print(f"Fichier nettoyé écrit: {out} (lignes: {len(df)})")


def aggregate_dept_year(df):
    # filtrer les lignes de niveau département
    df_dep = df[df['geo_object'] == 'DEP'].copy()
    if df_dep.empty:
        raise RuntimeError('Aucune ligne départementale trouvée (geo_object != DEP)')
    # convertir types
    df_dep['year'] = pd.to_numeric(df_dep['year'], errors='coerce').astype('Int64')
    df_dep['value'] = pd.to_numeric(df_dep['value'].str.replace(',', '.'), errors='coerce').fillna(0)
    # pivot par sex
    pivot = df_dep.pivot_table(index=['geo', 'year'], columns='sex', values='value', aggfunc='sum', fill_value=0)
    pivot = pivot.reset_index()
    # calculer population : priorité à _T si présent, sinon M+F
    def compute_pop(row):
        if '_T' in row.index and pd.notna(row.get('_T')) and row.get('_T') > 0:
            return int(round(row.get('_T')))
        m = row.get('M', 0) if 'M' in row.index else 0
        f = row.get('F', 0) if 'F' in row.index else 0
        # d'autres sexes possibles : sommation de toutes colonnes non geo/year
        other = 0
        for k in row.index:
            if k not in ('geo', 'year', 'M', 'F', '_T'):
                try:
                    other += float(row.get(k) or 0)
                except Exception:
                    pass
        total = m + f + other
        return int(round(total))
    pivot['population'] = pivot.apply(compute_pop, axis=1)
    out = pivot[['geo', 'year', 'population']].rename(columns={'geo': 'departement', 'year': 'annee'})
    # nettoyer departement codes : garder chaînes (ex '01','2A')
    out['departement'] = out['departement'].astype(str).str.strip()
    out['annee'] = out['annee'].astype(int)
    out['population'] = out['population'].astype(int)
    return out


def main():
    if not os.path.exists(INFILE):
        print(f"Fichier introuvable: {INFILE}")
        return 1
    df = read_raw(INFILE)
    df = clean_headers(df)
    agg = aggregate_dept_year(df)
    agg.to_csv(AGG_OUT, index=False, encoding='utf-8')
    print(f"Agrégation département x année écrite: {AGG_OUT} (lignes: {len(agg)})")
    # afficher quelques lignes d'exemple
    print('\nExemples (premières lignes):')
    print(agg.head(10).to_string(index=False))
    return 0

if __name__ == '__main__':
    raise SystemExit(main())
```

## 4. Phase 3 : Processus de Migration vers Neo4j
Cette phase est le cœur technique du projet. Elle consiste à transformer des lignes de données tabulaires en un réseau de connaissances interconnecté.

### 4.1 Stratégie de Transformation
La migration ne se contente pas de copier les données ; elle les restructure selon la logique **"Whiteboard Design"** (modélisation directe des relations métier).
* **Extraction :** Récupération des données nettoyées depuis le dossier `data/processed/`.
* **Mapping :** Chaque ligne du CSV devient une relation `:ENREGISTRE` entre un nœud `:Service` et un nœud `:TypeCrime`.
* **Enrichissement :** Injection des données d'adjacence pour lier les nœuds `:Departement` entre eux via la relation `:EST_LIMITROPHE`.

### 4.2 Script de Migration (Cypher & LOAD CSV)
Pour garantir l'efficacité, nous utilisons la version stable **Neo4j 1.5.9** (ou compatible) avec des contraintes d'unicité pour éviter les doublons.

```cypher
// 1. Création des contraintes pour la performance
CREATE CONSTRAINT ON (d:Departement) ASSERT d.code IS UNIQUE;
CREATE CONSTRAINT ON (c:TypeCrime) ASSERT c.nom IS UNIQUE;

// 2. Migration des données de crimes
LOAD CSV WITH HEADERS FROM 'file:///synthese_crimes_2012_2021.csv' AS row
MERGE (dep:Departement {code: row.code_dept})
MERGE (crime:TypeCrime {nom: row.libelle_crime})
CREATE (s:Service {type: row.type_service, nom: row.nom_service})
CREATE (s)-[:EFFECTUE {annee: toInteger(row.annee), quantite: toInteger(row.valeur)}]->(crime)
CREATE (s)-[:RATTACHE_A]->(dep);
```

## 5. Phase 5 : Rédaction et Présentation du Rapport Final

Cette phase synthétise la méthodologie globale de migration et définit les protocoles de maintenance pour garantir la pérennité de la solution.

### 5.1 Méthodologies de Migration de Données
Dans le cadre de ce projet, trois méthodes de migration ont été étudiées pour passer du modèle relationnel (SQL) vers Neo4j :

1.  **Migration par ETL (Extract-Transform-Load) :** * *Processus :* Utilisation de scripts Python (Pandas) pour transformer les fichiers CSV en un format "nœuds et relations" avant l'injection.
    * *Avantage :* Permet un nettoyage complexe et une normalisation des données de la nomenclature État 4001 avant import.
2.  **Migration Directe (LOAD CSV) :** * *Processus :* Utilisation directe du moteur Cypher pour lire les fichiers CSV et créer les structures en une seule passe.
    * *Avantage :* Rapidité d'exécution et simplicité pour des jeux de données structurés.
3.  **Migration par Connecteurs (Neo4j ETL Tool) :**
    * *Processus :* Connexion directe entre une base SQL (PostgreSQL/MySQL) et Neo4j.
    * *Avantage :* Automatisation du mapping des clés étrangères en relations.

### 5.2 Section : Ajout de Nouvelles Données (Maintenance)
L'évolutivité est un critère majeur pour le Ministère. Voici comment le système gère l'arrivée des données de l'année 2022 et au-delà :

#### A. Dans la Base de Données Relationnelle
* **Procédure :** Nécessite l'insertion de nouvelles lignes dans la table des faits. Si une nouvelle catégorie d'infraction apparaît dans l'État 4001, il faut d'abord mettre à jour la table de référence (`Ref_Crime`) pour respecter les contraintes d'intégrité (clés étrangères).
* **Contrainte :** Schéma rigide. L'ajout d'une nouvelle dimension (ex: coordonnées GPS exactes) impose une modification de la structure de la table (`ALTER TABLE`).

#### B. Dans la Base de Données Graphe (Neo4j)
* **Procédure :** Utilisation du `MERGE` en Cypher. Si le département ou le crime existe déjà, Neo4j crée simplement une nouvelle relation `:ENREGISTRE` avec la propriété `annee: 2022`.
* **Avantage :** Schéma flexible (Schema-free). On peut ajouter des propriétés à la volée (ex: ajouter le nom du préfet sur un nœud `:Departement`) sans impacter les données existantes ni arrêter le service.

### 5.3 Synthèse des Résultats et Recommandations
L'analyse comparative démontre que le modèle Graphe est supérieur pour :
* **Le croisement PN/GN :** Visualisation immédiate de la répartition des efforts de sécurité sur un même territoire.
* **L'analyse territoriale :** Grâce aux relations d'adjacence, le Ministère peut passer d'une analyse statistique "en silo" à une analyse de réseau géographique.

**Recommandations :**
1.  **Enrichissement continu :** Intégrer les données de la Justice pour corréler les crimes enregistrés avec les condamnations réelles.
2.  **Visualisation :** Déployer **Neo4j Bloom** pour les analystes de la DGDSN afin de leur permettre d'explorer le graphe sans connaître le langage Cypher.
3.  **Performance :** Maintenir les contraintes d'unicité sur les codes index 4001 pour garantir des temps de réponse inférieurs à 100ms sur des requêtes nationales.

---

## Conclusion Générale
La migration vers un modèle graphe ne constitue pas seulement un changement technique, mais une évolution stratégique. Elle permet au Ministère de l'Intérieur de passer d'une base de données statique à une **Base de Connaissances Active**, capable de révéler des patterns criminels invisibles dans une structure relationnelle classique.