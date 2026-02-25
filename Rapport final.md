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

### 1.3 Présentation du jeu de données démographiques (INSEE)
Pour affiner l'analyse territoriale, nous avons intégré un référentiel de population issu des estimations de l'INSEE. Ce jeu de données permet de contextualiser la délinquance par rapport à la densité démographique.

* **Fichier source :** `population_by_dept_year.csv`
* **Contenu technique :**
    * **Dimensions Géographiques :** Codes et libellés officiels des départements (ex: `91` pour l'Essonne, `974` pour La Réunion).
    * **Granularité par Âge :** Les données sont segmentées par tranches d'âge (ex: `Y15T19` pour les 15-19 ans), ce qui permettrait, à terme, d'analyser la criminalité au regard de la pyramide des âges locale.
    * **Indicateurs de Genre :** Distinction Hommes/Femmes pour des analyses démographiques croisées.

**Valeur ajoutée pour le projet :**
L'intégration de ces métadonnées dans Neo4j permet de créer des propriétés `population` sur nos nœuds `:Departement`. Cela rend possible le calcul dynamique du **taux de victimation** directement en langage Cypher, offrant ainsi une vision bien plus précise de la réalité de l'insécurité sur le terrain.

---

## 2. Processus ETL (Extract, Transform, Load) et Modélisation

Le passage de fichiers sources hétérogènes (Excel, CSV, Metadata) à une base de connaissances exploitable nécessite une phase d'ETL rigoureuse pour garantir la qualité des analyses futures.

### 2.1 Nettoyage et Normalisation des Données
Nous utilisons un script **Python (Pandas)** pour transformer la donnée brute en donnée structurée :

1.  **Réunification du socle sécuritaire :** Fusion des 20 fichiers annuels (PN et GN) en une table unique, en alignant les index de la nomenclature **État 4001**.
2.  **Traitement des métadonnées démographiques :** Nettoyage du fichier `DS_ESTIMATION_POPULATION_metadata.csv` pour extraire les populations par département.
3.  **Normalisation technique :** * Suppression des caractères spéciaux et uniformisation des noms de colonnes.
    * Gestion des types : conversion des volumes de faits et des chiffres de population en entiers (`int`).
    * Alignement des codes géographiques (ex: traitement du code `2A/2B` pour la Corse et des codes Outre-mer).

### 2.2 Cahier des Charges du Modèle Conceptuel (MCD)
Le modèle doit permettre de répondre aux exigences métiers de la DGDSN :
* **Axe Temporel :** Comparer l'évolution de la délinquance sur 10 ans.
* **Axe Territorial :** Ventiler les crimes par département et par type de zone (urbaine pour la PN, rurale pour la GN).
* **Axe de Performance :** Calculer des taux de criminalité grâce au croisement avec les données de population INSEE.


![Modèle Conceptuel des Données - Analyse Crimes](./MCD.png)
*Figure 1 : Modèle Conceptuel des Données (MCD) intégrant les statistiques 4001 et les données INSEE.*

### 2.3 Modèle Logique de Données (MLD)
Le passage du conceptuel au logique se traduit par une architecture en **schéma en étoile**. Cette structure est conçue pour isoler les indicateurs chiffrés (les faits) des axes d'analyse (les dimensions).

#### Structure des Tables
* **Table de Faits (`FAITS_CRIMINELS`)** : Contient les volumes de crimes, liée par clés étrangères aux dimensions.
* **Dimensions (`DIM_INFRACTIONS`, `DIM_DEPARTEMENTS`, `DIM_SERVICES`, `DIM_TEMPS`)** : Stockent les libellés et les caractéristiques fixes (ex: nom du département, type de service PN/GN).
* **Table Contextuelle (`STAT_POPULATION`)** : Stocke les estimations de population INSEE par département et par année pour permettre le calcul de ratios de victimation.

![Schéma Logique de Données en Étoile - Architecture Relationnelle](lien_vers_votre_capture_MLD.png)

*Figure 2 : Traduction logique du modèle en schéma en étoile.*

### 2.4 Chargement en Base Relationnelle (Pivot)
Avant la migration vers le graphe, les données sont injectées dans une base SQL (SGBDR) pour servir de socle de référence. Ce modèle pivot est structuré comme suit :

* **Table `Services` :** Stocke le type de service (Police/Gendarmerie) et son rattachement géographique.
* **Table `Geographie` :** Centralise les noms de départements et leur **population totale** issue du fichier INSEE.
* **Table `Nomenclature_4001` :** Référentiel des libellés d'infractions.
* **Table `Faits_Criminels` :** Table de faits contenant les mesures (Quantité, Année) liant les services aux types de crimes.

### 2.4 Analyse des limites du modèle Relationnel
Bien que robuste pour le stockage, ce modèle montre des limites pour les analyses complexes demandées :
* **Jointures coûteuses :** Calculer des corrélations entre population, chômage et crimes sur plusieurs départements voisins sature les ressources.
* **Rigidité :** L'ajout de nouvelles dimensions géographiques (adjacences) complexifie inutilement le schéma SQL.

---

## 3. Migration vers le Modèle Graphe (Neo4j)
Nous faisons une restructuration du modèle logique relationnel vers un modèle orienté graphe.

Contrairement à une base SQL où les relations sont implicites via des clés étrangères, Neo4j matérialise explicitement ces relations sous forme d’arêtes.

### 3.1 Conception du Schéma de Nœuds et Relations
Le modèle retenu repose sur la transformation du schéma en étoile relationnel vers un modèle centré sur l’entité métier `Fait`.

![Exemple de requête Graph](./Modèle_graph.png)

**Les Nœuds :**
* `:Infraction` (Ex: Vols, Violences).
* `:Departement` (Ex: 75, 13).
* `:Service` (Police ou Gendarmerie).
* `:Annee` (2012...2021).
* `:Fait` (nombre de faits d’une infraction donnée, constatés par un service, dans un département et une année donnée.)

**Les Relations :**
* `(Departement)-[:HAS_FACT]->(Fait)`
* `(Fait)-[:OF_INFRACTION]->(Infraction)`
* `(Fait)-[:BY_SERVICE]->(Service)`
* `(Fait)-[:IN_YEAR]->(Annee)`

### 3.2 Justification du choix du nœud Fait
Deux approches étaient possibles :
1. Stocker nombre_faits comme propriété sur une relation.
2. Créer un nœud intermédiaire Fait.

Nous avons retenu la seconde option pour :

* Conserver la granularité temporelle
* Faciliter les agrégations
* Permettre des extensions futures (ex : ajout d’un taux, indicateurs contextuels)
* Éviter des relations multi-propriétés complexes

Ce choix permet une meilleure lisibilité et une évolutivité accrue du modèle.
### 3.3 Stratégie de Migration Technique
La migration a été réalisée via un script Python utilisant :

* psycopg2 pour l’extraction depuis PostgreSQL
* neo4j-driver pour l’injection via Bolt

Le processus s’est déroulé en plusieurs étapes :

1. Création des contraintes d’unicité
2. Import des dimensions (Départements, Infractions, Services, Années)
3. Création des nœuds Fait
4. Création des relations via MERGE

Exemple d’instruction Cypher utilisée :

```cypher
MERGE (f:Fait {id_fait: row.id_fait})
SET f.nombre_faits = row.nombre_faits
MERGE (d)-[:HAS_FACT]->(f)
MERGE (f)-[:OF_INFRACTION]->(i)
MERGE (f)-[:BY_SERVICE]->(s)
MERGE (f)-[:IN_YEAR]->(t)
```
![Exemple de requête Graph](./exemple_requete.png)

## 4. Validation et Vérification de Cohérence

Après migration, plusieurs contrôles ont été effectués :

### 4.1 Vérification des volumes

```cypher
MATCH (f:Fait) RETURN count(f);
```
Comparaison avec :

```SQL
SELECT COUNT(*) FROM faits_criminels;
```
Résultat : `736 267`

Les volumes sont identiques, confirmant la complétude de la migration.

### 4.2 Vérification des faits orphelins

```cypher
MATCH (f:Fait)
WHERE NOT ( (:Departement)-[:HAS_FACT]->(f) )
RETURN count(f);
```
Résultat : `0`

### 4.3 Test des requêtes métiers

Exemple : 

```cypher
MATCH (d:Departement)-[:HAS_FACT]->(f:Fait)-[:OF_INFRACTION]->(i:Infraction)
WITH d.code_dept AS departement,
     i.libelle AS infraction,
     SUM(f.nombre_faits) AS total
RETURN departement, infraction, total
ORDER BY total DESC;
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