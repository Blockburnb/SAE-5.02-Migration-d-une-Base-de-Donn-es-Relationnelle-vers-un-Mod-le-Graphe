# SAE-5.02 — Migration d'une base relationnelle vers un modèle graphe

## Commanditaire
Ministère de l’Intérieur — Direction Générale des Données et de la Sécurité Numérique

## Titre du projet
Migration d'une base de données relationnelle vers un modèle graphe pour l’analyse des crimes et délits (2012–2021)

---

## 1. Contexte général
Dans le cadre de la modernisation des outils d’analyse des données relatives aux crimes et délits enregistrés par la Police et la Gendarmerie nationales, ce projet vise à améliorer l’exploitation des données. Les bases relationnelles peuvent montrer des limites pour certaines analyses complexes ; l’usage d’un SGBD orienté graphe (ex. Neo4j) est envisagé pour répondre à ces besoins.

Ce cahier des charges définit les objectifs, les phases et les livrables attendus pour mener la migration.

## 2. Objectifs
- Optimiser l’analyse des données de 2012 à 2021.
- Proposer un modèle graphe facilitant les analyses avancées (relations entre départements, brigades, types de crimes, etc.).
- Enrichir les données avec des sources publiques (par ex. adjacences des communes).
- Assurer une migration claire et reproductible depuis le modèle relationnel vers Neo4j.
- Documenter les étapes et fournir des scripts/outils exploitables.

## 3. Description des travaux (phases)

### Phase 1 — Analyse des sources et modélisation
- Étudier le fichier Excel fourni (statistiques par année, PN et GN).
- Identifier les entités et relations principales.
- Produire un MCD (modèle conceptuel de données) et un modèle logique relationnel.
- Implémenter une base relationnelle (SGBD au choix) et l’alimenter depuis le fichier.

### Phase 2 — Analyse des limites du modèle relationnel
- Évaluer performances, lisibilité et complexité des requêtes.
- Proposer un modèle graphe : définir les nœuds (départements, régions, brigades, types d’infractions, années, etc.) et les relations.

### Phase 3 — Migration vers le modèle graphe
- Développer des scripts de transformation (Python ou autre) pour migrer les données vers Neo4j.
- Outil cible : Neo4j (version stable recommandée indiquée dans le projet).
- Justifier les choix de transformation et les relations créées.

### Phase 4 — Validation et exploitation
- Vérifier la cohérence et l’exactitude des données migrées.
- Tester des requêtes métiers (ex. : types de crimes les plus fréquents par département, connexions brigades-départements).
- Comparer les performances relationnelle vs graphe (optionnel / bonus).

### Phase 5 — Rédaction et présentation
- Documenter l’intégralité du travail (analyse, conception, migration, validation).
- Préparer une présentation synthétique et, si possible, une vidéo démonstrative.

## 4. Livrables attendus
- Base relationnelle :
  - MCD et modèle logique.
  - Script SQL de création et d’alimentation.
- Base graphe :
  - Schéma des nœuds et des relations.
  - Scripts de migration (ex. Python, Cypher).
- Rapport final :
  - Documentation complète, comparaisons, exemples de requêtes et résultats.
  - Présentation synthétique (+ vidéo si disponible).

## 5. Méthodologie et contraintes
- Fournir des scripts reproductibles et commentés.
- Prévoir une section sur l’alimentation incrémentale (ajout de nouvelles données) pour le modèle relationnel et le modèle graphe.
- Respecter les bonnes pratiques de nommage et de normalisation des données.

## 6. Planning et remise
- Date de remise : à préciser (TBD).
- Prévoir des jalons intermédiaires pour validation (analyse, prototype graphe, tests).

## 7. Fichiers fournis
- `crimes-et-delits-enregistres-par-les-services-de-gendarmerie-et-de-police-depuis-2012.xlsx` — données sources

---

Pour toute question ou précision, indiquer le point à clarifier dans le dépôt ou contacter le commanditaire.
