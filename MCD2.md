```mermaid
erDiagram
    DIM_INFRACTIONS ||--o{ FAITS_CRIMINELS : "concerne"
    DIM_DEPARTEMENTS ||--o{ FAITS_CRIMINELS : "localise"
    DIM_SERVICES ||--o{ FAITS_CRIMINELS : "enregistre"
    DIM_TEMPS ||--o{ FAITS_CRIMINELS : "date"
    
    DIM_DEPARTEMENTS ||--o{ STAT_POPULATION : "concerne"
    DIM_TEMPS ||--o{ STAT_POPULATION : "mesure_en"

    DIM_INFRACTIONS {
        int id_infraction PK
        string libelle
    }

    DIM_DEPARTEMENTS {
        string code_dept PK
    }

    DIM_SERVICES {
        int id_service PK
        string type_service
        string nom_unite
        string perimetre
    }

    DIM_TEMPS {
        int annee PK
    }

    FAITS_CRIMINELS {
        int id_fait PK
        int annee FK
        int id_infraction FK
        string code_dept FK
        int id_service FK
        int nombre_faits
    }

    STAT_POPULATION {
        int annee PK, FK
        string code_dept PK, FK
        int population
    }

```

---

## Légende des Cardinalités

| Symbole | Signification | Explication dans ce projet |
| :--- | :--- | :--- |
| `\|\|--o{` | **Un vers Zéro ou Plusieurs** | Un département peut exister dans la table `DIM_DEPARTEMENTS` même s'il n'a aucun fait criminel enregistré pour une année donnée. |
| **PK** | **Primary Key** | Clé primaire unique identifiant chaque ligne de la table. |
| **FK** | **Foreign Key** | Clé étrangère permettant de lier une table de faits à une dimension. |
| **PK, FK** | **Clé Composée** | Dans `STAT_POPULATION`, l'identifiant unique est le couple (Année + Département). |
