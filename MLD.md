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
