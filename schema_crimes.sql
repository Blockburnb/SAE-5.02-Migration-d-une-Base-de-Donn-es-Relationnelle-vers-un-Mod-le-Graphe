-- 1. Dimension : Les types de crimes
CREATE TABLE DIM_INFRACTIONS (
    id_infraction INTEGER PRIMARY KEY,
    libelle VARCHAR(255) NOT NULL
);

-- 2. Dimension : Les Départements
CREATE TABLE DIM_DEPARTEMENTS (
    code_dept VARCHAR(3) PRIMARY KEY
);

-- 3. Dimension : Les Services
CREATE TABLE DIM_SERVICES (
    id_service SERIAL PRIMARY KEY,
    type_service VARCHAR(10),
    nom_unite VARCHAR(255),
    perimetre VARCHAR(100)
);

-- 4. Table Temps
CREATE TABLE DIM_TEMPS (
    annee INTEGER PRIMARY KEY
);

-- 5. Table de Faits
CREATE TABLE FAITS_CRIMINELS (
    id_fait SERIAL PRIMARY KEY,
    annee INT NOT NULL,
    nombre_faits INT DEFAULT 0,

    id_infraction INT,
    code_dept VARCHAR(3),
    id_service INT,

    FOREIGN KEY (id_infraction) REFERENCES DIM_INFRACTIONS(id_infraction),
    FOREIGN KEY (code_dept) REFERENCES DIM_DEPARTEMENTS(code_dept),
    FOREIGN KEY (id_service) REFERENCES DIM_SERVICES(id_service),
    FOREIGN KEY (annee) REFERENCES DIM_TEMPS(annee)
);

-- 6. Population
CREATE TABLE STAT_POPULATION (
    annee INT,
    code_dept VARCHAR(3),
    population INT,

    PRIMARY KEY (annee, code_dept),
    FOREIGN KEY (code_dept) REFERENCES DIM_DEPARTEMENTS(code_dept),
    FOREIGN KEY (annee) REFERENCES DIM_TEMPS(annee)
);
