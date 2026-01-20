-- 1. Dimension : Les types de crimes (Index 1 à 107)
CREATE TABLE DIM_INFRACTIONS (
    id_infraction INTEGER PRIMARY KEY,   -- Correspond à votre colonne 'code_index'
    libelle VARCHAR(255) NOT NULL    -- Correspond à 'libelle_index'
);

-- 2. Dimension : Les Départements
CREATE TABLE DIM_DEPARTEMENTS (
    code_dept VARCHAR(3) PRIMARY KEY -- Format '01', '2A', '974' (String important!)
    -- On pourra ajouter 'nom_dept' ici plus tard si besoin
);

-- 3. Dimension : Les Services (Unités de police/gendarmerie)
-- Utiliser AUTOINCREMENT compatible SQLite
CREATE TABLE DIM_SERVICES (
    id_service INTEGER PRIMARY KEY AUTOINCREMENT,
    type_service VARCHAR(10),        -- 'GN' ou 'PN'
    nom_unite VARCHAR(255),          -- Votre colonne 'CSP' (ex: CGD POINDIMIE)
    perimetre VARCHAR(100)           -- Votre colonne 'perimetre'
);

-- Table des années (dimension temps)
CREATE TABLE DIM_TEMPS (
    annee INTEGER PRIMARY KEY
);

-- 4. Table de Faits : Les Crimes enregistrés
CREATE TABLE FAITS_CRIMINELS (
    id_fait INTEGER PRIMARY KEY AUTOINCREMENT,
    annee INT NOT NULL,
    nombre_faits INT DEFAULT 0,
    
    -- Clés étrangères (Foreign Keys)
    id_infraction INT,
    code_dept VARCHAR(3),
    id_service INT,
    
    FOREIGN KEY (id_infraction) REFERENCES DIM_INFRACTIONS(id_infraction),
    FOREIGN KEY (code_dept) REFERENCES DIM_DEPARTEMENTS(code_dept),
    FOREIGN KEY (id_service) REFERENCES DIM_SERVICES(id_service),
    FOREIGN KEY (annee) REFERENCES DIM_TEMPS(annee)
);

-- 5. Table Contextuelle : Population par année
CREATE TABLE STAT_POPULATION (
    annee INT,
    code_dept VARCHAR(3),
    population INT,
    
    PRIMARY KEY (annee, code_dept),
    FOREIGN KEY (code_dept) REFERENCES DIM_DEPARTEMENTS(code_dept),
    FOREIGN KEY (annee) REFERENCES DIM_TEMPS(annee)
);