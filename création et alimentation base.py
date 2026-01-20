import pandas as pd
import sqlite3
import os

# 1. CONFIGURATION & CONNEXION
# ---------------------------------------------------------
db_name = "crimes_database.db"
# Si le fichier DB existe déjà, on le supprime pour repartir à zéro (optionnel)
if os.path.exists(db_name):
    os.remove(db_name)

# On crée la connexion (cela crée le fichier vide si inexistant)
conn = sqlite3.connect(db_name)
cursor = conn.cursor()

# On active les clés étrangères pour SQLite
cursor.execute("PRAGMA foreign_keys = ON;")

print("--- 1. Création de la structure (Tables) ---")
# On lit votre fichier SQL (assurez-vous d'avoir enregistré le code SQL précédent dans schema.sql)
with open('schema_crimes.sql', 'r', encoding='utf-8') as f:
    sql_script = f.read()
cursor.executescript(sql_script)
conn.commit()
print("Tables créées avec succès.")

# 2. CHARGEMENT DES DONNÉES CSV
# ---------------------------------------------------------
print("--- 2. Lecture des fichiers CSV ---")
# Lire en forçant departement en str pour éviter conversions indésirables
df_crimes = pd.read_csv('crimes_clean_2012_2021.csv', dtype={'departement': str})
df_pop = pd.read_csv('population_by_dept_year.csv', dtype={'departement': str})

# Petits nettoyages de sûreté (targeted, ne pas écraser colonnes textes)
# Departement : garder format '01','2A', etc.
df_crimes['departement'] = df_crimes['departement'].astype(str).str.zfill(2)
# Annees en int (coerce en cas de mauvaise valeur)
df_crimes['annee'] = pd.to_numeric(df_crimes['annee'], errors='coerce').astype('Int64')
# Nombre de faits : normaliser en integer, remplacer NA par 0
if 'nombre_faits' in df_crimes.columns:
    df_crimes['nombre_faits'] = pd.to_numeric(df_crimes['nombre_faits'], errors='coerce').fillna(0).astype(int)
else:
    raise KeyError("La colonne 'nombre_faits' est introuvable dans crimes_clean_2012_2021.csv")

# Préparer colonnes services pour merge (normaliser NA -> empty string pour matching)
for c in ['service', 'CSP', 'perimetre']:
    if c in df_crimes.columns:
        df_crimes[c] = df_crimes[c].fillna('').astype(str)
    else:
        # créer colonne vide si manquante pour éviter KeyError plus loin
        df_crimes[c] = ''

# Nettoyage population
if 'departement' in df_pop.columns:
    df_pop['departement'] = df_pop['departement'].astype(str).str.zfill(2)
else:
    raise KeyError("La colonne 'departement' est introuvable dans population_by_dept_year.csv")
if 'annee' in df_pop.columns:
    df_pop['annee'] = pd.to_numeric(df_pop['annee'], errors='coerce').astype('Int64')
else:
    raise KeyError("La colonne 'annee' est introuvable dans population_by_dept_year.csv")
if 'population' in df_pop.columns:
    df_pop['population'] = pd.to_numeric(df_pop['population'], errors='coerce').fillna(0).astype(int)
else:
    raise KeyError("La colonne 'population' est introuvable dans population_by_dept_year.csv")


# 3. ALIMENTATION DES DIMENSIONS
# ---------------------------------------------------------
print("--- 3. Alimentation des Dimensions ---")

# A. DIM_INFRACTIONS
# On prend les codes et libellés uniques
if not {'code_index', 'libelle_index'}.issubset(df_crimes.columns):
    raise KeyError("Les colonnes 'code_index' et/ou 'libelle_index' sont absentes de df_crimes")

dim_infractions = df_crimes[['code_index', 'libelle_index']].drop_duplicates().rename(columns={'code_index': 'id_infraction', 'libelle_index': 'libelle'})
# Ecrire en utilisant INSERT OR IGNORE pour éviter erreur UNIQUE si doublons
# Préparer les tuples (id_infraction, libelle)
records = [(r['id_infraction'], r['libelle']) for r in dim_infractions.to_dict(orient='records')]
cursor.executemany("INSERT OR IGNORE INTO DIM_INFRACTIONS (id_infraction, libelle) VALUES (?, ?)", records)
conn.commit()
print(f"-> {len(dim_infractions)} infractions (tentatives d'insertion) traitées.")

# B. DIM_DEPARTEMENTS
# On prend tous les départements uniques des deux fichiers (crimes et pop)
depts_crimes = df_crimes['departement'].astype(str).unique()
depts_pop = df_pop['departement'].astype(str).unique()
all_depts = pd.unique(pd.concat([pd.Series(depts_crimes), pd.Series(depts_pop)]))
# Schema currently defines only code_dept, donc n'envoyer que cette colonne
# dim_dept = pd.DataFrame({'code_dept': all_depts, 'nom_dept': 'Inconnu'}) # ancien
dim_dept = pd.DataFrame({'code_dept': all_depts})
dim_dept.to_sql('DIM_DEPARTEMENTS', conn, if_exists='append', index=False)
print(f"-> {len(dim_dept)} départements insérés.")

# C. DIM_SERVICES
# C'est plus complexe car l'ID est auto-généré par la base.
# 1. On extrait les uniques
cols_service = ['service', 'CSP', 'perimetre']
dim_services_source = df_crimes[cols_service].drop_duplicates().copy()
# Normaliser colonnes et renommer pour matcher la base SQL
dim_services_source.columns = ['type_service', 'nom_unite', 'perimetre']
# Insérer (id_service auto)
dim_services_source.to_sql('DIM_SERVICES', conn, if_exists='append', index=False)
print(f"-> {len(dim_services_source)} services insérés.")

# 3. CRUCIAL : On relit la table pour récupérer les IDs générés !
# On a besoin d'une map : { (type, nom, perimetre) -> id_service }
df_services_db = pd.read_sql("SELECT * FROM DIM_SERVICES", conn)
print("-> Mapping des services récupéré.")


# D. DIM_TEMPS
# On extrait toutes les années uniques
annees = pd.unique(df_crimes['annee'].dropna())
dim_temps = pd.DataFrame({'annee': annees})
dim_temps.to_sql('DIM_TEMPS', conn, if_exists='append', index=False)
print(f"-> {len(dim_temps)} années insérées.")


# 4. ALIMENTATION DE LA TABLE DE FAITS
# ---------------------------------------------------------
print("--- 4. Alimentation de la Table de Faits (Patience...) ---")

# On doit remplacer les infos textes du DataFrame principal par les IDs

# 1. Mapping Service : On fait un MERGE (jointure) entre le gros fichier et la table des services récupérée
# On fusionne sur les colonnes communes (type, nom, perimetre)
# Attention : dans df_crimes c'est 'service', 'CSP', 'perimetre'
# Dans df_services_db c'est 'type_service', 'nom_unite', 'perimetre'
# S'assurer que les types sont comparables
for c in ['service', 'CSP', 'perimetre']:
    df_crimes[c] = df_crimes[c].astype(str)
for c in ['type_service', 'nom_unite', 'perimetre']:
    if c in df_services_db.columns:
        df_services_db[c] = df_services_db[c].astype(str)

# Merge
df_merged = pd.merge(
    df_crimes,
    df_services_db,
    left_on=['service', 'CSP', 'perimetre'],
    right_on=['type_service', 'nom_unite', 'perimetre'],
    how='left',
    validate='m:1'  # many crimes -> one service
)

# Rapporter les services non appariés
missing_services = df_merged['id_service'].isna().sum()
print(f"-> Services non appariés (après merge): {missing_services}")
if missing_services > 0:
    print(df_merged.loc[df_merged['id_service'].isna(), ['service', 'CSP', 'perimetre']].drop_duplicates().head(10))

# 2. Préparation finale
required_cols = ['annee', 'nombre_faits', 'code_index', 'id_service', 'departement']
for c in required_cols:
    if c not in df_merged.columns:
        raise KeyError(f"Colonne requise '{c}' manquante après merge")

df_faits = df_merged[[
    'annee',
    'nombre_faits',
    'code_index',   # Deviendra id_infraction
    'id_service',   # L'ID qu'on vient de récupérer
    'departement'   # Deviendra code_dept
]].copy()

# Renommer pour matcher exactement la table SQL
df_faits.columns = ['annee', 'nombre_faits', 'id_infraction', 'id_service', 'code_dept']

# Forcer types simples
df_faits['annee'] = pd.to_numeric(df_faits['annee'], errors='coerce').astype('Int64')
if df_faits['id_service'].dtype == 'object':
    # attempt to cast to numeric where possible
    df_faits['id_service'] = pd.to_numeric(df_faits['id_service'], errors='coerce').astype('Int64')

# 3. Insertion en masse (avec chunksize)
df_faits.to_sql('FAITS_CRIMINELS', conn, if_exists='append', index=False, chunksize=10000)
print(f"-> {len(df_faits)} faits criminels insérés avec succès !")


# 5. ALIMENTATION POPULATION
# ---------------------------------------------------------
print("--- 5. Alimentation Population ---")
df_pop_clean = df_pop[['departement', 'annee', 'population']].copy()
df_pop_clean['departement'] = df_pop_clean['departement'].astype(str).str.zfill(2) # S'assurer du format
df_pop_clean.columns = ['code_dept', 'annee', 'population']

# Filtrer pour garder seulement les années qui existent dans DIM_TEMPS (contrainte FK)
df_pop_clean = df_pop_clean[df_pop_clean['annee'].isin(annees)]

# Insérer par chunks
df_pop_clean.to_sql('STAT_POPULATION', conn, if_exists='append', index=False, chunksize=10000)
print("-> Population insérée.")

conn.close()
print("--- TERMINE : Base de données prête ---")
