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
df_crimes = pd.read_csv('crimes_clean_2012_2021.csv')
df_pop = pd.read_csv('population_by_dept_year.csv')

# Petits nettoyages de sûreté
df_crimes['departement'] = df_crimes['departement'].astype(str).str.zfill(2) # Force '01' au lieu de '1'
df_crimes = df_crimes.fillna(0)


# 3. ALIMENTATION DES DIMENSIONS
# ---------------------------------------------------------
print("--- 3. Alimentation des Dimensions ---")

# A. DIM_INFRACTIONS
# On prend les codes et libellés uniques
dim_infractions = df_crimes[['code_index', 'libelle_index']].drop_duplicates().rename(columns={'code_index': 'id_infraction', 'libelle_index': 'libelle'})
dim_infractions.to_sql('DIM_INFRACTIONS', conn, if_exists='append', index=False)
print(f"-> {len(dim_infractions)} infractions insérées.")

# B. DIM_DEPARTEMENTS
# On prend tous les départements uniques des deux fichiers (crimes et pop)
depts_crimes = df_crimes['departement'].unique()
depts_pop = df_pop['departement'].astype(str).unique()
all_depts = pd.unique(pd.concat([pd.Series(depts_crimes), pd.Series(depts_pop)]))
dim_dept = pd.DataFrame({'code_dept': all_depts, 'nom_dept': 'Inconnu'}) # On n'a pas le nom, on met un placeholder
dim_dept.to_sql('DIM_DEPARTEMENTS', conn, if_exists='append', index=False)
print(f"-> {len(dim_dept)} départements insérés.")

# C. DIM_SERVICES
# C'est plus complexe car l'ID est auto-généré par la base.
# 1. On extrait les uniques
cols_service = ['service', 'CSP', 'perimetre']
dim_services_source = df_crimes[cols_service].drop_duplicates()
dim_services_source.columns = ['type_service', 'nom_unite', 'perimetre'] # Renommer pour matcher la base SQL

# 2. On insère (l'id_service sera créé automatiquement par la DB)
dim_services_source.to_sql('DIM_SERVICES', conn, if_exists='append', index=False)
print(f"-> {len(dim_services_source)} services insérés.")

# 3. CRUCIAL : On relit la table pour récupérer les IDs générés !
# On a besoin d'une map : { (type, nom, perimetre) -> id_service }
df_services_db = pd.read_sql("SELECT * FROM DIM_SERVICES", conn)
print("-> Mapping des services récupéré.")


# D. DIM_TEMPS
# On extrait toutes les années uniques
annees = pd.unique(df_crimes['annee'])
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
df_merged = pd.merge(
    df_crimes, 
    df_services_db, 
    left_on=['service', 'CSP', 'perimetre'], 
    right_on=['type_service', 'nom_unite', 'perimetre'],
    how='left'
)

# 2. Préparation finale
df_faits = df_merged[[
    'annee', 
    'nombre_faits', 
    'code_index',   # Deviendra id_infraction
    'id_service',   # L'ID qu'on vient de récupérer
    'departement'   # Deviendra code_dept
]].copy()

# Renommer pour matcher exactement la table SQL
df_faits.columns = ['annee', 'nombre_faits', 'id_infraction', 'id_service', 'code_dept']

# 3. Insertion en masse
df_faits.to_sql('FAITS_CRIMINELS', conn, if_exists='append', index=False)
print(f"-> {len(df_faits)} faits criminels insérés avec succès !")


# 5. ALIMENTATION POPULATION
# ---------------------------------------------------------
print("--- 5. Alimentation Population ---")
df_pop_clean = df_pop[['departement', 'annee', 'population']].copy()
df_pop_clean['departement'] = df_pop_clean['departement'].astype(str).str.zfill(2) # S'assurer du format
df_pop_clean.columns = ['code_dept', 'annee', 'population']

# Filtrer pour garder seulement les années qui existent dans DIM_TEMPS (contrainte FK)
df_pop_clean = df_pop_clean[df_pop_clean['annee'].isin(annees)]

df_pop_clean.to_sql('STAT_POPULATION', conn, if_exists='append', index=False)
print("-> Population insérée.")

conn.close()
print("--- TERMINE : Base de données prête ---")