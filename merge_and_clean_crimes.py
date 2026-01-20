"""
merge_and_clean_crimes.py

Script pour fusionner et nettoyer les fichiers CSV de statistiques des crimes (format large -> long)
Usage: python merge_and_clean_crimes.py --input-dir . --output crimes_clean_2012_2021.csv

Produits:
- Fichier CSV final avec le schéma: [annee, service, CSP, code_index, libelle_index, nombre_faits]
- Affiche un résumé (nombre de lignes, aperçu)

Dépendances: pandas
"""

import os
import glob
import re
import argparse
import sys
import pandas as pd


def read_csv_smart(path):
    """Lis un fichier CSV ou Excel de manière robuste.
    - Si l'extension est .xls/.xlsx, lit la première feuille via pandas.read_excel en essayant des moteurs.
    - Sinon, essaye plusieurs séparateurs et encodages pour lire un CSV.
    Retourne un DataFrame ou lève une erreur.
    """
    ext = os.path.splitext(path)[1].lower()
    # support Excel
    if ext in ('.xls', '.xlsx'):
        last_exc = None
        # essayer avec openpyxl pour xlsx, xlrd pour xls
        try_engines = []
        if ext == '.xlsx':
            try_engines = ['openpyxl', None]
        else:
            try_engines = ['xlrd', None]
        for eng in try_engines:
            try:
                if eng is None:
                    df = pd.read_excel(path)
                else:
                    df = pd.read_excel(path, engine=eng)
                if df.shape[1] >= 3:
                    return df
            except Exception as e:
                last_exc = e
                continue
        # si la lecture Excel échoue, on continue vers tentative CSV
    # si pas Excel, ou fallback, tenter CSV
    seps = [';', ',', '\t']
    encs = ['utf-8', 'cp1252', 'latin-1']
    last_exc = None
    for enc in encs:
        for sep in seps:
            try:
                df = pd.read_csv(path, sep=sep, engine='python', encoding=enc)
                # require at least 3 columns (index, libelle, au moins 1 département)
                if df.shape[1] >= 3:
                    return df
            except Exception as e:
                last_exc = e
                continue
    # dernier essai: pandas auto-detect
    try:
        df = pd.read_csv(path, engine='python', sep=None)
        if df.shape[1] >= 3:
            return df
    except Exception:
        pass
    raise ValueError(f"Impossible de lire le fichier ({path}). Dernière erreur: {last_exc}")


def extract_meta_from_filename(filename):
    """Extrait l'année et le service (PN/GN) depuis le nom du fichier.
    Retourne (annee (int), service (str)) ou (None, None) si non trouvé.
    Amélioration: recherche des mots 'gendarmerie' ou 'police' si GN/PN absent.
    """
    basename = os.path.basename(filename)
    # année: four digits 2012-2021
    year_match = re.search(r"\b(20\d{2})\b", basename)
    annee = None
    if year_match:
        y = int(year_match.group(1))
        annee = y
    # service: GN ou PN ou recherche de mots-clés
    svc_match = re.search(r"\b(GN|PN)\b", basename, re.IGNORECASE)
    if svc_match:
        service = svc_match.group(1).upper()
    else:
        low = basename.lower()
        if 'gendarmerie' in low or 'gendarmerie' in low.replace('-', ' '):
            service = 'GN'
        elif 'police' in low:
            service = 'PN'
        elif 'gn' in low and 'pn' not in low:
            service = 'GN'
        elif 'pn' in low and 'gn' not in low:
            service = 'PN'
        else:
            service = None
    return annee, service


def normalize_nombre(x):
    """Nettoie une valeur (str/num) pour la convertir en int.
    Gère séparateurs milliers et décimal (',' ou '.') et valeurs non numériques.
    Exemples traités correctement : '1 234', '1.234,56', '3,0' -> 1234, 1235, 3
    """
    if pd.isna(x):
        return 0
    s = str(x).strip()
    if s == '' or s == '-' or s.lower() in ('nan', 'na'):
        return 0
    # normaliser espaces insécables et espaces
    s = s.replace('\xa0', '').replace(' ', '')
    # cas avec milliers et décimale : '1.234,56' -> remove '.' thousands, ',' -> '.' decimal
    if '.' in s and ',' in s:
        s = s.replace('.', '').replace(',', '.')
    else:
        # si seul le séparateur ',' est présent, le considérer comme décimal
        if ',' in s and '.' not in s:
            s = s.replace(',', '.')
        # sinon conserver '.' comme séparateur décimal éventuel
    # garder seulement chiffres, le point décimal et le signe '-'
    s = re.sub(r'[^0-9\.\-]', '', s)
    if s == '' or s == '-' or s == '.' or s == '-.':
        return 0
    try:
        f = float(s)
        # convertir en entier (arrondi au plus proche pour sécurité)
        return int(round(f))
    except Exception:
        return 0


def process_file(path):
    """Traite un fichier CSV ou un fichier Excel contenant plusieurs feuilles.
    - Pour un CSV: comportement inchangé (une table -> melt).
    - Pour un Excel: parcourt les feuilles nommées 'Services PN YYYY' / 'Services GN YYYY' et tente
      de détecter la ligne d'en-tête automatiquement (recherche de 'Code' ou 'Libell').
    Retourne un DataFrame concaténé pour le(s) dataset(s) trouvés dans le fichier.
    """
    print(f"Traitement: {os.path.basename(path)}")
    ext = os.path.splitext(path)[1].lower()
    all_parts = []

    if ext in ('.xls', '.xlsx'):
        xl = pd.ExcelFile(path)
        for sheet in xl.sheet_names:
            # ne traiter que les feuilles de type 'Services' (PN/GN)
            if not re.search(r"services\s*(pn|gn)|\bpn\b|\bgn\b", sheet, re.IGNORECASE):
                continue
            # extraire meta depuis le nom de la feuille si possible
            annee_sheet, service_sheet = extract_meta_from_filename(sheet)
            # fallback sur le nom de fichier
            if annee_sheet is None or service_sheet is None:
                annee_file, service_file = extract_meta_from_filename(path)
                annee = annee_sheet or annee_file
                service = service_sheet or service_file
            else:
                annee = annee_sheet
                service = service_sheet

            # lire la feuille sans en-tête pour détecter la ligne header
            df_raw = pd.read_excel(path, sheet_name=sheet, header=None)
            header_row = None
            max_search = min(80, len(df_raw))
            for i in range(max_search):
                row = df_raw.iloc[i].astype(str).str.strip().str.lower()
                if row.str.contains('code').any() or row.str.contains('libell').any() or row.str.contains('libellé').any():
                    header_row = i
                    break
            if header_row is None:
                # fallback: choisir la première ligne ayant >=3 valeurs non-null
                for i in range(max_search):
                    non_null_count = df_raw.iloc[i].replace('', pd.NA).notna().sum()
                    if non_null_count >= 3:
                        header_row = i
                        break
            if header_row is None:
                header_row = 0

            # relire la feuille en utilisant la ligne d'en-tête détectée
            try:
                # déterminer dynamiquement le nombre de niveaux d'en-tête (1, 2 ou 3)
                header_levels = 1
                if header_row > 0:
                    # lire temporairement les quelques lignes d'au-dessus pour compter les valeurs non-null
                    peek = pd.read_excel(path, sheet_name=sheet, header=None, nrows=header_row)
                    # compter lignes non-nullity pour les deux lignes au-dessus
                    if header_row >= 2 and peek.iloc[header_row-2].replace('', pd.NA).notna().sum() >= 3:
                        header_levels = 3
                    elif peek.iloc[header_row-1].replace('', pd.NA).notna().sum() >= 3:
                        header_levels = 2
                # construire l'argument header pour pandas
                if header_levels == 1:
                    df = pd.read_excel(path, sheet_name=sheet, header=header_row)
                elif header_levels == 2:
                    df = pd.read_excel(path, sheet_name=sheet, header=[header_row-1, header_row])
                else:
                    df = pd.read_excel(path, sheet_name=sheet, header=[header_row-2, header_row-1, header_row])
            except Exception as e:
                print(f"  Erreur lecture feuille {sheet}: {e}")
                continue
            # supprimer colonnes/rows vides
            df = df.dropna(axis=0, how='all')
            df = df.dropna(axis=1, how='all')
            if df.shape[1] < 3:
                print(f"  Feuille {sheet} ignorée (moins de 3 colonnes après nettoyage)")
                continue

            # gérer cas de MultiIndex dans les colonnes pour extraire departement et perimetre
            departement_map = {}
            perimetre_map = {}
            if isinstance(df.columns, pd.MultiIndex):
                new_cols = []
                seen = {}
                for col in df.columns:
                    # col peut être tuple de longueur 2 ou 3
                    if len(col) == 3:
                        top = str(col[0]).strip()
                        mid = str(col[1]).strip()
                        bot = str(col[2]).strip()
                    else:
                        top = str(col[0]).strip()
                        mid = ''
                        bot = str(col[1]).strip()
                    # extraire code département du niveau supérieur si présent
                    m = re.search(r"(\d{1,3}[AB]?)", top)
                    dept = m.group(1) if m else top
                    perim = mid if mid and mid.lower() not in ('nan', '') else ''
                    csp = bot if bot and bot.lower() not in ('nan', '') else mid if mid else top
                    # garantir l'unicité du nom de colonne
                    key = csp
                    if key in seen:
                        seen[key] += 1
                        key = f"{csp}__{seen[csp]}"
                    else:
                        seen[key] = 1
                    new_cols.append(key)
                    departement_map[key] = dept
                    perimetre_map[key] = perim
                df.columns = new_cols

            # renommer premières colonnes puis melt
            cols = list(df.columns)
            first_col, second_col = cols[0], cols[1]
            df = df.rename(columns={first_col: 'code_index', second_col: 'libelle_index'})
            id_vars = ['code_index', 'libelle_index']
            value_vars = [c for c in df.columns if c not in id_vars]
            try:
                # var_name 'CSP'
                df_long = df.melt(id_vars=id_vars, value_vars=value_vars, var_name='CSP', value_name='nombre_faits')
            except Exception as e:
                print(f"  Erreur melt feuille {sheet}: {e}")
                continue
            # metadata
            df_long['annee'] = annee
            df_long['service'] = service
            # extraire departement et perimetre à partir des maps si présentes
            if departement_map:
                df_long['departement'] = df_long['CSP'].map(departement_map).astype(str).str.strip()
                df_long['perimetre'] = df_long['CSP'].map(perimetre_map).astype(str).str.strip()
                # pour les GN, perimetre_map contiendra '' et restera vide
            else:
                # fallback: tenter d'extraire un code département et laisser perimetre vide
                df_long['departement'] = df_long['CSP'].astype(str).str.extract(r"(\d{1,3}[AB]?)", expand=False).fillna('').astype(str).str.strip()
                df_long['perimetre'] = ''
                # retirer le code du CSP pour garder un nom CSP propre
                df_long['CSP'] = df_long['CSP'].astype(str).str.replace(r"\b\d{1,3}[AB]?$", '', regex=True).str.strip()

            # nettoyage minimal des colonnes
            df_long['CSP'] = df_long['CSP'].astype(str).str.strip()
            df_long['perimetre'] = df_long['perimetre'].astype(str).replace('nan', '').fillna('').str.strip()
            df_long['code_index'] = df_long['code_index'].astype(str).str.strip()
            df_long['libelle_index'] = df_long['libelle_index'].astype(str).str.strip()
            df_long['nombre_faits'] = df_long['nombre_faits'].apply(normalize_nombre).astype(int)
            # réordonner pour inclure la colonne 'departement' et 'perimetre'
            df_long = df_long[['annee', 'service', 'departement', 'perimetre', 'CSP', 'code_index', 'libelle_index', 'nombre_faits']]
            all_parts.append(df_long)
        if not all_parts:
            raise ValueError(f"Aucune feuille de service traitée dans {path}")
        return pd.concat(all_parts, ignore_index=True)

    # sinon traitement CSV habituel
    annee_file, service_file = extract_meta_from_filename(path)
    if annee_file is None or service_file is None:
        print(f"  Attention: impossible d'extraire annee/service pour {path} (annee={annee_file}, service={service_file})")
    df = read_csv_smart(path)
    cols = list(df.columns)
    first_col, second_col = cols[0], cols[1]
    df = df.rename(columns={first_col: 'code_index', second_col: 'libelle_index'})
    id_vars = ['code_index', 'libelle_index']
    value_vars = [c for c in df.columns if c not in id_vars]
    df_long = df.melt(id_vars=id_vars, value_vars=value_vars, var_name='CSP', value_name='nombre_faits')
    df_long['annee'] = annee_file
    df_long['service'] = service_file
    # tenter d'extraire un code département depuis le nom de la colonne CSP
    df_long['departement'] = df_long['CSP'].astype(str).str.extract(r"(\d{1,3}[AB]?)", expand=False).fillna('').astype(str).str.strip()
    # pour les CSV on ne connais pas le perimetre -> mettre vide
    df_long['perimetre'] = ''
    df_long['CSP'] = df_long['CSP'].astype(str).str.replace(r"\b\d{1,3}[AB]?$", '', regex=True).str.strip()
    df_long['CSP'] = df_long['CSP'].astype(str).str.strip()
    df_long['code_index'] = df_long['code_index'].astype(str).str.strip()
    df_long['libelle_index'] = df_long['libelle_index'].astype(str).str.strip()
    df_long['nombre_faits'] = df_long['nombre_faits'].apply(normalize_nombre).astype(int)
    df_long = df_long[['annee', 'service', 'departement', 'perimetre', 'CSP', 'code_index', 'libelle_index', 'nombre_faits']]
    return df_long


def main(input_dir, output_file):
    # Ne traiter que le fichier Excel source exact demandé
    target_name = 'crimes-et-delits-enregistres-par-les-services-de-gendarmerie-et-de-police-depuis-2012.xlsx'
    target_path = os.path.join(input_dir, target_name)
    if not os.path.exists(target_path):
        print(f"Fichier attendu introuvable: {target_path}")
        return 1
    files = [target_path]
    # retirer le fichier de sortie s'il se trouve dans le même dossier (précaution)
    abs_output = os.path.abspath(output_file)
    files = [f for f in files if os.path.abspath(f) != abs_output]
    if not files:
        print(f"Aucun fichier à traiter après filtrage (output exclu).")
        return 1
    parts = []
    for f in files:
        try:
            part = process_file(f)
            parts.append(part)
        except Exception as e:
            print(f"  Erreur pour {f}: {e}")
    if not parts:
        print("Aucun DataFrame valide n'a été produit.")
        return 1
    try:
        master = pd.concat(parts, ignore_index=True)
    except Exception as e:
        print(f"Erreur lors de la concaténation des DataFrames: {e}")
        return 1
    # remplacer valeurs nulles restantes
    master['nombre_faits'] = master['nombre_faits'].fillna(0).astype(int)
    # exporter
    master.to_csv(output_file, index=False, encoding='utf-8-sig')
    print(f"Fichier exporté: {output_file}")
    print(f"Nombre total de lignes: {len(master)}")

    # Aperçu : afficher l'en-tête puis les 5 premières lignes pour PN et GN séparément
    print("Aperçu : 5 premières lignes PN puis 5 premières GN")
    # afficher l'en-tête CSV
    header = ','.join(master.columns)
    print(header)

    pn = master[master['service'] == 'PN'].head(5)
    gn = master[master['service'] == 'GN'].head(5)

    def _print_df_as_csv_no_header(df):
        if df.empty:
            print('[aucune ligne]')
            return
        # to_csv retourne une chaîne; header=False pour éviter redondance
        s = df.to_csv(index=False, header=False)
        # imprimer chaque ligne produite
        for line in s.strip().splitlines():
            print(line)

    print('\nFirst 5 PN:')
    _print_df_as_csv_no_header(pn)
    print('\nFirst 5 GN:')
    _print_df_as_csv_no_header(gn)
    return 0


if __name__ == '__main__':
    # Exécution non interactive : aucun argument attendu.
    # Le script traitera toujours le fichier source attendu dans le même dossier que ce script
    # et produira le fichier de sortie verrouillé 'crimes_clean_2012_2021.csv'.
    script_dir = os.path.dirname(os.path.abspath(__file__))
    input_dir = script_dir
    output_file = 'crimes_clean_2012_2021.csv'
    rc = main(input_dir, output_file)
    sys.exit(rc)
