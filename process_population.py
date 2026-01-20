"""
process_population.py

Script pour retraiter le fichier DS_ESTIMATION_POPULATION_data.csv :
- produire un CSV nettoyé avec en-têtes lisibles `DS_ESTIMATION_POPULATION_clean.csv`
- agréger au niveau département x année et produire `population_by_dept_year.csv`

Usage (dans le dossier du dépôt) :
    python process_population.py

"""
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
