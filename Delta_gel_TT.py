from flask import Flask, request, jsonify
import pandas as pd
import requests
import os
import unicodedata

app = Flask(__name__)

TMP_DIR = "/tmp"
os.makedirs(TMP_DIR, exist_ok=True)

# URLs des CSV sur GitHub (peuvent être surchargées via JSON POST)
GEL_URL_DEFAULT = "https://raw.githubusercontent.com/pobel-cmd/make-csv-exchange/refs/heads/main/test-GEL.csv"
TT_URL_DEFAULT  = "https://raw.githubusercontent.com/pobel-cmd/make-csv-exchange/refs/heads/main/test-TT.csv"

ID_COL = "IdRegistre"
COMPARE_COLS = ["Nom", "Prenom", "Date_de_naissance"]  # adapte si besoin


# ---------- Utils ----------
def download_csv(url, filename):
    try:
        r = requests.get(url, timeout=20)
        r.raise_for_status()
    except Exception as e:
        raise ValueError(f"Erreur lors du téléchargement de {url}: {e}")
    path = os.path.join(TMP_DIR, filename)
    with open(path, "wb") as f:
        f.write(r.content)
    return path

def as_str(x):
    return None if pd.isna(x) else str(x)

def strip_compact(s):
    if s is None:
        return None
    s = " ".join(s.split())  # compacter espaces multiples
    return s.strip()

def remove_accents(s):
    if s is None:
        return None
    nfkd = unicodedata.normalize("NFKD", s)
    return "".join([c for c in nfkd if not unicodedata.combining(c)])

def normalize_generic(s):
    """Normalisation légère et robuste pour comparer sans bruit d'accents/casse/espaces."""
    if s is None:
        return None
    s = as_str(s)
    s = strip_compact(s)
    s = remove_accents(s)
    s = s.upper()
    return s

def normalize_value(col, val):
    # On peut spécialiser par colonne si besoin (dates, etc.)
    # Ici: même traitement pour Nom, Prenom, Date_de_naissance.
    return normalize_generic(as_str(val))


# ---------- Core diff ----------
def compute_delta(df_gel, df_tt):
    # Assurer types string et présence colonnes
    for c in [ID_COL] + COMPARE_COLS:
        if c not in df_gel.columns:
            raise ValueError(f"Colonne manquante dans GEL: {c}")
        if c not in df_tt.columns:
            raise ValueError(f"Colonne manquante dans TT: {c}")

    # Sélection colonnes utiles + nettoyage basique (sans normalisation destructive)
    df_gel = df_gel[[ID_COL] + COMPARE_COLS].copy().astype("string")
    df_tt  = df_tt[[ID_COL] + COMPARE_COLS].copy().astype("string")

    # Index par ID pour lookup rapide (si doublons, le dernier gagne)
    df_gel = df_gel.dropna(subset=[ID_COL]).drop_duplicates(subset=[ID_COL], keep="last").set_index(ID_COL)
    df_tt  = df_tt.dropna(subset=[ID_COL]).drop_duplicates(subset=[ID_COL], keep="last").set_index(ID_COL)

    ids_gel = set(df_gel.index)
    ids_tt  = set(df_tt.index)

    to_create_ids = sorted(list(ids_gel - ids_tt))
    to_delete_ids = sorted(list(ids_tt - ids_gel))
    intersect_ids = sorted(list(ids_gel & ids_tt))

    # Ajouts: on renvoie la ligne GEL brute
    to_create = []
    for rid in to_create_ids:
        rec = df_gel.loc[rid]
        out = {ID_COL: rid}
        for c in COMPARE_COLS:
            out[c] = as_str(rec[c])
        to_create.append(out)

    # Suppressions: on renvoie juste l'ID (TT doit supprimer)
    to_delete = [{"IdRegistre": rid} for rid in to_delete_ids]

    # Mises à jour: comparer champ par champ sur valeurs normalisées
    to_update = []
    for rid in intersect_ids:
        gel_row = df_gel.loc[rid]
        tt_row  = df_tt.loc[rid]
        changes = []
        for c in COMPARE_COLS:
            gel_raw = as_str(gel_row[c])
            tt_raw  = as_str(tt_row[c])

            gel_norm = normalize_value(c, gel_raw)
            tt_norm  = normalize_value(c, tt_raw)

            # Si l'une est None et pas l'autre, ou si normalisés diffèrent -> update
            if gel_norm != tt_norm:
                changes.append({
                    "field": c,
                    "old": tt_raw,   # valeur actuelle dans TT
                    "new": gel_raw,  # valeur de référence à appliquer (GEL)
                })

        if changes:
            to_update.append({
                ID_COL: rid,
                "changes": changes
            })

    return {
        "counts": {
            "create": len(to_create),
            "update": len(to_update),
            "delete": len(to_delete)
        },
        "to_create": to_create,
        "to_update": to_update,
        "to_delete": to_delete
    }


# ---------- Flask endpoints ----------
@app.route("/", methods=["GET"])
def home():
    return "✅ API Delta_gel_TT is running!"

@app.route("/compare", methods=["POST"])
def compare_endpoint():
    """
    Body JSON (optionnel):
    {
      "gel_url": "...",
      "tt_url":  "...",
      "id_col": "IdRegistre",
      "compare_cols": ["Nom","Prenom","Date_de_naissance"]
    }
    """
    try:
        body = request.get_json(silent=True) or {}
        gel_url = body.get("gel_url", GEL_URL_DEFAULT)
        tt_url  = body.get("tt_url",  TT_URL_DEFAULT)

        global ID_COL, COMPARE_COLS
        if body.get("id_col"):
            ID_COL = body["id_col"]
        if body.get("compare_cols"):
            COMPARE_COLS = body["compare_cols"]

        gel_path = download_csv(gel_url, "gel.csv")
        tt_path  = download_csv(tt_url, "tt.csv")

        df_gel = pd.read_csv(gel_path, dt_
