from flask import Flask, request, jsonify
import pandas as pd
import requests
import os
import unicodedata

app = Flask(__name__)

TMP_DIR = "/tmp"
os.makedirs(TMP_DIR, exist_ok=True)

# URLs par défaut (surchageables via JSON POST)
GEL_URL_DEFAULT = "https://raw.githubusercontent.com/pobel-cmd/make-csv-exchange/refs/heads/main/test-GEL.csv"
TT_URL_DEFAULT  = "https://raw.githubusercontent.com/pobel-cmd/make-csv-exchange/refs/heads/main/test-TT.csv"

# Schéma
ID_COL = "IdRegistre"
COMPARE_COLS = ["Nom", "Prenom", "Date_de_naissance", "Alias", "Nature"]
TT_IDCOLS = ["RowID"]  # colonne technique TimeTonic

INVALID_STRINGS = {"", "undefined", "null", "none", "nan"}  # insensible à la casse


# ---------------- Utils ----------------
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
    s = " ".join(s.split())
    return s.strip()

def remove_accents(s):
    if s is None:
        return None
    nfkd = unicodedata.normalize("NFKD", s)
    return "".join([c for c in nfkd if not unicodedata.combining(c)])

def normalize_generic(s):
    if s is None:
        return None
    s = as_str(s)
    s = strip_compact(s)
    s = remove_accents(s)
    s = s.upper()
    return s

def clean_field(val):
    """Retourne None si vide/'undefined'/'null'/... sinon valeur trim."""
    s = as_str(val)
    if s is None:
        return None
    s2 = strip_compact(s)
    if s2 is None:
        return None
    if s2.lower() in INVALID_STRINGS:
        return None
    return s2

def clean_id(val):
    """IdRegistre valide: non vide, ≠ '0', ≠ undefined/null."""
    s = clean_field(val)
    if s is None:
        return None
    if s == "0":
        return None
    return s

def normalize_value(col, val):
    """Normalisation pour comparaison (accents/casse/espaces)."""
    return normalize_generic(clean_field(val))


# ---------------- Core diff ----------------
def compute_delta(df_gel, df_tt):
    # Vérif colonnes
    for c in [ID_COL] + COMPARE_COLS:
        if c not in df_gel.columns:
            raise ValueError(f"Colonne manquante dans GEL: {c}")
        if c not in df_tt.columns:
            raise ValueError(f"Colonne manquante dans TT: {c}")

    # Colonnes utiles (garder RowID si dispo côté TT)
    keep_tt_cols = [ID_COL] + COMPARE_COLS + [c for c in TT_IDCOLS if c in df_tt.columns]
    df_gel = df_gel[[ID_COL] + COMPARE_COLS].copy()
    df_tt  = df_tt[keep_tt_cols].copy()

    # Nettoyage des valeurs
    df_gel[ID_COL] = df_gel[ID_COL].apply(clean_id)
    df_tt[ID_COL]  = df_tt[ID_COL].apply(clean_id)
    for c in COMPARE_COLS:
        df_gel[c] = df_gel[c].apply(clean_field)
        df_tt[c]  = df_tt[c].apply(clean_field)

    # Index par ID
    df_gel = df_gel.dropna(subset=[ID_COL]).drop_duplicates(subset=[ID_COL], keep="last").set_index(ID_COL)
    df_tt  = df_tt.dropna(subset=[ID_COL]).drop_duplicates(subset=[ID_COL], keep="last").set_index(ID_COL)

    ids_gel = set(df_gel.index)
    ids_tt  = set(df_tt.index)

    to_create_ids = sorted(list(ids_gel - ids_tt))   # GEL pas TT
    to_delete_ids = sorted(list(ids_tt - ids_gel))   # TT pas GEL
    intersect_ids = sorted(list(ids_gel & ids_tt))   # communs

    # ---------- CREATE (valeurs GEL) ----------
    to_createArray = []
    for rid in to_create_ids:
        gel_row = df_gel.loc[rid]
        out = {"IdRegistre": rid}
        for c in COMPARE_COLS:
            out[c] = gel_row.get(c, None)
        to_createArray.append(out)

    # ---------- DELETE (infos TT pour vérif) ----------
    to_deleteArray = []
    for rid in to_delete_ids:
        if rid in df_tt.index:
            tt_row = df_tt.loc[rid]
            payload = {"IdRegistre": rid}
            for c in COMPARE_COLS:
                payload[c] = tt_row.get(c, None)
            if "RowID" in df_tt.columns:
                rowid = as_str(tt_row.get("RowID"))
                if rowid:
                    payload["RowID"] = rowid
            to_deleteArray.append(payload)
        else:
            to_deleteArray.append({"IdRegistre": rid})

    # ---------- UPDATE (ligne GEL + RowID TT + has_Alias) ----------
    to_update_rowsArray = []
    for rid in intersect_ids:
        gel_row = df_gel.loc[rid]
        tt_row  = df_tt.loc[rid]
        changed = False
        for c in COMPARE_COLS:
            if normalize_value(c, gel_row.get(c, None)) != normalize_value(c, tt_row.get(c, None)):
                changed = True
                break
        if changed:
            row_payload = {"IdRegistre": rid}
            for c in COMPARE_COLS:
                row_payload[c] = gel_row.get(c, None)  # valeurs de référence = GEL
            # RowID si dispo côté TT
            if "RowID" in df_tt.columns:
                rowid = as_str(tt_row.get("RowID"))
                if rowid:
                    row_payload["RowID"] = rowid
            # Flag pratique pour router dans Make
            row_payload["has_Alias"] = bool(row_payload.get("Alias"))
            to_update_rowsArray.append(row_payload)

    return {
        "counts": {
            "create": len(to_createArray),
            "update": len(to_update_rowsArray),
            "delete": len(to_deleteArray),
        },
        "to_createArray": to_createArray,
        "to_update_rowsArray": to_update_rowsArray,
        "to_deleteArray": to_deleteArray
    }


# ---------------- Flask endpoints ----------------
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
      "compare_cols": ["Nom","Prenom","Date_de_naissance","Alias","Nature"]
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
        tt_path  = download_csv(tt_url,  "tt.csv")

        df_gel = pd.read_csv(gel_path, dtype=str, keep_default_na=False)
        df_tt  = pd.read_csv(tt_path,  dtype=str, keep_default_na=False)

        delta = compute_delta(df_gel, df_tt)
        return jsonify({"status": "ok", "message": "Comparaison terminée", **delta})

    except Exception as e:
        return jsonify({"status": "error", "message": str(e)}), 200


if __name__ == "__main__":
    port = int(os.environ.get("PORT", 10000))
    app.run(host="0.0.0.0", port=port)
