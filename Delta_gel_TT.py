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

# Schéma minimal
ID_COL = "IdRegistre"
COMPARE_COLS = ["Nom", "Prenom", "Date_de_naissance", "Alias", "Nature"]
TT_IDCOLS = ["RowID"]  # colonne technique TT

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

def normalize_value(col, val):
    return normalize_generic(as_str(val))

# ---------------- Core diff ----------------
def compute_delta(df_gel, df_tt):
    # vérif colonnes
    for c in [ID_COL] + COMPARE_COLS:
        if c not in df_gel.columns:
            raise ValueError(f"Colonne manquante dans GEL: {c}")
        if c not in df_tt.columns:
            raise ValueError(f"Colonne manquante dans TT: {c}")

    # colonnes utiles (on garde RowID si présent dans TT)
    tt_cols = [ID_COL] + COMPARE_COLS + [c for c in TT_IDCOLS if c in df_tt.columns]
    df_gel = df_gel[[ID_COL] + COMPARE_COLS].copy().astype("string")
    df_tt  = df_tt[tt_cols].copy().astype("string")

    # index par ID
    df_gel = (
        df_gel.dropna(subset=[ID_COL])
              .drop_duplicates(subset=[ID_COL], keep="last")
              .set_index(ID_COL)
    )
    df_tt = (
        df_tt.dropna(subset=[ID_COL])
             .drop_duplicates(subset=[ID_COL], keep="last")
             .set_index(ID_COL)
    )

    ids_gel = set(df_gel.index)
    ids_tt  = set(df_tt.index)

    to_create_ids = sorted(list(ids_gel - ids_tt))   # dans GEL, pas dans TT -> créer
    to_delete_ids = sorted(list(ids_tt - ids_gel))   # dans TT, pas dans GEL -> supprimer
    intersect_ids = sorted(list(ids_gel & ids_tt))   # dans les deux -> comparer

    # ---------- CREATE (lignes GEL complètes) ----------
    to_createArray = []
    for rid in to_create_ids:
        gel_row = df_gel.loc[rid]
        out = {"IdRegistre": rid}
        for c in COMPARE_COLS:
            out[c] = as_str(gel_row[c])
        to_createArray.append(out)

    # ---------- DELETE (infos TT complètes pour vérif) ----------
    # On renvoie : RowID (si disponible), IdRegistre, Nom, Prenom, Date_de_naissance, Alias, Nature — tous issus de TT
    to_deleteArray = []
    for rid in to_delete_ids:
        if rid in df_tt.index:
            tt_row = df_tt.loc[rid]
            delete_payload = {"IdRegistre": rid}
            # recopier les champs TT visibles pour vérification
            for c in COMPARE_COLS:
                delete_payload[c] = as_str(tt_row[c]) if c in tt_row.index else None
            # ajouter RowID si dispo
            if "RowID" in tt_row.index and as_str(tt_row["RowID"]):
                delete_payload["RowID"] = as_str(tt_row["RowID"])
            to_deleteArray.append(delete_payload)
        else:
            # cas très rare si incohérence d'indexation : on met au moins l'IdRegistre
            to_deleteArray.append({"IdRegistre": rid})

    # ---------- UPDATE (on renvoie la ligne GEL + RowID TT si dispo) ----------
    to_update_rowsArray = []
    for rid in intersect_ids:
        gel_row = df_gel.loc[rid]
        tt_row  = df_tt.loc[rid]
        changed = False
        for c in COMPARE_COLS:
            gel_raw = as_str(gel_row[c])
            tt_raw  = as_str(tt_row[c])
            if normalize_value(c, gel_raw) != normalize_value(c, tt_raw):
                changed = True
        if changed:
            row_payload = {"IdRegistre": rid}
            for c in COMPARE_COLS:
                row_payload[c] = as_str(gel_row[c])  # valeurs de référence = GEL
            if "RowID" in tt_row.index and as_str(tt_row["RowID"]):
                row_payload["RowID"] = as_str(tt_row["RowID"])
            to_update_rowsArray.append(row_payload)

    payload = {
        "counts": {
            "create": len(to_createArray),
            "update": len(to_update_rowsArray),
            "delete": len(to_deleteArray),
        },
        "to_createArray": to_createArray,
        "to_update_rowsArray": to_update_rowsArray,
        "to_deleteArray": to_deleteArray
    }
    return payload

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
      "compare_cols": ["Nom","Prenom","Date_de_naissance", "Alias", "Nature"]
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

        df_gel = pd.read_csv(gel_path, dtype=str, keep_default_na=False, na_values=[""])
        df_tt  = pd.read_csv(tt_path,  dtype=str, keep_default_na=False, na_values=[""])

        delta = compute_delta(df_gel, df_tt)

        return jsonify({
            "status": "ok",
            "message": "Comparaison terminée",
            **delta
        })

    except Exception as e:
        return jsonify({"status": "error", "message": str(e)}), 200

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 10000))
    app.run(host="0.0.0.0", port=port)
