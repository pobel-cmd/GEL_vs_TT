from flask import Flask, request, jsonify
import pandas as pd
import requests
import os
import unicodedata

app = Flask(__name__)

TMP_DIR = "/tmp"
os.makedirs(TMP_DIR, exist_ok=True)

GEL_URL_DEFAULT = "https://raw.githubusercontent.com/pobel-cmd/make-csv-exchange/refs/heads/main/test-GEL.csv"
TT_URL_DEFAULT  = "https://raw.githubusercontent.com/pobel-cmd/make-csv-exchange/refs/heads/main/test-TT.csv"

ID_COL = "IdRegistre"
COMPARE_COLS = ["Nom", "Prenom", "Date_de_naissance"]


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


# ---------- Core diff ----------
def compute_delta(df_gel, df_tt, updates_as_rows: bool):
    # Vérifier colonnes
    for c in [ID_COL] + COMPARE_COLS:
        if c not in df_gel.columns:
            raise ValueError(f"Colonne manquante dans GEL: {c}")
        if c not in df_tt.columns:
            raise ValueError(f"Colonne manquante dans TT: {c}")

    # Colonnes utiles et types
    df_gel = df_gel[[ID_COL] + COMPARE_COLS].copy().astype("string")
    df_tt  = df_tt[[ID_COL] + COMPARE_COLS].copy().astype("string")

    # Index par ID
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

    to_create_ids = sorted(list(ids_gel - ids_tt))
    to_delete_ids = sorted(list(ids_tt - ids_gel))
    intersect_ids = sorted(list(ids_gel & ids_tt))

    # Ajouts (lignes GEL complètes)
    to_create = []
    for rid in to_create_ids:
        rec = df_gel.loc[rid]
        out = {"IdRegistre": rid}
        for c in COMPARE_COLS:
            out[c] = as_str(rec[c])
        to_create.append(out)

    # Suppressions (ID seul)
    to_delete = [{"IdRegistre": rid} for rid in to_delete_ids]

    # Updates
    to_update_changes = []  # ancien format (avec old/new) — on pourra le supprimer si updates_as_rows=True
    to_update_rows    = []  # nouveau format demandé : ligne complète (valeurs GEL)
    for rid in intersect_ids:
        gel_row = df_gel.loc[rid]
        tt_row  = df_tt.loc[rid]
        changed = False
        changes = []
        for c in COMPARE_COLS:
            gel_raw = as_str(gel_row[c])
            tt_raw  = as_str(tt_row[c])
            gel_norm = normalize_value(c, gel_raw)
            tt_norm  = normalize_value(c, tt_raw)
            if gel_norm != tt_norm:
                changed = True
                changes.append({"field": c, "old": tt_raw, "new": gel_raw})
        if changed:
            # version “rows” (valeurs GEL complètes)
            row_out = {"IdRegistre": rid}
            for c in COMPARE_COLS:
                row_out[c] = as_str(gel_row[c])
            to_update_rows.append(row_out)

            # version “changes” (conservée si besoin)
            to_update_changes.append({"IdRegistre": rid, "changes": changes})

    payload = {
        "counts": {
            "create": len(to_create),
            "update": len(to_update_rows),
            "delete": len(to_delete),
        },
        "to_create": to_create,
        "to_delete": to_delete,
    }

    # Selon le mode demandé :
    if updates_as_rows:
        payload["to_update"] = to_update_rows           # uniquement lignes complètes
    else:
        payload["to_update"] = to_update_changes        # ancien format
        payload["to_update_rows"] = to_update_rows      # + nouveau format en parallèle

    return payload


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
      "compare_cols": ["Nom","Prenom","Date_de_naissance"],
      "update_payload": "rows" | "changes"   # défaut: "changes" + "to_update_rows"
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

        updates_as_rows = (body.get("update_payload") == "rows")

        gel_path = download_csv(gel_url, "gel.csv")
        tt_path  = download_csv(tt_url,  "tt.csv")

        df_gel = pd.read_csv(gel_path, dtype=str, keep_default_na=False, na_values=[""])
        df_tt  = pd.read_csv(tt_path,  dtype=str, keep_default_na=False, na_values=[""])

        delta = compute_delta(df_gel, df_tt, updates_as_rows)

        return jsonify({"status": "ok", "message": "Comparaison terminée", **delta})

    except Exception as e:
        return jsonify({"status": "error", "message": str(e)}), 200


if __name__ == "__main__":
    port = int(os.environ.get("PORT", 10000))
    app.run(host="0.0.0.0", port=port)
