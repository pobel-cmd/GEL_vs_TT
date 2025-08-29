from flask import Flask, request, jsonify
import pandas as pd
import requests
import os
import unicodedata

app = Flask(__name__)

TMP_DIR = "/tmp"
os.makedirs(TMP_DIR, exist_ok=True)

# URLs par défaut (surchageables via JSON POST si tu veux)
GEL_URL_DEFAULT = "https://raw.githubusercontent.com/pobel-cmd/make-csv-exchange/refs/heads/main/test-GEL.csv"
TT_URL_DEFAULT  = "https://raw.githubusercontent.com/pobel-cmd/make-csv-exchange/refs/heads/main/test-TT.csv"

# Schéma
ID_COL = "IdRegistre"
COMPARE_COLS = ["Nom", "Prenom", "Date_de_naissance", "Alias", "Nature"]
TT_IDCOLS = ["RowID"]  # colonne technique TimeTonic

# Tokens traités comme "vides" (insensibles à la casse)
INVALID_STRINGS = {"", "undefined", "null", "none", "nan"}

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

def raw_stripped(val):
    """Texte brut trim (sans normalisation)."""
    return strip_compact(as_str(val))

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
    """None si vide/'undefined'/'null'/... sinon valeur trim."""
    s = raw_stripped(val)
    if s is None:
        return None
    if s.lower() in INVALID_STRINGS:
        return None
    return s

def clean_id(val):
    """IdRegistre valide: non vide, ≠ '0', ≠ undefined/null."""
    s = clean_field(val)
    if s is None:
        return None
    if s == "0":
        return None
    return s

def normalize_value(col, val):
    """Pour comparer (accents/casse/espaces)."""
    return normalize_generic(clean_field(val))

def apply_empty_mode(row_dict):
    """Convertit None -> "" pour tous les champs (sauf RowID)."""
    out = {}
    for k, v in row_dict.items():
        if k == "RowID":
            out[k] = v
        else:
            out[k] = "" if v is None else v
    return out

def is_invalid_token_nonempty(raw):
    """True si la chaîne brute est 'undefined'/'null'/'none'/'nan' (≠ '')."""
    if raw is None:
        return False
    r = raw.lower()
    return r in INVALID_STRINGS and r != ""


# ---------------- Core diff ----------------
def compute_delta(df_gel_input, df_tt_input):
    # Vérif colonnes minimales
    for c in [ID_COL] + COMPARE_COLS:
        if c not in df_gel_input.columns:
            raise ValueError(f"Colonne manquante dans GEL: {c}")
        if c not in df_tt_input.columns:
            raise ValueError(f"Colonne manquante dans TT: {c}")

    # Sélection colonnes utiles
    df_gel_all = df_gel_input[[ID_COL] + COMPARE_COLS].copy()
    df_tt_all  = df_tt_input[[ID_COL] + COMPARE_COLS + [c for c in TT_IDCOLS if c in df_tt_input.columns]].copy()

    # --- RAW COPIES (pour détecter 'undefined' & co) ---
    df_gel_raw = df_gel_all.copy()
    df_tt_raw  = df_tt_all.copy()

    # Indexer les RAW sur un ID nettoyé pour aligner avec les CLEAN
    df_gel_raw["_IDCLEAN_"] = df_gel_raw[ID_COL].apply(clean_id)
    df_tt_raw["_IDCLEAN_"]  = df_tt_raw[ID_COL].apply(clean_id)
    df_gel_raw = df_gel_raw.dropna(subset=["_IDCLEAN_"]).drop_duplicates(subset=["_IDCLEAN_"], keep="last").set_index("_IDCLEAN_")
    df_tt_raw  = df_tt_raw.dropna(subset=["_IDCLEAN_"]).drop_duplicates(subset=["_IDCLEAN_"], keep="last").set_index("_IDCLEAN_")

    # --- CLEAN COPIES (pour la sortie) ---
    df_gel = df_gel_raw.copy()
    df_tt  = df_tt_raw.copy()
    for c in COMPARE_COLS:
        df_gel[c] = df_gel[c].apply(clean_field)
        df_tt[c]  = df_tt[c].apply(clean_field)

    ids_gel = set(df_gel.index)
    ids_tt  = set(df_tt.index)

    to_create_ids = sorted(list(ids_gel - ids_tt))   # GEL pas TT
    to_delete_ids = sorted(list(ids_tt - ids_gel))   # TT pas GEL
    intersect_ids = sorted(list(ids_gel & ids_tt))   # communs

    # ---------- CREATE (valeurs GEL clean, vides -> "") ----------
    to_createArray = []
    for rid in to_create_ids:
        gel_row = df_gel.loc[rid]
        row_payload = {"IdRegistre": rid}
        for c in COMPARE_COLS:
            row_payload[c] = gel_row.get(c, None)
        row_payload = apply_empty_mode(row_payload)
        to_createArray.append(row_payload)

    # ---------- DELETE (infos TT clean + RowID, vides -> "") ----------
    to_deleteArray = []
    for rid in to_delete_ids:
        if rid in df_tt.index:
            tt_row_clean = df_tt.loc[rid]
            tt_row_raw   = df_tt_raw.loc[rid]
            payload = {"IdRegistre": rid}
            for c in COMPARE_COLS:
                payload[c] = tt_row_clean.get(c, None)
            # RowID depuis RAW (non nettoyé)
            if "RowID" in tt_row_raw.index:
                rowid = as_str(tt_row_raw.get("RowID"))
                if rowid:
                    payload["RowID"] = rowid
            payload = apply_empty_mode(payload)
            to_deleteArray.append(payload)
        else:
            to_deleteArray.append({"IdRegistre": rid})

    # ---------- UPDATE (TT = miroir exact de GEL ; vides -> "") ----------
    to_update_rowsArray = []
    for rid in intersect_ids:
        gel_row_clean = df_gel.loc[rid]
        tt_row_clean  = df_tt.loc[rid]
        gel_row_raw   = df_gel_raw.loc[rid]
        tt_row_raw    = df_tt_raw.loc[rid]

        changed = False
        row_payload = {"IdRegistre": rid}

        for c in COMPARE_COLS:
            # bruts (pour détecter 'undefined' côté TT)
            gel_raw = raw_stripped(gel_row_raw.get(c, None))
            tt_raw  = raw_stripped(tt_row_raw.get(c, None))

            # nettoyés (pour comparer au sens métier)
            gel_v = gel_row_clean.get(c, None)  # peut être None
            tt_v  = tt_row_clean.get(c, None)

            # A) Cas spécial: TT a 'undefined' (ou null/none/nan) non vide ET GEL est vide -> changer
            if is_invalid_token_nonempty(tt_raw) and (gel_raw is None or gel_raw == ""):
                changed = True
            # B) Sinon: comparer normalisé
            elif normalize_value(c, gel_v) != normalize_value(c, tt_v):
                changed = True

            # miroir exact de GEL (valeur nettoyée)
            row_payload[c] = gel_v

        # RowID depuis RAW
        if "RowID" in tt_row_raw.index:
            rowid = as_str(tt_row_raw.get("RowID"))
            if rowid:
                row_payload["RowID"] = rowid

        if changed:
            row_payload = apply_empty_mode(row_payload)
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
      "tt_url":  "..."
      // Vides envoyés en "" par défaut (jamais 'undefined')
    }
    """
    try:
        body = request.get_json(silent=True) or {}
        gel_url = body.get("gel_url", GEL_URL_DEFAULT)
        tt_url  = body.get("tt_url",  TT_URL_DEFAULT)

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
