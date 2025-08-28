from flask import Flask, request, jsonify
import pandas as pd
import os

app = Flask(__name__)

TMP_DIR = "/tmp"
os.makedirs(TMP_DIR, exist_ok=True)

# URLs des CSV sur GitHub par défaut
GEL_URL = "https://raw.githubusercontent.com/pobel-cmd/csv-storage/refs/heads/main/test-GEL.csv"
TT_URL  = "https://raw.githubusercontent.com/pobel-cmd/csv-storage/refs/heads/main/test-TT.csv"

# Noms des fichiers temporaires
GEL_FILE = os.path.join(TMP_DIR, "gel.csv")
TT_FILE  = os.path.join(TMP_DIR, "tt.csv")

def download_csv(url, path):
    """Télécharge un CSV depuis GitHub si le fichier n'existe pas déjà"""
    import requests
    if not os.path.exists(path):
        r = requests.get(url, timeout=20)
        r.raise_for_status()
        with open(path, "wb") as f:
            f.write(r.content)

def compare_csv(gel_path, tt_path):
    """Compare les deux CSV et retourne les modifications"""
    df_gel = pd.read_csv(gel_path, dtype=str).fillna("")
    df_tt  = pd.read_csv(tt_path, dtype=str).fillna("")

    # Colonnes à vérifier
    required_gel = ["IdRegistre", "Nom", "Prenom", "Date_de_naissance"]
    required_tt  = ["IdRegistre", "Nom", "Prenom", "Date_de_naissance"]

    if not all(c in df_gel.columns for c in required_gel):
        raise ValueError(f"Colonnes manquantes dans GEL: {required_gel}")
    if not all(c in df_tt.columns for c in required_tt):
        raise ValueError(f"Colonnes manquantes dans TT: {required_tt}")

    # Tri pour garantir alignement des index
    df_gel = df_gel.sort_values("IdRegistre").reset_index(drop=True)
    df_tt  = df_tt.sort_values("IdRegistre").reset_index(drop=True)

    # Fusion sur IdRegistre
    df_merged = pd.merge(df_gel, df_tt, on="IdRegistre", how="outer", suffixes=("_gel", "_tt"))

    # Colonnes à comparer
    compare_cols = ["Nom", "Prenom", "Date_de_naissance"]
    mask = (df_merged[[c+"_gel" for c in compare_cols]] != df_merged[[c+"_tt" for c in compare_cols]]).any(axis=1)

    df_diff = df_merged[mask]

    # DataFrame final
    df_modif = df_diff[["IdRegistre"] + [c+"_gel" for c in compare_cols]]
    df_modif.columns = ["IdRegistre"] + compare_cols

    return df_modif

@app.route("/", methods=["GET"])
def home():
    return "✅ API Delta_gel_TT is running!"

@app.route("/upload_csv", methods=["POST"])
def upload_csv():
    """Recevoir des fichiers CSV GEL et TT depuis Make"""
    try:
        if "gel" in request.files:
            request.files["gel"].save(GEL_FILE)
        if "tt" in request.files:
            request.files["tt"].save(TT_FILE)
        return jsonify({"status": "ok", "message": "Fichiers reçus"})
    except Exception as e:
        return jsonify({"status": "error", "message": str(e)}), 200

@app.route("/compare", methods=["POST"])
def compare_endpoint():
    """Compare les CSV et renvoie les modifications"""
    try:
        # Si Make a uploadé les fichiers, on les prend, sinon on télécharge depuis GitHub
        download_csv(GEL_URL, GEL_FILE)
        download_csv(TT_URL, TT_FILE)

        df_modif = compare_csv(GEL_FILE, TT_FILE)
        modifications = df_modif.to_dict(orient="records")

        return jsonify({
            "status": "ok",
            "message": "Comparaison terminée",
            "modifications": modifications
        })

    except Exception as e:
        return jsonify({"status": "error", "message": str(e)}), 200

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 10000))
    app.run(host="0.0.0.
