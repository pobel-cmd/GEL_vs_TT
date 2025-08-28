from flask import Flask, request, jsonify
import pandas as pd
import requests
import os

app = Flask(__name__)

# Dossier temporaire pour stocker les CSV
TMP_DIR = "/tmp"
os.makedirs(TMP_DIR, exist_ok=True)

# URLs des CSV sur GitHub
GEL_URL = "https://raw.githubusercontent.com/pobel-cmd/GEL_vs_TT/main/test-GEL.csv"
TT_URL  = "https://raw.githubusercontent.com/pobel-cmd/GEL_vs_TT/main/test-TT.csv"

def download_csv(url, filename):
    """Télécharge un CSV depuis GitHub et le sauvegarde temporairement"""
    try:
        r = requests.get(url, timeout=20)
        r.raise_for_status()
    except Exception as e:
        raise ValueError(f"Erreur lors du téléchargement de {url}: {e}")
    
    path = os.path.join(TMP_DIR, filename)
    with open(path, "wb") as f:
        f.write(r.content)
    return path

def compare_csv(gel_path, tt_path):
    """Compare les deux CSV et retourne les modifications"""
    try:
        df_gel = pd.read_csv(gel_path, dtype=str)
        df_tt  = pd.read_csv(tt_path, dtype=str)
    except Exception as e:
        raise ValueError(f"Impossible de lire les CSV: {e}")

    # Colonnes essentielles
    required_cols = ["IdRegistre", "Nom", "Prenom", "Date_de_naissance"]
    for c in required_cols:
        if c not in df_gel.columns:
            raise ValueError(f"Colonne manquante dans GEL: {c}")
        if c not in df_tt.columns:
            raise ValueError(f"Colonne manquante dans TT: {c}")

    # Remplacer NaN par ''
    df_gel.fillna("", inplace=True)
    df_tt.fillna("", inplace=True)

    # Fusion sur IdRegistre
    df_merged = df_gel.merge(df_tt, on="IdRegistre", how="left", suffixes=("_gel", "_tt"))

    # Colonnes à comparer
    compare_cols = ["Nom", "Prenom", "Date_de_naissance"]
    mask = (df_merged[[c+"_gel" for c in compare_cols]] != df_merged[[c+"_tt" for c in compare_cols]]).any(axis=1)

    df_diff = df_merged[mask]

    # DataFrame final des modifications
    df_modif = df_diff[["IdRegistre"] + [c+"_gel" for c in compare_cols]]
    df_modif.columns = ["IdRegistre"] + compare_cols

    return df_modif

@app.route("/", methods=["GET"])
def home():
    return "✅ API Delta_gel_TT is running!"

@app.route("/compare", methods=["POST"])
def compare_endpoint():
    """Compare les CSV et renvoie les modifications en JSON"""
    try:
        gel_path = download_csv(GEL_URL, "gel.csv")
        tt_path  = download_csv(TT_URL, "tt.csv")
        df_modif = compare_csv(gel_path, tt_path)

        # Remplacer les chaînes vides par null pour JSON
        modifications = df_modif.replace({"": None}).to_dict(orient="records")

        return jsonify({
            "status": "ok",
            "message": "Comparaison terminée",
            "modifications": modifications
        })

    except Exception as e:
        return jsonify({"status": "error", "message": str(e)}), 200

# PAS D'APP.RUN ici pour Render
# Render utilisera: gunicorn Delta_gel_TT:app
