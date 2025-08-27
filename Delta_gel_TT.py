from flask import Flask, jsonify
import pandas as pd
import requests
import os

app = Flask(__name__)

TMP_DIR = "/tmp"
os.makedirs(TMP_DIR, exist_ok=True)

# URLs des CSV sur GitHub
GEL_URL = "https://raw.githubusercontent.com/pobel-cmd/csv-storage/refs/heads/main/test-GEL.csv"
TT_URL  = "https://raw.githubusercontent.com/pobel-cmd/csv-storage/refs/heads/main/test-TT.csv"

def download_csv(url, filename):
    path = os.path.join(TMP_DIR, filename)
    try:
        r = requests.get(url, timeout=20)
        r.raise_for_status()
        with open(path, "wb") as f:
            f.write(r.content)
    except Exception as e:
        raise ValueError(f"Erreur lors du téléchargement de {url}: {e}")
    return path

def compare_csv(gel_path, tt_path):
    """Compare deux CSV et retourne les modifications"""
    df_gel = pd.read_csv(gel_path)
    df_tt  = pd.read_csv(tt_path)

    # Colonnes à comparer
    required_cols = ["IdRegistre", "Nom", "Prenom", "Date_de_naissance"]

    # Vérification des colonnes
    for col in required_cols:
        if col not in df_gel.columns:
            raise ValueError(f"Colonne manquante dans GEL: {col}")
        if col not in df_tt.columns:
            raise ValueError(f"Colonne manquante dans TT: {col}")

    # Remplacer NaN par une valeur vide pour comparaison
    df_gel_filled = df_gel.fillna("")
    df_tt_filled  = df_tt.fillna("")

    # Fusion sur IdRegistre
    df_merged = pd.merge(df_gel_filled, df_tt_filled, on="IdRegistre", how="left", suffixes=("_gel","_tt"))

    # Masque pour détecter les différences
    compare_cols = ["Nom", "Prenom", "Date_de_naissance"]
    mask = (df_merged[[c+"_gel" for c in compare_cols]] != df_merged[[c+"_tt" for c in compare_cols]]).any(axis=1)

    # DataFrame des modifications
    df_diff = df_merged[mask]
    df_modif = df_diff[["IdRegistre"] + [c+"_gel" for c in compare_cols]]
    df_modif.columns = ["IdRegistre"] + compare_cols

    return df_modif

@app.route("/", methods=["GET"])
def home():
    return "✅ API Delta_gel_TT is running!"

@app.route("/compare", methods=["POST"])
def compare_endpoint():
    try:
        gel_path = download_csv(GEL_URL, "gel.csv")
        tt_path  = download_csv(TT_URL, "tt.csv")
        df_modif = compare_csv(gel_path, tt_path)

        modifications = df_modif.to_dict(orient="records")
        return jsonify({"status": "ok", "message": "Comparaison terminée", "modifications": modifications})
    except Exception as e:
        return jsonify({"status": "error", "message": str(e)}), 200

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 10000))
    app.run(host="0.0.0.0", port=port)
