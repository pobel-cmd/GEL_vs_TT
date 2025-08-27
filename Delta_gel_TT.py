from flask import Flask, request, jsonify
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

def clean_string(s):
    if pd.isna(s):
        return None
    return str(s).strip()

def compare_csv(gel_path, tt_path):
    """Compare les deux CSV et retourne les modifications"""
    try:
        df_gel = pd.read_csv(gel_path, dtype=str)
        df_tt  = pd.read_csv(tt_path, dtype=str)
    except Exception as e:
        raise ValueError(f"Impossible de lire les CSV: {e}")

    # Colonnes à vérifier
    required_gel = ["IdRegistre", "Nom", "Prenom", "Date_de_naissance"]
    required_tt  = ["IdRegistre", "Nom", "Prenom", "Date_de_naissance"]

    for c in required_gel:
        if c not in df_gel.columns:
            raise ValueError(f"Colonne manquante dans GEL: {c}")
    for c in required_tt:
        if c not in df_tt.columns:
            raise ValueError(f"Colonne manquante dans TT: {c}")

    # Nettoyage des chaînes
    for c in ["Nom", "Prenom", "Date_de_naissance"]:
        df_gel[c] = df_gel[c].apply(clean_string)
        df_tt[c]  = df_tt[c].apply(clean_string)

    # Fusion sur IdRegistre
    df_merged = df_gel.merge(df_tt, on="IdRegistre", how="left", suffixes=("_gel", "_tt"))

    compare_cols = ["Nom", "Prenom", "Date_de_naissance"]

    # Comparaison ligne par ligne avec to_numpy pour éviter l'erreur d'index
    mask = (df_merged[[c+"_gel" for c in compare_cols]].to_numpy() !=
            df_merged[[c+"_tt" for c in compare_cols]].to_numpy()).any(axis=1)

    df_diff = df_merged[mask]

    # DataFrame final à renvoyer
    df_modif = df_diff[["IdRegistre"] + [c+"_gel" for c in compare_cols]]
    df_modif.columns = ["IdRegistre"] + compare_cols

    # Conversion NaN → None pour JSON
    df_modif = df_modif.where(pd.notna(df_modif), None)

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
    app.run(host="0.0.0.0", port=port)
