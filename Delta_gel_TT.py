from flask import Flask, request, jsonify
import pandas as pd
import requests
import os

app = Flask(__name__)

TMP_DIR = "/tmp"
os.makedirs(TMP_DIR, exist_ok=True)

# URLs brutes des CSV sur GitHub
URL_GEL = "https://raw.githubusercontent.com/pobel-cmd/csv-storage/refs/heads/main/test-GEL.csv"
URL_TT = "https://raw.githubusercontent.com/pobel-cmd/csv-storage/refs/heads/main/test-TT.csv"

def download_csv(url, local_filename):
    """Télécharge le CSV depuis GitHub"""
    r = requests.get(url)
    r.raise_for_status()
    path = os.path.join(TMP_DIR, local_filename)
    with open(path, "wb") as f:
        f.write(r.content)
    return path

def normalize_columns(df):
    """Nettoie les noms de colonnes et renomme pour correspondre"""
    df.columns = [c.strip() for c in df.columns]
    rename_map = {'ID registre': 'Id registre', 'Date de naissance (texte)': 'Date de naissance'}
    df.rename(columns=rename_map, inplace=True)
    return df

@app.route("/", methods=["GET"])
def home():
    return "✅ API Delta_gel_TT is running!"

@app.route("/compare", methods=["POST"])
def compare():
    try:
        # Téléchargement des CSV
        path_gel = download_csv(URL_GEL, "test-GEL.csv")
        path_tt = download_csv(URL_TT, "test-TT.csv")

        # Lecture avec pandas
        df_gel = pd.read_csv(path_gel)
        df_tt = pd.read_csv(path_tt)

        # Normalisation des colonnes
        df_gel = normalize_columns(df_gel)
        df_tt = normalize_columns(df_tt)

        # Vérification colonnes essentielles
        required_cols = ['Id registre', 'Nom', 'Prenom', 'Date de naissance']
        missing_gel = [c for c in required_cols if c not in df_gel.columns]
        missing_tt = [c for c in required_cols if c not in df_tt.columns]

        if missing_gel:
            return jsonify({"status":"error", "message": f"Colonnes manquantes dans GEL: {missing_gel}"})
        if missing_tt:
            return jsonify({"status":"error", "message": f"Colonnes manquantes dans TT: {missing_tt}"})

        # Comparaison basique : Id registre présent dans GEL mais pas dans TT
        df_merge = df_gel.merge(df_tt, on='Id registre', how='left', indicator=True)
        missing_in_tt = df_merge[df_merge['_merge']=='left_only']

        result = missing_in_tt[required_cols].to_dict(orient='records')

        return jsonify({"status":"success", "missing_in_TT": result})

    except Exception as e:
        return jsonify({"status":"error", "message": str(e)}), 500

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=int(os.environ.get("PORT", 5000)))