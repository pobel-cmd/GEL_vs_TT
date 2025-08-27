from flask import Flask, jsonify
import pandas as pd
import requests
import os

app = Flask(__name__)

TMP_DIR = "/tmp"
os.makedirs(TMP_DIR, exist_ok=True)

GEL_URL = "https://raw.githubusercontent.com/pobel-cmd/csv-storage/refs/heads/main/test-GEL.csv"
TT_URL  = "https://raw.githubusercontent.com/pobel-cmd/csv-storage/refs/heads/main/test-TT.csv"

def download_csv(url, filename):
    path = os.path.join(TMP_DIR, filename)
    r = requests.get(url, timeout=20)
    r.raise_for_status()
    with open(path, "wb") as f:
        f.write(r.content)
    return path

def compare_csv(gel_path, tt_path):
    df_gel = pd.read_csv(gel_path)
    df_tt  = pd.read_csv(tt_path)

    required_cols = ["IdRegistre", "Nom", "Prenom", "Date_de_naissance"]
    for col in required_cols:
        if col not in df_gel.columns:
            raise ValueError(f"Colonne manquante dans GEL: {col}")
        if col not in df_tt.columns:
            raise ValueError(f"Colonne manquante dans TT: {col}")

    # Remplacer NaN par vide pour comparaison
    df_gel_filled = df_gel.fillna("")
    df_tt_filled  = df_tt.fillna("")

    # Fusion sur IdRegistre
    df_merged = pd.merge(df_gel_filled, df_tt_filled, on="IdRegistre", how="outer", suffixes=("_gel","_tt"))

    compare_cols = ["Nom", "Prenom", "Date_de_naissance"]
    
    # Créer un masque pour chaque colonne
    mask = pd.Series(False, index=df_merged.index)
    for col in compare_cols:
        mask |= df_merged[f"{col}_gel"] != df_merged[f"{col}_tt"]

    # Filtrer uniquement les lignes avec au moins une différence
    df_diff = df_merged[mask]

    # Préparer le JSON final
    df_modif = df_diff[["IdRegistre"] + [f"{c}_gel" for c in compare_cols]]
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
        return jsonify({"status": "ok", "message": "Comparaison terminée", "modifications": df_modif.to_dict(orient="records")})
    except Exception as e:
        return jsonify({"status": "error", "message": str(e)}), 200

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 10000))
    app.run(host="0.0.0.0", port=port)
