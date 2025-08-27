from flask import Flask, request, jsonify
import pandas as pd
import requests
import os
import hashlib

app = Flask(__name__)

TMP_DIR = "/tmp"
os.makedirs(TMP_DIR, exist_ok=True)

# Colonnes à comparer
COMPARE_COLS = ["Nature","Nom","Prenom","Date_de_naissance","Alias"]

def download_csv(url, filename):
    """Télécharge un CSV depuis Google Drive ou autre URL"""
    r = requests.get(url)
    r.raise_for_status()
    path = os.path.join(TMP_DIR, filename)
    with open(path, "wb") as f:
        f.write(r.content)
    return path

def hash_row(row):
    """Créer un hash pour une ligne pandas"""
    row_str = '|'.join([str(row[c]) if c in row else '' for c in COMPARE_COLS])
    return hashlib.md5(row_str.encode('utf-8')).hexdigest()

def compare_csv(gel_path, tt_path):
    """Compare les CSV via hash et retourne uniquement les modifications"""
    df_gel = pd.read_csv(gel_path, usecols=["IdRegistre"] + COMPARE_COLS, dtype=str)
    df_tt  = pd.read_csv(tt_path,  usecols=["IdRegistre"] + COMPARE_COLS, dtype=str)

    # Créer un hash de comparaison
    df_gel["row_hash"] = df_gel.apply(hash_row, axis=1)
    df_tt["row_hash"]  = df_tt.apply(hash_row, axis=1)

    # Left join sur IdRegistre
    df_merged = df_gel.merge(df_tt[["IdRegistre","row_hash"]], on="IdRegistre", how="left", suffixes=("_gel","_tt"))

    # Sélectionner uniquement les lignes modifiées ou nouvelles
    df_diff = df_merged[df_merged["row_hash_tt"].isna() | (df_merged["row_hash_gel"] != df_merged["row_hash_tt"])]

    df_modif = df_diff[["IdRegistre"] + COMPARE_COLS]

    modif_path = os.path.join(TMP_DIR, "modifications.csv")
    df_modif.to_csv(modif_path, index=False)
    return modif_path

@app.route("/compare", methods=["POST"])
def compare_endpoint():
    data = request.get_json()
    gel_url = data.get("gel_csv_url")
    tt_url  = data.get("tt_csv_url")

    if not gel_url or not tt_url:
        return jsonify({"error": "Missing gel_csv_url or tt_csv_url"}), 400

    try:
        gel_path = download_csv(gel_url, "gel.csv")
        tt_path  = download_csv(tt_url, "tt.csv")
        modif_path = compare_csv(gel_path, tt_path)

        # Pour l'instant, on renvoie juste le chemin local
        return jsonify({
            "status": "ok",
            "message": "Comparaison terminée",
            "modifications_file": modif_path
        })

    except Exception as e:
        return jsonify({"status": "error", "message": str(e)}), 500

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 10000))
    app.run(host="0.0.0.0", port=port)