from flask import Flask, request, jsonify
import pandas as pd
import requests
import os

app = Flask(__name__)

TMP_DIR = "/tmp"
os.makedirs(TMP_DIR, exist_ok=True)

def download_csv(url, filename):
    r = requests.get(url)
    r.raise_for_status()
    path = os.path.join(TMP_DIR, filename)
    with open(path, "wb") as f:
        f.write(r.content)
    return path

def compare_csv(gel_path, tt_path):
    df_gel = pd.read_csv(gel_path)
    df_tt = pd.read_csv(tt_path)

    # Supprimer Date_Publication
    if "Date_Publication" in df_gel.columns:
        df_gel = df_gel.drop(columns=["Date_Publication"])
    if "Date_Publication" in df_tt.columns:
        df_tt = df_tt.drop(columns=["Date_Publication"])

    # Fusion sur IdRegistre
    df_merged = df_gel.merge(df_tt, on="IdRegistre", how="left", suffixes=("_gel", "_tt"))
    
    # Colonnes à comparer
    compare_cols = [c for c in df_gel.columns if c != "IdRegistre"]
    mask = (df_merged[[c+"_gel" for c in compare_cols]] != df_merged[[c+"_tt" for c in compare_cols]]).any(axis=1)
    df_diff = df_merged[mask]

    # Créer le DataFrame des modifs
    df_modif = df_diff[["IdRegistre"] + [c+"_gel" for c in compare_cols]]
    df_modif.columns = ["IdRegistre"] + compare_cols

    return df_modif

@app.route("/compare", methods=["POST"])
def compare_endpoint():
    data = request.get_json()
    gel_url = data.get("gel_csv_url")
    tt_url = data.get("tt_csv_url")

    if not gel_url or not tt_url:
        return jsonify({"error": "Missing gel_csv_url or tt_csv_url"}), 400

    try:
        gel_path = download_csv(gel_url, "gel.csv")
        tt_path = download_csv(tt_url, "tt.csv")
        df_modif = compare_csv(gel_path, tt_path)

        # Convertir en liste de dictionnaires pour JSON
        modifications = df_modif.to_dict(orient="records")
        
        return jsonify({
            "status": "ok",
            "message": "Comparaison terminée",
            "modifications": modifications
        })

    except Exception as e:
        return jsonify({"status": "error", "message": str(e)}), 500

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 10000))
    app.run(host="0.0.0.0", port=port)