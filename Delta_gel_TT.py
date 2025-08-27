from flask import Flask, request, jsonify
import pandas as pd
import requests
import os
import traceback

app = Flask(__name__)

TMP_DIR = "/tmp"
os.makedirs(TMP_DIR, exist_ok=True)

def download_csv(url, filename):
    """Télécharge un CSV depuis Google Drive ou autre URL"""
    r = requests.get(url)
    r.raise_for_status()
    # Vérifier que le contenu ressemble à un CSV
    if 'text/csv' not in r.headers.get('Content-Type', '') and not r.content.startswith(b'ID registre'):
        raise ValueError(f"Le fichier téléchargé ne semble pas être un CSV : {url}")
    path = os.path.join(TMP_DIR, filename)
    with open(path, "wb") as f:
        f.write(r.content)
    return path

def compare_csv(gel_path, tt_path):
    """Compare les deux CSV et retourne uniquement les modifications"""
    df_gel = pd.read_csv(gel_path)
    df_tt = pd.read_csv(tt_path)

    # Supprimer Date_Publication pour éviter les modifications inutiles
    if "Date publication" in df_gel.columns:
        df_gel = df_gel.drop(columns=["Date publication"])
    if "Date publication" in df_tt.columns:
        df_tt = df_tt.drop(columns=["Date publication"])

    # Fusionner sur Id registre
    df_merged = df_gel.merge(df_tt, on="Id registre", how="left", suffixes=("_gel", "_tt"))

    # Colonnes à comparer (hors Id registre)
    compare_cols = [c for c in df_gel.columns if c != "Id registre"]
    mask = (df_merged[[c+"_gel" for c in compare_cols]] != df_merged[[c+"_tt" for c in compare_cols]]).any(axis=1)
    df_diff = df_merged[mask]

    # Créer le DataFrame des modifications
    df_modif = df_diff[["Id registre"] + [c+"_gel" for c in compare_cols]]
    df_modif.columns = ["Id registre"] + compare_cols

    return df_modif

@app.route("/", methods=["GET"])
def home():
    return "✅ API Delta_gel_TT is running!"

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
        # Log complet sur Render pour debug
        print(traceback.format_exc())
        return jsonify({"status": "error", "message": str(e)}), 500

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 10000))
    app.run(host="0.0.0.0", port=port)
