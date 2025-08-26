from flask import Flask, request, jsonify
import pandas as pd
import requests
import os

app = Flask(__name__)

# Dossier local pour stocker temporairement les fichiers
TMP_DIR = "/tmp"
os.makedirs(TMP_DIR, exist_ok=True)

def download_csv(url, filename):
    """Télécharge un CSV depuis Google Drive ou autre URL"""
    r = requests.get(url)
    r.raise_for_status()
    path = os.path.join(TMP_DIR, filename)
    with open(path, "wb") as f:
        f.write(r.content)
    return path

def compare_csv(gel_path, tt_path):
    """Compare les deux CSV et retourne uniquement les modifications"""
    df_gel = pd.read_csv(gel_path)
    df_tt = pd.read_csv(tt_path)

    # Supprimer Date_Publication pour ne pas compter comme modification
    if "Date_Publication" in df_gel.columns:
        df_gel = df_gel.drop(columns=["Date_Publication"])
    if "Date_Publication" in df_tt.columns:
        df_tt = df_tt.drop(columns=["Date_Publication"])

    # Fusionner sur IdRegistre et comparer les colonnes restantes
    df_merged = df_gel.merge(df_tt, on="IdRegistre", how="left", suffixes=("_gel", "_tt"))
    
    # Trouver les lignes où au moins une valeur diffère (hors IdRegistre)
    compare_cols = [c for c in df_gel.columns if c != "IdRegistre"]
    mask = (df_merged[[c+"_gel" for c in compare_cols]] != df_merged[[c+"_tt" for c in compare_cols]]).any(axis=1)
    
    df_diff = df_merged[mask]

    # Recréer CSV des modifications avec colonnes finales pour TimeTonic
    df_modif = df_diff[["IdRegistre"] + [c+"_gel" for c in compare_cols]]
    df_modif.columns = ["IdRegistre"] + compare_cols

    modif_path = os.path.join(TMP_DIR, "modifications.csv")
    df_modif.to_csv(modif_path, index=False)
    return modif_path

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
        modif_path = compare_csv(gel_path, tt_path)
        
        # Ici tu peux uploader modif_path sur Drive et renvoyer le lien
        # Pour l'instant, on renvoie juste le chemin local
        return jsonify({
            "status": "ok",
            "message": "Comparaison terminée",
            "modifications_file": modif_path
        })

    except Exception as e:
        return jsonify({"status": "error", "message": str(e)}), 500

if __name__ == "__main__":
    # Render utilise PORT fourni par l'environnement
    port = int(os.environ.get("PORT", 10000))
    app.run(host="0.0.0.0", port=port)
