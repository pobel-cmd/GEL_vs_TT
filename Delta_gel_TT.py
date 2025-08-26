from flask import Flask, request, jsonify
import pandas as pd
import requests
import os
import traceback

app = Flask(__name__)

# Dossier local temporaire
TMP_DIR = "/tmp"
os.makedirs(TMP_DIR, exist_ok=True)

def download_csv(url, filename):
    """Télécharge un CSV depuis une URL et retourne le chemin local"""
    print(f"Téléchargement du fichier depuis {url}")
    r = requests.get(url)
    r.raise_for_status()  # plante si erreur HTTP
    path = os.path.join(TMP_DIR, filename)
    with open(path, "wb") as f:
        f.write(r.content)
    print(f"Fichier sauvegardé dans {path}")
    return path

def compare_csv(gel_path, tt_path):
    """Compare deux CSV et retourne uniquement les modifications"""
    df_gel = pd.read_csv(gel_path)
    df_tt = pd.read_csv(tt_path)

    # Vérifier colonnes attendues
    required_cols = ["IdRegistre", "Nature", "Nom", "Prenom", "Date_de_naissance", "Alias"]
    for col in required_cols:
        if col not in df_gel.columns:
            raise ValueError(f"Colonne manquante dans GEL CSV: {col}")
        if col not in df_tt.columns:
            raise ValueError(f"Colonne manquante dans TT CSV: {col}")

    # Supprimer Date_Publication si présente
    for df in [df_gel, df_tt]:
        if "Date_Publication" in df.columns:
            df.drop(columns=["Date_Publication"], inplace=True)

    # Fusionner sur IdRegistre
    df_merged = df_gel.merge(df_tt, on="IdRegistre", how="left", suffixes=("_gel", "_tt"))

    compare_cols = [c for c in required_cols if c != "IdRegistre"]
    mask = (df_merged[[c+"_gel" for c in compare_cols]] != df_merged[[c+"_tt" for c in compare_cols]]).any(axis=1)

    df_diff = df_merged[mask]
    df_modif = df_diff[["IdRegistre"] + [c+"_gel" for c in compare_cols]]
    df_modif.columns = ["IdRegistre"] + compare_cols

    modif_path = os.path.join(TMP_DIR, "modifications.csv")
    df_modif.to_csv(modif_path, index=False)
    print(f"Fichier des modifications généré : {modif_path}")
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

        return jsonify({
            "status": "ok",
            "message": "Comparaison terminée",
            "modifications_file": modif_path
        })

    except Exception as e:
        return jsonify({
            "status": "error",
            "message": str(e),
            "trace": traceback.format_exc()
        }), 500

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 10000))
    app.run(host="0.0.0.0", port=port)
