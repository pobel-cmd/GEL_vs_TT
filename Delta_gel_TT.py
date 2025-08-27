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

def normalize_text(s):
    """Nettoie les chaînes : supprime espaces début/fin, met en minuscules, remplace NaN par ''"""
    if pd.isna(s):
        return ""
    return str(s).strip().lower()

def normalize_date(s):
    """Essaye de parser la date et renvoie sous format YYYY-MM-DD, sinon vide"""
    try:
        return pd.to_datetime(s, dayfirst=True, errors='coerce').strftime("%Y-%m-%d")
    except:
        return ""

def compare_csv(gel_path, tt_path):
    """Compare les deux CSV et retourne les modifications"""
    try:
        df_gel = pd.read_csv(gel_path)
        df_tt  = pd.read_csv(tt_path)
    except Exception as e:
        raise ValueError(f"Impossible de lire les CSV: {e}")

    # Colonnes essentielles
    required_gel = ["IdRegistre", "Nom", "Prenom", "Date_de_naissance"]
    required_tt  = ["IdRegistre", "Nom", "Prenom", "Date_de_naissance"]

    if not all(c in df_gel.columns for c in required_gel):
        raise ValueError(f"Colonnes manquantes dans GEL: {required_gel}")
    if not all(c in df_tt.columns for c in required_tt):
        raise ValueError(f"Colonnes manquantes dans TT: {required_tt}")

    # Normalisation
    for c in ["Nom", "Prenom"]:
        df_gel[c] = df_gel[c].apply(normalize_text)
        df_tt[c]  = df_tt[c].apply(normalize_text)
    df_gel["Date_de_naissance"] = df_gel["Date_de_naissance"].apply(normalize_date)
    df_tt["Date_de_naissance"]  = df_tt["Date_de_naissance"].apply(normalize_date)

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

        modifications = df_modif.to_dict(orient="records")

        return jsonify({
            "status": "ok",
            "message": "Comparaison terminée",
            "modifications": modifications
        })

    except Exception as e:
        # Retour JSON en cas d'erreur
        return jsonify({"status": "error", "message": str(e)}), 200

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 10000))
    app.run(host="0.0.0.0", port=port)
