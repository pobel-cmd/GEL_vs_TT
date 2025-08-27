from flask import Flask, request, jsonify
import pandas as pd
import os
import io
import json

from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseDownload

app = Flask(__name__)

# Lecture de la clé JSON du compte service depuis la variable d'environnement
SERVICE_KEY_JSON = os.environ.get("GOOGLE_SERVICE_KEY")
if not SERVICE_KEY_JSON:
    raise RuntimeError("La variable d'environnement GOOGLE_SERVICE_KEY n'est pas définie.")

SERVICE_ACCOUNT_INFO = json.loads(SERVICE_KEY_JSON)
SCOPES = ["https://www.googleapis.com/auth/drive.readonly"]
CREDENTIALS = service_account.Credentials.from_service_account_info(
    SERVICE_ACCOUNT_INFO, scopes=SCOPES
)
DRIVE_SERVICE = build("drive", "v3", credentials=CREDENTIALS)

def download_csv_from_drive(file_id):
    """Télécharge un fichier CSV depuis Google Drive et retourne un DataFrame pandas"""
    request_drive = DRIVE_SERVICE.files().get_media(fileId=file_id)
    fh = io.BytesIO()
    downloader = MediaIoBaseDownload(fh, request_drive)
    done = False
    while not done:
        status, done = downloader.next_chunk()
    fh.seek(0)
    df = pd.read_csv(fh)
    return df

def compare_csv(df_gel, df_tt):
    """Compare les deux DataFrame et retourne les modifications"""
    # Supprimer les colonnes publication pour ne pas générer de faux delta
    for col in ["Date Publication", "Date dernière publication", "Date_publication"]:
        if col in df_gel.columns:
            df_gel = df_gel.drop(columns=[col])
        if col in df_tt.columns:
            df_tt = df_tt.drop(columns=[col])

    # Harmoniser le nom de colonne clé
    if "ID registre" in df_gel.columns:
        df_gel.rename(columns={"ID registre": "IdRegistre"}, inplace=True)
    if "ID registre" in df_tt.columns:
        df_tt.rename(columns={"ID registre": "IdRegistre"}, inplace=True)

    # Fusionner sur IdRegistre
    df_merged = df_gel.merge(df_tt, on="IdRegistre", how="left", suffixes=("_gel", "_tt"))

    compare_cols = [c for c in df_gel.columns if c != "IdRegistre"]
    mask = (df_merged[[c+"_gel" for c in compare_cols]] != df_merged[[c+"_tt" for c in compare_cols]]).any(axis=1)
    df_diff = df_merged[mask]

    df_modif = df_diff[["IdRegistre"] + [c+"_gel" for c in compare_cols]]
    df_modif.columns = ["IdRegistre"] + compare_cols
    return df_modif

@app.route("/compare", methods=["POST"])
def compare_endpoint():
    data = request.get_json()
    gel_file_id = data.get("gel_file_id")
    tt_file_id = data.get("tt_file_id")

    if not gel_file_id or not tt_file_id:
        return jsonify({"error": "Missing gel_file_id or tt_file_id"}), 400

    try:
        df_gel = download_csv_from_drive(gel_file_id)
        df_tt = download_csv_from_drive(tt_file_id)

        df_modif = compare_csv(df_gel, df_tt)
        modifications = df_modif.to_dict(orient="records")

        return jsonify({
            "status": "ok",
            "message": "Comparaison terminée",
            "modifications": modifications
        })

    except Exception as e:
        return jsonify({"status": "error", "message": str(e)}), 500

@app.route("/", methods=["GET"])
def home():
    return "✅ API Delta_gel_TT is running!"

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 10000))
    app.run(host="0.0.0.0", port=port)
