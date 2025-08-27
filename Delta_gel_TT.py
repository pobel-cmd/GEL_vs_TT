from flask import Flask, request, jsonify
import pandas as pd
import requests
import os

app = Flask(__name__)

TMP_DIR = "/tmp"
os.makedirs(TMP_DIR, exist_ok=True)

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
        # Téléchargement CSV
        gel_path = os.path.join(TMP_DIR, "gel.csv")
        tt_path = os.path.join(TMP_DIR, "tt.csv")
        r1 = requests.get(gel_url); r1.raise_for_status()
        r2 = requests.get(tt_url); r2.raise_for_status()
        with open(gel_path, "wb") as f: f.write(r1.content)
        with open(tt_path, "wb") as f: f.write(r2.content)

        # Lecture CSV et comparaison
        df_gel = pd.read_csv(gel_path)
        df_tt = pd.read_csv(tt_path)

        # Supprimer Date_Publication
        for col in ["Date_Publication", "Date dernière publication"]:
            if col in df_gel.columns: df_gel = df_gel.drop(columns=[col])
            if col in df_tt.columns: df_tt = df_tt.drop(columns=[col])

        # Fusion sur Id registre
        df_merged = df_gel.merge(df_tt, on="Id registre", how="left", suffixes=("_gel","_tt"))

        compare_cols = [c for c in df_gel.columns if c != "Id registre"]
        mask = (df_merged[[c+"_gel" for c in compare_cols]] != df_merged[[c+"_tt" for c in compare_cols]]).any(axis=1)
        df_diff = df_merged[mask]

        df_modif = df_diff[["Id registre"] + [c+"_gel" for c in compare_cols]]
        df_modif.columns = ["Id registre"] + compare_cols

        modifications = df_modif.to_dict(orient="records")
        return jsonify({"status": "ok", "message": "Comparaison terminée", "modifications": modifications})

    except Exception as e:
        return jsonify({"status": "error", "message": str(e)}), 500

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 10000))
    app.run(host="0.0.0.0", port=port)
