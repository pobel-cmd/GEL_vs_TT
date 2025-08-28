from flask import Flask, request, jsonify
import pandas as pd
import io

app = Flask(__name__)

def compare_csv_data(csv1_content, csv2_content):
    """Compare deux CSV et retourne les lignes modifiées"""
    # Lecture des CSV depuis la chaîne
    df_gel = pd.read_csv(io.StringIO(csv1_content))
    df_tt  = pd.read_csv(io.StringIO(csv2_content))

    # Colonnes essentielles
    required = ["IdRegistre", "Nom", "Prenom", "Date_de_naissance"]
    for df, name in zip([df_gel, df_tt], ["GEL", "TT"]):
        if not all(col in df.columns for col in required):
            raise ValueError(f"Colonnes manquantes dans {name}: {required}")

    # Fusion sur IdRegistre
    df_merged = df_gel.merge(df_tt, on="IdRegistre", how="left", suffixes=("_gel", "_tt"))

    # Comparaison
    compare_cols = ["Nom", "Prenom", "Date_de_naissance"]
    mask = (df_merged[[c+"_gel" for c in compare_cols]] != df_merged[[c+"_tt" for c in compare_cols]]).any(axis=1)

    df_diff = df_merged[mask]
    df_modif = df_diff[["IdRegistre"] + [c+"_gel" for c in compare_cols]]
    df_modif.columns = ["IdRegistre"] + compare_cols

    return df_modif

@app.route("/", methods=["GET"])
def home():
    return "✅ API Delta_gel_TT is running!"

@app.route("/upload_csv", methods=["POST"])
def upload_csv():
    """Recevoir les CSV via multipart/form-data depuis Make"""
    try:
        gel_file = request.files.get("gel")
        tt_file  = request.files.get("tt")

        if gel_file is None or tt_file is None:
            return jsonify({"status": "error", "message": "Fichiers gel et tt requis"}), 400

        gel_content = gel_file.read().decode("utf-8")
        tt_content  = tt_file.read().decode("utf-8")

        df_modif = compare_csv_data(gel_content, tt_content)
        modifications = df_modif.to_dict(orient="records")

        return jsonify({
            "status": "ok",
            "message": "Comparaison terminée",
            "modifications": modifications
        })

    except Exception as e:
        return jsonify({"status": "error", "message": str(e)}), 200

@app.route("/compare", methods=["POST"])
def compare_json():
    """Recevoir les CSV en JSON via Make"""
    try:
        data = request.get_json(force=True)
        csv1 = data.get("csv1")
        csv2 = data.get("csv2")

        if not csv1 or not csv2:
            return jsonify({"status": "error", "message": "csv1 et csv2 requis"}), 400

        df_modif = compare_csv_data(csv1, csv2)
        modifications = df_modif.to_dict(orient="records")

        return jsonify({
            "status": "ok",
            "message": "Comparaison terminée",
            "modifications": modifications
        })

    except Exception as e:
        return jsonify({"status": "error", "message": str(e)}), 200

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=10000, debug=True)
