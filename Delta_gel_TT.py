from flask import Flask, request, jsonify, send_file
import pandas as pd
import requests
import os
from io import StringIO

app = Flask(__name__)

# Fonction pour télécharger un CSV public depuis Google Drive
def download_csv_from_drive(file_id: str) -> pd.DataFrame:
    url = f"https://drive.google.com/uc?id={file_id}&export=download"
    response = requests.get(url)
    response.raise_for_status()
    return pd.read_csv(StringIO(response.text))

@app.route("/compare", methods=["POST"])
def compare_csv():
    try:
        # Le body JSON attendu contient les IDs de fichiers Drive
        data = request.json
        file_id_old = data.get("file_id_old")  # ex: TimeTonic
        file_id_new = data.get("file_id_new")  # ex: Gel des avoirs

        if not file_id_old or not file_id_new:
            return jsonify({"error": "Les deux paramètres file_id_old et file_id_new sont requis"}), 400

        # Téléchargement des fichiers
        df_old = download_csv_from_drive(file_id_old)
        df_new = download_csv_from_drive(file_id_new)

        # Vérification de la présence de la colonne clé
        if "IdRegistre" not in df_old.columns or "IdRegistre" not in df_new.columns:
            return jsonify({"error": "Les CSV doivent contenir une colonne 'IdRegistre' pour la comparaison"}), 400

        # On supprime la colonne Date_Publication si elle existe
        for df in [df_old, df_new]:
            if "Date_Publication" in df.columns:
                df.drop(columns=["Date_Publication"], inplace=True)

        # Fusion des deux datasets
        df_merged = df_old.merge(df_new, on="IdRegistre", how="outer", indicator=True, suffixes=("_old", "_new"))

        results = []

        # Ajouts
        added = df_merged[df_merged["_merge"] == "right_only"]
        for _, row in added.iterrows():
            new_data = {col.replace("_new", ""): row[col] for col in row.index if col.endswith("_new")}
            new_data["IdRegistre"] = row["IdRegistre"]
            new_data["Action"] = "ajout"
            results.append(new_data)

        # Suppressions
        removed = df_merged[df_merged["_merge"] == "left_only"]
        for _, row in removed.iterrows():
            old_data = {col.replace("_old", ""): row[col] for col in row.index if col.endswith("_old")}
            old_data["IdRegistre"] = row["IdRegistre"]
            old_data["Action"] = "suppression"
            results.append(old_data)

        # Modifications
        common = df_merged[df_merged["_merge"] == "both"]
        cols_old = [c for c in common.columns if c.endswith("_old")]
        cols_new = [c for c in common.columns if c.endswith("_new")]

        for _, row in common.iterrows():
            diff = False
            changes = {}
            for col_old, col_new in zip(cols_old, cols_new):
                if row[col_old] != row[col_new]:
                    diff = True
                changes[col_old.replace("_old", "")] = row[col_new]
            if diff:
                changes["IdRegistre"] = row["IdRegistre"]
                changes["Action"] = "modification"
                results.append(changes)

        # Conversion en DataFrame
        if not results:
            return jsonify({"message": "Aucune différence trouvée"}), 200

        delta_df = pd.DataFrame(results)

        # Sauvegarde en CSV temporaire
        output_file = "/tmp/delta.csv"
        delta_df.to_csv(output_file, index=False)

        # Retourne le CSV généré
        return send_file(output_file, mimetype="text/csv", as_attachment=True, download_name="delta.csv")

    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route("/")
def home():
    return "API Delta prête 🚀. POST /compare avec {file_id_old, file_id_new}."


if __name__ == "__main__":
    import os
    port = int(os.environ.get("PORT", 5000))
    app.run(host="0.0.0.0", port=port)