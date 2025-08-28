from flask import Flask, request, jsonify
import pandas as pd
from io import StringIO

app = Flask(__name__)

@app.route("/", methods=["POST"])
def compare_csv():
    try:
        data = request.json
        if not data or "csv1" not in data or "csv2" not in data:
            return jsonify({"error": "Les deux fichiers CSV (csv1 et csv2) sont requis."}), 400

        # Charger les CSV depuis les chaînes envoyées par Make
        csv1 = pd.read_csv(StringIO(data["csv1"]))
        csv2 = pd.read_csv(StringIO(data["csv2"]))

        # Trier et réindexer pour être sûr que la comparaison marche
        if "IdRegistre" in csv1.columns and "IdRegistre" in csv2.columns:
            csv1 = csv1.sort_values(by="IdRegistre").reset_index(drop=True)
            csv2 = csv2.sort_values(by="IdRegistre").reset_index(drop=True)

        # Aligner colonnes
        csv1, csv2 = csv1.align(csv2, join="outer", axis=1)

        # Comparer
        diff = csv1.compare(csv2, keep_shape=False, keep_equal=False)

        modifications = []
        for idx in diff.index:
            row = csv2.loc[idx]
            modifications.append(row.to_dict())

        return jsonify({
            "message": "Comparaison terminée",
            "nb_modifications": len(modifications),
            "modifications": modifications
        })

    except Exception as e:
        return jsonify({"error": str(e)}), 500


if __name__ == "__main__":
    # Local uniquement — en prod Render utilisera gunicorn
    app.run(host="0.0.0.0", port=10000, debug=True)
