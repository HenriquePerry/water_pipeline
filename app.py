from flask import Flask, jsonify
from dotenv import load_dotenv

from water_pipeline import run_pipeline

load_dotenv()

app = Flask(__name__)


@app.get("/health")
def health() -> tuple[dict[str, str], int]:
    return {"status": "ok"}, 200


@app.post("/run")
def run() -> tuple[dict, int]:
    result = run_pipeline()
    status_code = 200 if result.get("status") == "ok" else 500
    return jsonify(result), status_code


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000, debug=True)
