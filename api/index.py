import os
import re
import threading
from typing import Any

import numpy as np
import pandas as pd
import pyarrow.csv as pacsv
import requests
from flask import Flask, jsonify, render_template, request

app = Flask(__name__, template_folder="../templates")
app.config["JSON_SORT_KEYS"] = False

BIN_DATABASE_PATH = os.getenv("BIN_DATABASE_PATH", "templates/bin_database.csv")
VALIDATION_TIMEOUT = float(os.getenv("VALIDATION_TIMEOUT", "12"))
MAX_BATCH_SIZE = int(os.getenv("MAX_BATCH_SIZE", "200000"))
PUBLIC_BIN_API = os.getenv("PUBLIC_BIN_API", "https://lookup.binlist.net")

BIN_WEB_CACHE: dict[str, dict[str, Any]] = {}
BIN_WEB_CACHE_LOCK = threading.Lock()
_LOCAL_BIN_DB: dict[str, dict[str, Any]] | None = None

LINE_PATTERN = re.compile(
    r"(?P<number>\d{15,16})\D+(?P<month>0[1-9]|1[0-2])\D+(?P<year>\d{2}|\d{4})\D+(?P<cvv>\d{3,4})(?!\d)"
)


def _sanitize(value: Any) -> str:
    return "".join(ch for ch in str(value).strip() if ch.isdigit())


def _to_yy(year: str) -> str:
    return year[-2:]


def parse_line(raw: Any) -> dict[str, Any] | None:
    text = str(raw).strip()
    match = LINE_PATTERN.search(text)
    if not match:
        return None

    number = match.group("number")
    month = match.group("month")
    year = _to_yy(match.group("year"))
    cvv = match.group("cvv")

    return {
        "input": text,
        "number": number,
        "month": month,
        "year": year,
        "cvv": cvv,
        "formatted": f"{number}|{month}|{year}|{cvv}",
    }


def luhn_batch(numbers: list[str]) -> np.ndarray:
    if not numbers:
        return np.array([], dtype=bool)

    max_len = max((len(item) for item in numbers), default=0)
    if max_len == 0:
        return np.zeros(len(numbers), dtype=bool)

    digits = np.full((len(numbers), max_len), -1, dtype=np.int16)
    lengths = np.array([len(item) for item in numbers], dtype=np.int32)

    for idx, value in enumerate(numbers):
        if value:
            digits[idx, max_len - len(value) :] = np.fromiter((int(ch) for ch in value), dtype=np.int16)

    reverse = digits[:, ::-1]
    positions = np.arange(max_len)
    double_mask = (positions % 2 == 1)[None, :]

    work = np.where(double_mask & (reverse >= 0), reverse * 2, reverse)
    work = np.where(work > 9, work - 9, work)
    work = np.where(work < 0, 0, work)

    checksums = work.sum(axis=1)
    return (checksums % 10 == 0) & (lengths > 0)


def _normalize_enrichment(entry: dict[str, Any], source: str) -> dict[str, Any]:
    return {
        "source": source,
        "bank": str(entry.get("bank") or "N/A"),
        "brand": str(entry.get("brand") or "N/A"),
        "type": str(entry.get("type") or "N/A"),
        "country": str(entry.get("country") or "N/A"),
    }


def load_bin_database() -> dict[str, dict[str, Any]]:
    global _LOCAL_BIN_DB
    if _LOCAL_BIN_DB is not None:
        return _LOCAL_BIN_DB

    try:
        if not os.path.exists(BIN_DATABASE_PATH):
            _LOCAL_BIN_DB = {}
            return _LOCAL_BIN_DB

        table = pacsv.read_csv(BIN_DATABASE_PATH)
        df = table.to_pandas(types_mapper=pd.ArrowDtype)
        df = df.fillna("")

        db: dict[str, dict[str, Any]] = {}
        for row in df.to_dict(orient="records"):
            key = _sanitize(row.get("bin", ""))[:6]
            if key:
                db[key] = {
                    "bank": row.get("bank", ""),
                    "brand": row.get("brand", ""),
                    "type": row.get("type", ""),
                    "country": row.get("country", ""),
                }

        _LOCAL_BIN_DB = db
        return _LOCAL_BIN_DB
    except Exception as exc:
        app.logger.exception("Failed to load local BIN database: %s", exc)
        _LOCAL_BIN_DB = {}
        return _LOCAL_BIN_DB


def fetch_bin_from_web(bin_code: str) -> dict[str, Any]:
    if not bin_code:
        return _normalize_enrichment({}, "none")

    with BIN_WEB_CACHE_LOCK:
        if bin_code in BIN_WEB_CACHE:
            return BIN_WEB_CACHE[bin_code]

    headers = {"Accept-Version": "3", "User-Agent": "wow/7.0"}
    url = f"{PUBLIC_BIN_API.rstrip('/')}/{bin_code}"

    try:
        response = requests.get(url, headers=headers, timeout=VALIDATION_TIMEOUT)
        response.raise_for_status()
        payload = response.json()

        result = _normalize_enrichment(
            {
                "bank": (payload.get("bank") or {}).get("name", "N/A"),
                "brand": payload.get("scheme", "N/A"),
                "type": payload.get("type", "N/A"),
                "country": (payload.get("country") or {}).get("name", "N/A"),
            },
            "web",
        )
    except requests.Timeout:
        result = _normalize_enrichment({"bank": "Timeout"}, "web_timeout")
    except requests.RequestException as exc:
        result = _normalize_enrichment({"bank": f"Error: {exc}"}, "web_error")
    except ValueError:
        result = _normalize_enrichment({"bank": "Invalid JSON"}, "web_error")

    with BIN_WEB_CACHE_LOCK:
        BIN_WEB_CACHE[bin_code] = result

    return result


def enrich_record(number: str, local_db: dict[str, dict[str, Any]]) -> dict[str, Any]:
    bin_code = number[:6] if len(number) >= 6 else ""
    if bin_code and bin_code in local_db:
        return _normalize_enrichment(local_db[bin_code], "local")
    if bin_code:
        return fetch_bin_from_web(bin_code)
    return _normalize_enrichment({}, "none")


@app.get("/")
def dashboard() -> str:
    return render_template("index.html")


@app.post("/api/process")
def process_batch():
    try:
        payload = request.get_json(silent=True) or {}
        records = payload.get("records", [])

        if isinstance(records, str):
            records = [line for line in records.splitlines() if line.strip()]

        if not isinstance(records, list):
            return jsonify({"error": "'records' must be a list or multiline string"}), 400

        if len(records) > MAX_BATCH_SIZE:
            return jsonify({"error": f"Max batch size is {MAX_BATCH_SIZE}"}), 413

        parsed: list[dict[str, Any]] = []
        invalid_results: list[dict[str, Any]] = []

        for raw in records:
            item = parse_line(raw)
            if item is None:
                invalid_results.append(
                    {
                        "input": str(raw),
                        "normalized": _sanitize(raw),
                        "reason": "Formato incompleto. Se requiere NUMERO|MES|AÑO|CVV",
                        "enrichment": _normalize_enrichment({}, "none"),
                    }
                )
            else:
                parsed.append(item)

        numbers = [item["number"] for item in parsed]
        luhn_results = luhn_batch(numbers)
        local_db = load_bin_database()

        valid_results: list[dict[str, Any]] = []
        for item, is_valid in zip(parsed, luhn_results.tolist()):
            enriched = enrich_record(item["number"], local_db)
            payload_item = {
                "input": item["input"],
                "number": item["number"],
                "month": item["month"],
                "year": item["year"],
                "cvv": item["cvv"],
                "formatted": item["formatted"],
                "enrichment": enriched,
            }
            if is_valid:
                valid_results.append(payload_item)
            else:
                invalid_results.append(
                    {
                        **payload_item,
                        "reason": "Luhn inválido",
                    }
                )

        return jsonify(
            {
                "total": len(records),
                "valid": len(valid_results),
                "invalid": len(invalid_results),
                "valid_results": valid_results,
                "invalid_results": invalid_results,
                "cache_size": len(BIN_WEB_CACHE),
            }
        )
    except MemoryError:
        return jsonify({"error": "Not enough memory for this payload"}), 507
    except Exception as exc:
        app.logger.exception("Unexpected processing error: %s", exc)
        return jsonify({"error": "Internal processing error", "details": str(exc)}), 500


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=7860)
