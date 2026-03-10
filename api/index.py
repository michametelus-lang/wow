import os
from dataclasses import dataclass
from functools import lru_cache
from typing import Any
from uuid import uuid4

import braintree
import pandas as pd
from flask import Flask, jsonify, render_template, request

app = Flask(__name__, template_folder="../templates")
app.config["JSON_SORT_KEYS"] = False

BIN_DB_PATH = os.getenv("BIN_DB_PATH", "templates/bin-database.csv")
FALLBACK_BIN_DB_PATH = os.getenv("FALLBACK_BIN_DB_PATH", "templates/bin_database.csv")
PROCESSOR_CODE_MESSAGES = {
    "2000": "Do Not Honor: el banco emisor rechazó la operación.",
    "2001": "Insufficient Funds: fondos insuficientes.",
    "2004": "Expired Card: la tarjeta está expirada.",
    "2005": "Invalid Credit Card Number: número inválido.",
    "2010": "CVV verification failed: CVV inválido.",
    "2015": "Transaction not allowed: no permitido por el emisor.",
    "2038": "Processor declined: rechazo general del procesador.",
}

SANDBOX_ISSUER_HINTS = {
    "411111": "VISA SANDBOX",
    "400011": "VISA SANDBOX",
    "555555": "MASTERCARD SANDBOX",
    "510510": "MASTERCARD SANDBOX",
    "378282": "AMEX SANDBOX",
    "601111": "DISCOVER SANDBOX",
}


@dataclass(slots=True)
class CardPayload:
    identifier: str
    number: str
    month: str
    year: str
    cvv: str


def _clean_digits(value: str) -> str:
    return "".join(ch for ch in str(value) if ch.isdigit())


def _normalize_year(year: str) -> str:
    digits = _clean_digits(year)
    if len(digits) == 2:
        return f"20{digits}"
    return digits


def _normalize_month(month: str) -> str:
    digits = _clean_digits(month)
    if not digits:
        return ""
    if len(digits) == 1:
        digits = f"0{digits}"
    return digits


@lru_cache(maxsize=1)
def _load_bin_df() -> pd.DataFrame:
    selected_path = BIN_DB_PATH if os.path.exists(BIN_DB_PATH) else FALLBACK_BIN_DB_PATH
    try:
        df = pd.read_csv(selected_path, dtype=str, low_memory=False, keep_default_na=False)
    except Exception as exc:
        app.logger.warning("No se pudo cargar BIN DB local: %s", exc)
        return pd.DataFrame(columns=["bin", "bank"])

    df.columns = [str(c).strip().lower() for c in df.columns]
    if "bin" not in df.columns:
        df["bin"] = ""
    if "bank" not in df.columns and "issuer" in df.columns:
        df["bank"] = df["issuer"]
    if "bank" not in df.columns:
        df["bank"] = "UNKNOWN"

    df["bin"] = df["bin"].astype(str).str.replace(r"\D", "", regex=True).str.slice(0, 6)
    return df


def _issuer_from_number(number: str) -> str:
    bin6 = _clean_digits(number)[:6]
    if not bin6:
        return "UNKNOWN"
    df = _load_bin_df()
    match = df[df["bin"] == bin6]
    if match.empty:
        return SANDBOX_ISSUER_HINTS.get(bin6, "UNKNOWN")
    issuer = str(match.iloc[0].get("bank", "UNKNOWN")) or "UNKNOWN"
    if issuer == "UNKNOWN":
        issuer = SANDBOX_ISSUER_HINTS.get(bin6, "UNKNOWN")
    return issuer


def _build_gateway() -> braintree.BraintreeGateway:
    merchant_id = os.getenv("BRAINTREE_MERCHANT_ID", "").strip()
    public_key = os.getenv("BRAINTREE_PUBLIC_KEY", "").strip()
    private_key = os.getenv("BRAINTREE_PRIVATE_KEY", "").strip()

    if not merchant_id or not public_key or not private_key:
        raise EnvironmentError(
            "Faltan credenciales de Braintree. Define BRAINTREE_MERCHANT_ID, "
            "BRAINTREE_PUBLIC_KEY y BRAINTREE_PRIVATE_KEY."
        )

    return braintree.BraintreeGateway(
        braintree.Configuration(
            environment=braintree.Environment.Sandbox,
            merchant_id=merchant_id,
            public_key=public_key,
            private_key=private_key,
        )
    )


def _extract_verification(result: Any) -> Any:
    verification = None
    if getattr(result, "credit_card", None):
        verification = getattr(result.credit_card, "verification", None)
        if verification is None:
            verifications = getattr(result.credit_card, "verifications", None)
            if verifications:
                verification = verifications[0]
    if verification is None:
        verification = getattr(result, "credit_card_verification", None)
    return verification


def diagnose_payment_method_status(card: CardPayload) -> dict[str, Any]:
    issuer = _issuer_from_number(card.number)
    month = _normalize_month(card.month)
    year = _normalize_year(card.year)

    if not card.number or not month or not year or not card.cvv:
        return {
            "identifier": card.identifier,
            "issuer": issuer,
            "status": "DECLINED",
            "bank_result": "Payload incompleto",
            "response_code": None,
            "verification_status": None,
        }

    if len(_clean_digits(card.number)) < 12:
        return {
            "identifier": card.identifier,
            "issuer": issuer,
            "status": "DECLINED",
            "bank_result": "Número de tarjeta inválido (longitud insuficiente).",
            "response_code": "BT_LOCAL_FORMAT",
            "verification_status": None,
        }

    try:
        gateway = _build_gateway()
        customer_result = gateway.customer.create({"id": f"diag_{uuid4().hex[:18]}"})
        if not customer_result.is_success:
            raise RuntimeError("No se pudo crear customer temporal para vault verification")

        result = gateway.credit_card.create(
            {
                "customer_id": customer_result.customer.id,
                "number": _clean_digits(card.number),
                "expiration_month": month,
                "expiration_year": year,
                "cvv": _clean_digits(card.cvv),
                "options": {"verify_card": True},
            }
        )

        verification = _extract_verification(result)
        if verification is None:
            token = getattr(getattr(result, "credit_card", None), "token", None)
            if token:
                verify_result = gateway.credit_card.verify(token)
                verification = _extract_verification(verify_result)

        if verification is None:
            details = []
            if getattr(result, "errors", None):
                details = [err.message for err in result.errors.deep_errors]

            raw_message = " | ".join(details) if details else str(getattr(result, "message", "")).strip()
            if "not an accepted test number" in raw_message.lower():
                friendly = "Número inválido para Sandbox de Braintree (usa test numbers oficiales)."
                response_code = "BT_SANDBOX_TEST_NUMBER"
            else:
                friendly = raw_message or "Braintree no devolvió objeto de verificación."
                response_code = "BT_NO_VERIFICATION"

            return {
                "identifier": card.identifier,
                "issuer": issuer,
                "status": "DECLINED",
                "bank_result": friendly,
                "response_code": response_code,
                "verification_status": None,
                "raw_error": raw_message or None,
            }

        verification_status = str(getattr(verification, "status", "")).lower()
        response_code = getattr(verification, "processor_response_code", None)
        processor_text = getattr(verification, "processor_response_text", None)

        if verification_status == "verified":
            return {
                "identifier": card.identifier,
                "issuer": issuer,
                "status": "APPROVED",
                "bank_result": processor_text or "verified",
                "response_code": response_code,
                "verification_status": verification_status,
            }

        return {
            "identifier": card.identifier,
            "issuer": issuer,
            "status": "DECLINED",
            "bank_result": PROCESSOR_CODE_MESSAGES.get(str(response_code), processor_text or verification_status or "declined"),
            "response_code": response_code,
            "verification_status": verification_status,
        }

    except Exception as exc:
        return {
            "identifier": card.identifier,
            "issuer": issuer,
            "status": "DECLINED",
            "bank_result": f"Error: {exc}",
            "response_code": None,
            "verification_status": None,
        }


@app.get("/")
def index() -> str:
    return render_template("index.html")


@app.post("/validate")
def validate():
    payload: dict[str, Any] = request.get_json(silent=True) or {}
    card = CardPayload(
        identifier=str(payload.get("identifier", "")).strip(),
        number=str(payload.get("number", "")).strip(),
        month=str(payload.get("month", "")).strip(),
        year=str(payload.get("year", "")).strip(),
        cvv=str(payload.get("cvv", "")).strip(),
    )
    result = diagnose_payment_method_status(card)
    return jsonify(result), 200


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=7860)
