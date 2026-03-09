#!/usr/bin/env python3
"""Diagnóstico de métodos de pago para Braintree Sandbox.

Requisitos:
- Variables de entorno: BRAINTREE_MERCHANT_ID, BRAINTREE_PUBLIC_KEY, BRAINTREE_PRIVATE_KEY
- Archivo local de BIN: bin-database.csv
"""

from __future__ import annotations

import json
import os
from dataclasses import dataclass
from typing import Any

import braintree
import pandas as pd

PROCESSOR_CODE_MESSAGES = {
    "2000": "Do Not Honor: el banco emisor rechazó la transacción.",
    "2001": "Insufficient Funds: fondos insuficientes.",
    "2004": "Expired Card: la tarjeta está expirada.",
    "2005": "Invalid Credit Card Number: número de tarjeta inválido.",
    "2010": "CVV verification failed: CVV inválido.",
    "2015": "Transaction not allowed: no permitido por el emisor.",
    "2038": "Processor declined: rechazo general del procesador.",
}


@dataclass(slots=True)
class CardPayload:
    number: str
    month: str
    year: str
    cvv: str


def _read_card_attr(card: Any, name: str) -> str:
    value = getattr(card, name, "")
    return str(value).strip()


def _clean_digits(value: str) -> str:
    return "".join(ch for ch in value if ch.isdigit())


def load_bin_dataframe(bin_database_path: str = "bin-database.csv") -> pd.DataFrame:
    df = pd.read_csv(bin_database_path, dtype=str, keep_default_na=False)
    df.columns = [str(c).strip().lower() for c in df.columns]

    if "bin" not in df.columns:
        raise ValueError("El archivo bin-database.csv debe incluir la columna 'bin'.")

    df["bin"] = df["bin"].astype(str).str.replace(r"\D", "", regex=True).str.slice(0, 6)
    df = df[df["bin"].str.len() == 6]
    return df


def lookup_issuer(df: pd.DataFrame, card_number: str) -> dict[str, str]:
    bin6 = _clean_digits(card_number)[:6]
    if not bin6:
        return {"bin": "", "bank": "UNKNOWN", "type": "UNKNOWN"}

    matched = df[df["bin"] == bin6]
    if matched.empty:
        return {"bin": bin6, "bank": "UNKNOWN", "type": "UNKNOWN"}

    row = matched.iloc[0].to_dict()
    return {
        "bin": bin6,
        "bank": str(row.get("bank") or row.get("issuer") or "UNKNOWN"),
        "type": str(row.get("type") or "UNKNOWN"),
    }


def _build_gateway() -> braintree.BraintreeGateway:
    merchant_id = os.getenv("BRAINTREE_MERCHANT_ID", "").strip()
    public_key = os.getenv("BRAINTREE_PUBLIC_KEY", "").strip()
    private_key = os.getenv("BRAINTREE_PRIVATE_KEY", "").strip()

    if not merchant_id or not public_key or not private_key:
        raise EnvironmentError(
            "Credenciales incompletas. Configura BRAINTREE_MERCHANT_ID, "
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


def verify_payment_method_diagnostics(card: Any, bin_database_path: str = "bin-database.csv") -> dict[str, Any]:
    """Verifica tarjeta en Braintree Vault y devuelve diagnóstico JSON-serializable."""
    payload = CardPayload(
        number=_clean_digits(_read_card_attr(card, "number")),
        month=_clean_digits(_read_card_attr(card, "month")),
        year=_clean_digits(_read_card_attr(card, "year")),
        cvv=_clean_digits(_read_card_attr(card, "cvv")),
    )

    issuer_details = {"bin": "", "bank": "UNKNOWN", "type": "UNKNOWN"}
    try:
        bin_df = load_bin_dataframe(bin_database_path)
        issuer_details = lookup_issuer(bin_df, payload.number)
    except Exception as exc:
        issuer_details["lookup_error"] = str(exc)

    if not payload.number or not payload.month or not payload.year or not payload.cvv:
        return {
            "status": "error",
            "verified": False,
            "message": "Payload de tarjeta incompleto.",
            "processor_response_code": None,
            "issuer": issuer_details,
        }

    try:
        gateway = _build_gateway()

        result = gateway.credit_card.create(
            {
                "customer_id": "diagnostics_customer",
                "number": payload.number,
                "expiration_month": payload.month,
                "expiration_year": payload.year,
                "cvv": payload.cvv,
                "options": {
                    "verify_card": True,
                },
            }
        )

        if result.is_success:
            return {
                "status": "success",
                "verified": True,
                "message": "Método de pago verificado correctamente en Sandbox.",
                "processor_response_code": None,
                "issuer": issuer_details,
            }

        credit_card_verification = getattr(result, "credit_card_verification", None)
        processor_code = getattr(credit_card_verification, "processor_response_code", None)
        processor_text = getattr(credit_card_verification, "processor_response_text", None)

        human_message = PROCESSOR_CODE_MESSAGES.get(
            str(processor_code),
            processor_text or "Verificación fallida por el procesador.",
        )

        validation_errors = []
        if getattr(result, "errors", None):
            validation_errors = [
                {
                    "attribute": err.attribute,
                    "code": err.code,
                    "message": err.message,
                }
                for err in result.errors.deep_errors
            ]

        return {
            "status": "declined",
            "verified": False,
            "message": human_message,
            "processor_response_code": processor_code,
            "processor_response_text": processor_text,
            "validation_errors": validation_errors,
            "issuer": issuer_details,
        }

    except braintree.exceptions.AuthorizationError as exc:
        return {
            "status": "error",
            "verified": False,
            "message": "Error de autorización con Braintree.",
            "processor_response_code": None,
            "details": str(exc),
            "issuer": issuer_details,
        }
    except braintree.exceptions.ServerError as exc:
        return {
            "status": "error",
            "verified": False,
            "message": "Error interno del servidor Braintree.",
            "processor_response_code": None,
            "details": str(exc),
            "issuer": issuer_details,
        }
    except EnvironmentError as exc:
        return {
            "status": "error",
            "verified": False,
            "message": str(exc),
            "processor_response_code": None,
            "issuer": issuer_details,
        }
    except Exception as exc:  # fallback de red/parsing/SDK
        return {
            "status": "error",
            "verified": False,
            "message": "Error inesperado durante diagnóstico de pago.",
            "processor_response_code": None,
            "details": str(exc),
            "issuer": issuer_details,
        }


if __name__ == "__main__":
    sample_card = CardPayload(number="4111111111111111", month="12", year="2030", cvv="123")
    response = verify_payment_method_diagnostics(sample_card)
    print(json.dumps(response, indent=2, ensure_ascii=False))
