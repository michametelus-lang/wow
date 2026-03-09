#!/usr/bin/env python3
"""Diagnóstico robusto de métodos de pago con Braintree Sandbox."""

from __future__ import annotations

import json
import os
from dataclasses import dataclass
from typing import Any

import braintree
import pandas as pd

PROCESSOR_CODE_MESSAGES = {
    "2000": "Do Not Honor: el banco emisor rechazó la operación.",
    "2001": "Insufficient Funds: fondos insuficientes.",
    "2004": "Expired Card: la tarjeta está expirada.",
    "2005": "Invalid Credit Card Number: número inválido.",
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
    return str(getattr(card, name, "")).strip()


def _clean_digits(value: str) -> str:
    return "".join(ch for ch in value if ch.isdigit())


def load_bin_dataframe(bin_database_path: str = "templates/bin-database.csv") -> pd.DataFrame:
    df = pd.read_csv(bin_database_path, dtype=str, keep_default_na=False)
    df.columns = [str(c).strip().lower() for c in df.columns]

    if "bin" not in df.columns:
        raise ValueError("La base local debe incluir la columna 'bin'.")

    df["bin"] = df["bin"].astype(str).str.replace(r"\D", "", regex=True).str.slice(0, 6)
    df = df[df["bin"].str.len() == 6]
    return df


def lookup_issuer(df: pd.DataFrame, card_number: str) -> dict[str, str]:
    bin6 = _clean_digits(card_number)[:6]
    if not bin6:
        return {"bin": "", "bank": "UNKNOWN"}

    matched = df[df["bin"] == bin6]
    if matched.empty:
        return {"bin": bin6, "bank": "UNKNOWN"}

    row = matched.iloc[0].to_dict()
    return {"bin": bin6, "bank": str(row.get("bank") or row.get("issuer") or "UNKNOWN")}


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


def _diagnostic_message(processor_code: Any, processor_text: Any) -> str:
    if processor_code is not None:
        mapped = PROCESSOR_CODE_MESSAGES.get(str(processor_code))
        if mapped:
            return mapped
    return str(processor_text or "No fue posible obtener detalle técnico del procesador.")


def diagnose_payment_method_status(card: Any, bin_database_path: str = "templates/bin-database.csv") -> dict[str, Any]:
    """Ejecuta diagnóstico profundo del estado de verificación de tarjeta en Braintree.

    COMPLIANT: solo cuando verification.status == 'verified'.
    NON_COMPLIANT: cualquier otro estado (processor_declined, gateway_rejected, failed, etc).
    """
    payload = CardPayload(
        number=_clean_digits(_read_card_attr(card, "number")),
        month=_clean_digits(_read_card_attr(card, "month")),
        year=_clean_digits(_read_card_attr(card, "year")),
        cvv=_clean_digits(_read_card_attr(card, "cvv")),
    )

    issuer = {"bin": "", "bank": "UNKNOWN"}
    try:
        issuer = lookup_issuer(load_bin_dataframe(bin_database_path), payload.number)
    except Exception as exc:
        issuer = {"bin": _clean_digits(payload.number)[:6], "bank": "UNKNOWN", "lookup_error": str(exc)}

    if not payload.number or not payload.month or not payload.year or not payload.cvv:
        return {
            "status": "NON_COMPLIANT",
            "verification_status": None,
            "message": "Payload incompleto para diagnóstico.",
            "processor_response_code": None,
            "issuer": issuer,
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
                "options": {"verify_card": True},
            }
        )

        verification = None
        if getattr(result, "credit_card", None):
            verification = getattr(result.credit_card, "verification", None)
        if verification is None:
            verification = getattr(result, "credit_card_verification", None)

        if verification is None:
            validation_errors = []
            if getattr(result, "errors", None):
                validation_errors = [
                    {"attribute": e.attribute, "code": e.code, "message": e.message}
                    for e in result.errors.deep_errors
                ]
            return {
                "status": "NON_COMPLIANT",
                "verification_status": None,
                "message": "Sin objeto verification en respuesta de Braintree.",
                "processor_response_code": None,
                "validation_errors": validation_errors,
                "issuer": issuer,
            }

        verification_status = str(getattr(verification, "status", "")).lower()
        processor_code = getattr(verification, "processor_response_code", None)
        processor_text = getattr(verification, "processor_response_text", None)

        compliant = verification_status == "verified"
        return {
            "status": "COMPLIANT" if compliant else "NON_COMPLIANT",
            "verification_status": verification_status,
            "message": "Verificación aprobada por Sandbox." if compliant else _diagnostic_message(processor_code, processor_text),
            "processor_response_code": processor_code,
            "processor_response_text": processor_text,
            "issuer": issuer,
        }

    except braintree.exceptions.AuthorizationError as exc:
        return {
            "status": "NON_COMPLIANT",
            "verification_status": None,
            "message": "Error de autorización contra Braintree Sandbox.",
            "processor_response_code": None,
            "details": str(exc),
            "issuer": issuer,
        }
    except braintree.exceptions.ServerError as exc:
        return {
            "status": "NON_COMPLIANT",
            "verification_status": None,
            "message": "Error del servidor Braintree.",
            "processor_response_code": None,
            "details": str(exc),
            "issuer": issuer,
        }
    except EnvironmentError as exc:
        return {
            "status": "NON_COMPLIANT",
            "verification_status": None,
            "message": str(exc),
            "processor_response_code": None,
            "issuer": issuer,
        }
    except Exception as exc:
        return {
            "status": "NON_COMPLIANT",
            "verification_status": None,
            "message": "Fallo inesperado durante diagnóstico.",
            "processor_response_code": None,
            "details": str(exc),
            "issuer": issuer,
        }


def verify_payment_method_diagnostics(card: Any, bin_database_path: str = "templates/bin-database.csv") -> dict[str, Any]:
    """Wrapper backward-compatible."""
    return diagnose_payment_method_status(card, bin_database_path)


if __name__ == "__main__":
    sample = CardPayload(number="4111111111111111", month="12", year="2030", cvv="123")
    print(json.dumps(diagnose_payment_method_status(sample), ensure_ascii=False, indent=2))
