#!/usr/bin/env python3
"""Data Scrubbing y Scoring multinivel para registros bancarios masivos.

Entrada esperada por línea (formato exacto):
numero|mes|año|cvv
"""

from __future__ import annotations

import argparse
import re
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass
from functools import partial
from pathlib import Path

import pandas as pd

LINE_PATTERN = re.compile(
    r"^\s*(?P<number>\d{15,16})\|(?P<month>0[1-9]|1[0-2])\|(?P<year>\d{2}|\d{4})\|(?P<cvv>\d{3,4})\s*$"
)
HIGH_TIER_LEVELS = ("INFINITE", "PLATINUM", "WORLD ELITE", "BUSINESS")
LOW_FRICTION_COUNTRIES = {"US", "UK", "CA"}
REFERENCE_YEAR = 2026
REFERENCE_MONTH = 3


@dataclass(slots=True)
class ParsedRecord:
    number: str
    month: str
    year: str
    cvv: str

    @property
    def exp_year_4(self) -> int:
        if len(self.year) == 2:
            return 2000 + int(self.year)
        return int(self.year)

    @property
    def exp_month(self) -> int:
        return int(self.month)

    @property
    def output_line(self) -> str:
        return f"{self.number}|{self.month}|{self.year}|{self.cvv}"


def parse_line(raw: str) -> ParsedRecord | None:
    match = LINE_PATTERN.fullmatch(raw)
    if not match:
        return None
    return ParsedRecord(
        number=match.group("number"),
        month=match.group("month"),
        year=match.group("year"),
        cvv=match.group("cvv"),
    )


def validate_luhn(number: str) -> bool:
    digits = [int(c) for c in number if c.isdigit()]
    if len(digits) not in (15, 16):
        return False

    checksum = 0
    parity = len(digits) % 2
    for idx, digit in enumerate(digits):
        if idx % 2 == parity:
            digit *= 2
            if digit > 9:
                digit -= 9
        checksum += digit
    return checksum % 10 == 0


def load_bins_csv(path: Path) -> dict[int, dict[str, dict[str, str]]]:
    df = pd.read_csv(path, dtype=str, keep_default_na=False)
    df.columns = [c.strip().lower() for c in df.columns]

    required = ["bin", "country", "bank", "type", "level"]
    missing = [c for c in required if c not in df.columns]
    if missing:
        raise ValueError(f"bins.csv sin columnas requeridas: {', '.join(missing)}")

    if "brand" not in df.columns:
        for alias in ("scheme", "network", "marca"):
            if alias in df.columns:
                df["brand"] = df[alias]
                break
        else:
            df["brand"] = "UNKNOWN"

    df["bin"] = df["bin"].str.replace(r"\D", "", regex=True)
    df = df[df["bin"].str.len().between(6, 8)]

    for col in ["country", "bank", "type", "level", "brand"]:
        df[col] = df[col].str.strip()

    bins_by_length: dict[int, dict[str, dict[str, str]]] = {6: {}, 7: {}, 8: {}}
    for row in df.to_dict(orient="records"):
        bin_key = row["bin"]
        bins_by_length[len(bin_key)][bin_key] = {
            "country": row["country"],
            "bank": row["bank"],
            "type": row["type"],
            "level": row["level"],
            "brand": row["brand"],
        }

    return bins_by_length


def lookup_bin(number: str, bins_by_length: dict[int, dict[str, dict[str, str]]]) -> dict[str, str] | None:
    for length in (8, 7, 6):
        info = bins_by_length[length].get(number[:length])
        if info:
            return info
    return None


def compute_score(record: ParsedRecord, bin_info: dict[str, str]) -> int:
    if (record.exp_year_4, record.exp_month) < (REFERENCE_YEAR, REFERENCE_MONTH):
        return 0

    score = 35
    level = (bin_info.get("level") or "").upper()
    card_type = (bin_info.get("type") or "").upper()
    country = (bin_info.get("country") or "").upper()

    if any(tier in level for tier in HIGH_TIER_LEVELS):
        score += 30
    if card_type == "CREDIT":
        score += 25
    if country in LOW_FRICTION_COUNTRIES:
        score += 5

    return max(0, min(100, score))


def describe(bin_info: dict[str, str]) -> str:
    brand = (bin_info.get("brand") or "UNKNOWN").upper()
    level = (bin_info.get("level") or "UNKNOWN").upper()
    bank = (bin_info.get("bank") or "UNKNOWN").upper()
    country = (bin_info.get("country") or "UNK").upper()
    return f"{brand} {level} - {bank} ({country})"


def process_one(line: str, bins_by_length: dict[int, dict[str, dict[str, str]]]) -> str | None:
    parsed = parse_line(line.strip())
    if parsed is None:
        return None

    if not validate_luhn(parsed.number):
        return None

    bin_info = lookup_bin(parsed.number, bins_by_length)
    if not bin_info:
        return None

    score = compute_score(parsed, bin_info)
    details = describe(bin_info)
    return f"{parsed.output_line} | SCORE: {score} | {details}"


def run(input_file: Path, bins_file: Path, output_file: Path, workers: int) -> tuple[int, int]:
    lines = input_file.read_text(encoding="utf-8", errors="ignore").splitlines()
    bins_by_length = load_bins_csv(bins_file)

    with ThreadPoolExecutor(max_workers=workers) as executor:
        func = partial(process_one, bins_by_length=bins_by_length)
        results = list(executor.map(func, lines, chunksize=1000))

    valid_rows = [row for row in results if row is not None]
    output_file.write_text("\n".join(valid_rows), encoding="utf-8")

    for row in valid_rows:
        print(row)

    return len(lines), len(valid_rows)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Data scrubbing y scoring de registros bancarios")
    parser.add_argument("--input", required=True, type=Path, help="Archivo de entrada (TXT)")
    parser.add_argument("--bins", required=True, type=Path, help="Archivo bins.csv")
    parser.add_argument("--output", required=True, type=Path, help="Archivo de salida (TXT)")
    parser.add_argument("--workers", type=int, default=12, help="Cantidad de workers concurrentes")
    return parser


if __name__ == "__main__":
    args = build_parser().parse_args()
    total, valid = run(args.input, args.bins, args.output, max(1, args.workers))
    print(f"Procesadas: {total} | Válidas: {valid} | Descartadas: {total - valid}")
