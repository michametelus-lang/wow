---
title: wow
emoji: 🚀
colorFrom: indigo
colorTo: blue
sdk: docker
pinned: false
---

# WOW

High-performance analysis and validation service designed for Hugging Face Spaces.

## Runtime

- **SDK:** Docker
- **App Port:** `7860`
- **Production Server:** Gunicorn
- **Entry Point:** `api.index:app`

## Local structure

- `api/index.py`: Flask app + vectorized Luhn engine + enrichment flow.
- `templates/index.html`: 3-column dashboard UI.
- `templates/bin_database.csv`: Local enrichment seed database.
- `Dockerfile`: Container build and runtime configuration.

## Deploying on Hugging Face Spaces

This repository is configured for a **Docker Space** and listens on port `7860`, which is compatible with Hugging Face Spaces runtime expectations.
