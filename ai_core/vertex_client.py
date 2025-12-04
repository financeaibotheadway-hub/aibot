# -*- coding: utf-8 -*-
"""Wrapper around Vertex AI generative model."""

import logging
from functools import lru_cache

import vertexai
from vertexai.preview.generative_models import GenerativeModel

from .config import BQ_PROJECT, VERTEX_LOCATION

logger = logging.getLogger("ai-bot")

_vertex_inited = False
_model = None


def _ensure_model():
    global _vertex_inited, _model
    if not _vertex_inited:
        try:
            vertexai.init(project=BQ_PROJECT, location=VERTEX_LOCATION)
        except Exception:
            logger.warning("Vertex init failed; relying on ambient creds", exc_info=True)
        _model = GenerativeModel("gemini-2.5-flash")
        _vertex_inited = True
    return _model


def generate_text(prompt: str, temperature: float = 0.0) -> str:
    model = _ensure_model()
    resp = model.generate_content(prompt, generation_config={"temperature": temperature})
    return resp.text.strip()
