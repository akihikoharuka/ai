"""FastAPI application entry point."""

from __future__ import annotations

import logging

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from backend.api.routes import router
from backend.config import settings
from backend.logging_config import setup_logging

setup_logging(log_level=settings.log_level, log_file=settings.log_file)
logger = logging.getLogger(__name__)

app = FastAPI(
    title="Synthetic Data Generator",
    description="Agentic AI tool for generating realistic synthetic data from SQL schemas",
    version="0.1.0",
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

app.include_router(router)

logger.info(
    "Synthetic Data Generator starting — model=%s, output_dir=%s, log_file=%s",
    settings.llm_model,
    settings.output_dir,
    settings.log_file,
)


@app.get("/health")
async def health():
    return {"status": "healthy"}
