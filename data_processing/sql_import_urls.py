"""Importador de URLs de Looker a la base SQL."""

from __future__ import annotations

import os
from typing import Optional

import pandas as pd
from sqlalchemy import text
from sqlalchemy.engine import Engine

from .sql_utils import file_sha256, get_engine

COLUMN_NAMES = {
    'Account name': 'account_name',
    'Ad name': 'nombre_ad',
    'Reach': 'reach',
    'Ad Preview Link': 'ad_preview_link',
    'Ad Creative Thumbnail Url': 'ad_creative_thumburl',
}


def import_urls_excel(path_excel: str, id_cliente: int, *, engine: Optional[Engine] = None) -> int:
    """Importa el Excel de Looker con previsualizaciones.

    Devuelve el ``id_url`` registrado en ``archivos_url``.
    """

    engine = engine or get_engine()
    hash_value = file_sha256(path_excel)
    filename = os.path.basename(path_excel)

    df = pd.read_excel(path_excel)
    df = df.rename(columns=COLUMN_NAMES)
    missing = [c for c in COLUMN_NAMES.values() if c not in df.columns]
    if missing:
        raise ValueError(f"Columnas faltantes en el Excel: {missing}")

    with engine.begin() as conn:
        existing = conn.execute(
            text("SELECT id_url FROM archivos_url WHERE hash_archivo=:h"),
            {"h": hash_value},
        ).fetchone()
        if existing:
            raise ValueError("El archivo de URLs ya fue importado")

        result = conn.execute(
            text(
                "INSERT INTO archivos_url (id_cliente, nombre_archivo, hash_archivo) "
                "VALUES (:cid, :name, :hash)"
            ),
            {"cid": id_cliente, "name": filename, "hash": hash_value},
        )
        id_url = int(result.lastrowid)

        for _, row in df.iterrows():
            conn.execute(
                text(
                    """
                    INSERT INTO vistas_preview (id_cliente, nombre_ad, reach, ad_preview_link, ad_creative_thumburl)
                    VALUES (:cid, :ad, :reach, :plink, :thumb)
                    ON CONFLICT(id_cliente, nombre_ad) DO UPDATE SET
                        reach=excluded.reach,
                        ad_preview_link=excluded.ad_preview_link,
                        ad_creative_thumburl=excluded.ad_creative_thumburl,
                        updated_at=CURRENT_TIMESTAMP
                    """
                ),
                {
                    "cid": id_cliente,
                    "ad": row["nombre_ad"],
                    "reach": int(row["reach"]) if not pd.isna(row["reach"]) else None,
                    "plink": row["ad_preview_link"],
                    "thumb": row["ad_creative_thumburl"],
                },
            )

    return id_url

