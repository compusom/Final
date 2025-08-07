"""Importador de métricas desde archivos Excel a la base SQL."""

from __future__ import annotations

import os
from typing import Optional

import pandas as pd
from sqlalchemy import text
from sqlalchemy.engine import Engine

from .sql_utils import file_sha256, get_engine

# Mapeo mínimo de columnas del Excel a las columnas internas de la tabla
# ``metricas``.  Se incluyen las necesarias para los reportes de performance.
COLUMN_MAP = {
    'Día': 'date',
    'Nombre de la campaña': 'Campaign',
    'Nombre del conjunto de anuncios': 'AdSet',
    'Nombre del anuncio': 'Anuncio',
    'Públicos personalizados incluidos': 'publicos_in',
    'Importe gastado (EUR)': 'spend',
    'Compras': 'purchases',
    'Valor de conversión de compras': 'value',
    'Impresiones': 'impr',
    'Clics en el enlace': 'clicks',
    'Alcance': 'reach',
}


def import_metricas_excel(path_excel: str, id_cliente: int, *, engine: Optional[Engine] = None) -> int:
    """Importa un Excel de métricas de Meta.

    Parameters
    ----------
    path_excel: str
        Ruta al archivo Excel de origen.
    id_cliente: int
        Identificador del cliente dueño del reporte.
    engine: Engine, optional
        Conexión a utilizar.  Si no se provee se crea a partir de la
        configuración.

    Returns
    -------
    int
        ``id_reporte`` generado para el archivo importado.
    """

    engine = engine or get_engine()
    hash_value = file_sha256(path_excel)
    filename = os.path.basename(path_excel)

    df = pd.read_excel(path_excel)
    df = df.rename(columns=COLUMN_MAP)
    missing = [col for col in COLUMN_MAP.values() if col not in df.columns]
    if missing:
        raise ValueError(f"Columnas faltantes en el Excel: {missing}")

    with engine.begin() as conn:
        existing = conn.execute(
            text("SELECT id_reporte FROM archivos_reporte WHERE hash_archivo=:h"),
            {"h": hash_value},
        ).fetchone()
        if existing:
            raise ValueError("El archivo de métricas ya fue importado")

        result = conn.execute(
            text(
                "INSERT INTO archivos_reporte (id_cliente, nombre_archivo, hash_archivo) "
                "VALUES (:cid, :name, :hash)"
            ),
            {"cid": id_cliente, "name": filename, "hash": hash_value},
        )
        # SQLite expone lastrowid
        id_reporte = int(result.lastrowid)

        df['id_reporte'] = id_reporte
        df.to_sql('metricas', conn, if_exists='append', index=False)

    return id_reporte

