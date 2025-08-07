"""Loader de datos de performance desde SQL."""

from __future__ import annotations

from typing import Optional

import pandas as pd
from sqlalchemy import text
from sqlalchemy.engine import Engine

from .sql_utils import get_engine


def load_performance_data(id_cliente: int, fecha_desde: Optional[str] = None, fecha_hasta: Optional[str] = None, *, engine: Optional[Engine] = None) -> pd.DataFrame:
    """Obtiene las métricas y URLs asociadas para un cliente.

    Parameters
    ----------
    id_cliente: int
        Identificador del cliente a consultar.
    fecha_desde, fecha_hasta: str, optional
        Rangos de fecha en formato ``YYYY-MM-DD``.  Si se omiten se devuelve
        todo el rango disponible.
    engine: Engine, optional
        Conexión a utilizar.  Por defecto se crea una nueva.
    """

    engine = engine or get_engine()

    query = (
        "SELECT m.date, m.Campaign, m.AdSet, m.Anuncio, m.publicos_in, m.spend, m.purchases, m.value, m.impr, m.clicks, m.reach,"
        "       v.ad_preview_link, v.ad_creative_thumburl "
        "FROM metricas m "
        "JOIN archivos_reporte ar ON m.id_reporte = ar.id_reporte "
        "LEFT JOIN vistas_preview v ON ar.id_cliente = v.id_cliente AND m.Anuncio = v.nombre_ad "
        "WHERE ar.id_cliente = :cid"
    )

    params = {"cid": id_cliente}
    if fecha_desde:
        query += " AND m.date >= :from"
        params["from"] = fecha_desde
    if fecha_hasta:
        query += " AND m.date <= :to"
        params["to"] = fecha_hasta

    df = pd.read_sql(text(query), engine, params=params)

    # Renombrar columnas para compatibilidad con el resto del pipeline
    rename_map = {
        'publicos_in': 'Públicos In',
        'spend': 'spend',
        'purchases': 'purchases',
        'value': 'value',
        'impr': 'impr',
        'clicks': 'clicks',
        'reach': 'reach',
        'Campaign': 'Campaign',
        'AdSet': 'AdSet',
        'Anuncio': 'Anuncio',
    }
    df = df.rename(columns=rename_map)
    return df

