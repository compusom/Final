import pandas as pd
from sqlalchemy import text

from data_processing.sql_utils import get_engine, reset_database
from data_processing.sql_import_metricas import import_metricas_excel
from data_processing.sql_import_urls import import_urls_excel
from data_processing.sql_loader import load_performance_data


def test_sql_flow(tmp_path):
    db_path = tmp_path / "test.db"
    engine = get_engine(f"sqlite:///{db_path}")
    reset_database(engine)

    # Crear cliente base
    with engine.begin() as conn:
        conn.execute(text("INSERT INTO clientes (id_cliente, nombre_cuenta) VALUES (1, 'Cuenta')"))

    # Excel de métricas
    metrics_df = pd.DataFrame({
        'Día': ['2025-01-01'],
        'Nombre de la campaña': ['Camp'],
        'Nombre del conjunto de anuncios': ['Set'],
        'Nombre del anuncio': ['Ad1'],
        'Públicos personalizados incluidos': ['Aud1'],
        'Importe gastado (EUR)': [10.0],
        'Compras': [1],
        'Valor de conversión de compras': [20.0],
        'Impresiones': [100],
        'Clics en el enlace': [5],
        'Alcance': [80],
    })
    metrics_path = tmp_path / 'meta.xlsx'
    metrics_df.to_excel(metrics_path, index=False)

    import_metricas_excel(str(metrics_path), 1, engine=engine)

    # Excel de URLs
    urls_df = pd.DataFrame({
        'Account name': ['Cuenta'],
        'Ad name': ['Ad1'],
        'Reach': [80],
        'Ad Preview Link': ['http://preview'],
        'Ad Creative Thumbnail Url': ['http://thumb'],
    })
    urls_path = tmp_path / 'looker.xlsx'
    urls_df.to_excel(urls_path, index=False)

    import_urls_excel(str(urls_path), 1, engine=engine)

    df = load_performance_data(1, engine=engine)
    assert df.shape[0] == 1
    row = df.iloc[0]
    assert row['Públicos In'] == 'Aud1'
    assert row['spend'] == 10.0
    assert row['purchases'] == 1
    assert row['ad_preview_link'] == 'http://preview'

    reset_database(engine)
    with engine.begin() as conn:
        count = conn.execute(text("SELECT COUNT(*) FROM metricas")).scalar()
        assert count == 0
