import pandas as pd
import queue
from data_processing.aggregators import _agregar_datos_diarios


def test_aggregate_new_metrics():
    df = pd.DataFrame({
        'date': pd.to_datetime(['2025-06-01','2025-06-01']),
        'Campaign': ['C1','C1'],
        'AdSet': ['A1','A1'],
        'Anuncio': ['Ad1','Ad1'],
        'campaign_budget': [100, 100],
        'adset_budget': [50, 60],
        'objective': ['Ventas', 'Ventas'],
        'purchase_type': ['Subasta', 'Subasta'],
        'delivery_general_status': ['Activo', 'Activo'],
    })
    q = queue.SimpleQueue()
    result = _agregar_datos_diarios(df, q)
    assert 'campaign_budget' in result.columns
    assert result['campaign_budget'].iloc[0] == 100
    assert 'adset_budget' in result.columns
    assert result['adset_budget'].iloc[0] == 55
    assert 'objective' in result.columns
    assert result['objective'].iloc[0] == 'Ventas'
    assert 'purchase_type' in result.columns
    assert 'delivery_general_status' in result.columns
