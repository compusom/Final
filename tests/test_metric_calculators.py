import pandas as pd
from data_processing.metric_calculators import calcular_dias_activos_totales


def test_active_days_impressions_filter():
    df = pd.DataFrame({
        'Campaign': ['C1', 'C1', 'C1', 'C1'],
        'AdSet': ['A1', 'A1', 'A1', 'A1'],
        'Anuncio': ['Ad1', 'Ad1', 'Ad1', 'Ad1'],
        'date': pd.to_datetime(['2024-01-01','2024-01-02','2024-01-02','2024-01-03']),
        'Entrega': ['Activo', 'Activo', 'Apagado', 'Activo'],
        'impr': [10, 0, 15, 5]
    })
    res = calcular_dias_activos_totales(df)
    assert res['Anuncio']['Días_Activo_Total'].iloc[0] == 2
