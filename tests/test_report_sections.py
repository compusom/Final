import pandas as pd
from datetime import datetime
from data_processing.report_sections import _generar_tabla_bitacora_top_ads
from data_processing.report_sections import _generar_tabla_bitacora_top_adsets
from data_processing.report_sections import _generar_tabla_bitacora_top_campaigns
from data_processing.report_sections import _clean_audience_string


def test_top_ads_basic_columns(capsys):
    df = pd.DataFrame({
        'date': pd.to_datetime(['2024-06-01','2024-06-02']),
        'Campaign': ['Camp','Camp'],
        'AdSet': ['Set','Set'],
        'Anuncio': ['Ad1','Ad1'],
        'spend': [10, 20],
        'impr': [100, 200],
        'reach': [80, 150],
        'purchases': [1, 2],
        'visits': [10, 20],
        'value': [50, 120],
        'clicks': [5, 10],
        'clicks_out': [1, 2],
        'rv3': [0, 0],
        'rv25': [1, 2],
        'rv75': [0, 1],
        'rv100': [0, 1],
        'rtime': [4, 5],
        'url_final': ['https://ex.com', 'https://ex.com'],
        'puja': [0.5, 0.5],
        'interacciones': [7, 8],
        'comentarios': [1, 2],
        'Públicos In': ['Inc1', 'Inc2'],
        'Públicos Ex': ['Exc1', 'Exc2'],
    })
    periods = [
        (datetime(2024,6,1), datetime(2024,6,2), 'Semana actual'),
        (datetime(2024,5,25), datetime(2024,5,31), '1ª semana anterior'),
        (datetime(2024,5,18), datetime(2024,5,24), '2ª semana anterior'),
    ]
    active = pd.DataFrame({
        'Campaign': ['Camp'],
        'AdSet': ['Set'],
        'Anuncio': ['Ad1'],
        'Días_Activo_Total': [2]
    })
    logs = []
    _generar_tabla_bitacora_top_ads(df, periods, active, logs.append, '$', top_n=1)
    output = "\n".join(logs)
    assert 'Top 1 Ads Bitácora - Semana actual' in output
    assert 'Anuncio' in output
    assert 'Días Act' in output

def test_clean_audience_string():
    assert _clean_audience_string('123:Aud1 | 456:Aud2') == 'Aud1 | Aud2'
    # Also handle comma separated values
    assert _clean_audience_string('123:Aud1, 456:Aud2') == 'Aud1 | Aud2'


def test_top_adsets_weekly_table(capsys):
    df = pd.DataFrame({
        'date': pd.to_datetime(['2024-06-01', '2024-06-02']),
        'Campaign': ['Camp', 'Camp'],
        'AdSet': ['Set', 'Set'],
        'spend': [10, 15],
        'impr': [100, 150],
        'reach': [90, 130],
        'purchases': [1, 1],
        'visits': [10, 12],
        'value': [20, 25],
    })
    periods = [
        (datetime(2024, 6, 1), datetime(2024, 6, 2), 'Semana actual'),
        (datetime(2024, 5, 25), datetime(2024, 5, 31), '1ª semana anterior'),
    ]
    active = pd.DataFrame({
        'Campaign': ['Camp'],
        'AdSet': ['Set'],
        'Días_Activo_Total': [2]
    })
    logs = []
    _generar_tabla_bitacora_top_adsets(df, periods, active, logs.append, '$', top_n=1)
    output = "\n".join(logs)
    assert 'Top 1 AdSets Bitácora - Semana actual' in output
    assert 'Días Act' in output


def test_top_adsets_deduplication():
    df = pd.DataFrame({
        'date': pd.to_datetime(['2024-06-01', '2024-06-02']),
        'Campaign': ['Camp', 'Camp'],
        'AdSet': ['Set', 'Set'],
        'spend': [10, 15],
        'impr': [100, 150],
        'reach': [90, 130],
        'purchases': [1, 1],
        'visits': [10, 12],
        'value': [20, 25],
    })
    periods = [(datetime(2024, 6, 1), datetime(2024, 6, 2), 'Semana actual')]
    # Active days with duplicate row
    active = pd.DataFrame({
        'Campaign': ['Camp', 'Camp'],
        'AdSet': ['Set', 'Set'],
        'Días_Activo_Total': [2, 2],
    })
    logs = []
    _generar_tabla_bitacora_top_adsets(df, periods, active, logs.append, '$', top_n=1)
    row_lines = [l for l in logs if l.startswith('| Camp ')]
    assert len(row_lines) == 1


def test_top_campaigns_weekly_table(capsys):
    df = pd.DataFrame({
        'date': pd.to_datetime(['2024-06-01', '2024-06-02']),
        'Campaign': ['Camp', 'Camp'],
        'spend': [10, 15],
        'impr': [100, 150],
        'reach': [90, 130],
        'purchases': [1, 1],
        'visits': [10, 12],
        'value': [20, 25],
    })
    periods = [
        (datetime(2024, 6, 1), datetime(2024, 6, 2), 'Semana actual'),
        (datetime(2024, 5, 25), datetime(2024, 5, 31), '1ª semana anterior'),
    ]
    active = pd.DataFrame({
        'Campaign': ['Camp'],
        'Días_Activo_Total': [2]
    })
    logs = []
    _generar_tabla_bitacora_top_campaigns(df, periods, active, logs.append, '$', top_n=1)
    output = "\n".join(logs)
    assert 'Top 1 Campañas Bitácora - Semana actual' in output

