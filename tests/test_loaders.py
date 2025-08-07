import queue
import pandas as pd
from data_processing.loaders import cargar_y_preparar_datos

def test_value_fallback_from_avg(tmp_path):
    df = pd.DataFrame({
        'Día': ['2025-06-01', '2025-06-02'],
        'Nombre de la campaña': ['Camp', 'Camp'],
        'Nombre del conjunto de anuncios': ['Set', 'Set'],
        'Valor de conversión de compras promedio': [50, 50],
        'Compras': [2, 3],
    })
    file_path = tmp_path / 'data.xlsx'
    df.to_excel(file_path, index=False)

    q = queue.SimpleQueue()
    result, _, _ = cargar_y_preparar_datos([str(file_path)], q, '__ALL__')
    assert 'value' in result.columns
    expected = df['Valor de conversión de compras promedio'].iloc[1] * df['Compras'].iloc[1]
    assert result['value'].iloc[0] == expected


def test_new_columns_mapping(tmp_path):
    df = pd.DataFrame({
        'Día': ['2025-06-01', '2025-06-02'],
        'Nombre de la campaña': ['Camp', 'Camp'],
        'Nombre del conjunto de anuncios': ['Set', 'Set'],
        'Estado de la entrega': ['Activo', 'Activo'],
        'presupuesto Campaña': [100, 100],
        'Presupuesto Adset': [50, 50],
        'Objetivo': ['Ventas', 'Ventas'],
        'Tipo de compra': ['Subasta', 'Subasta'],
    })
    file_path = tmp_path / 'data2.xlsx'
    df.to_excel(file_path, index=False)

    q = queue.SimpleQueue()
    result, _, _ = cargar_y_preparar_datos([str(file_path)], q, '__ALL__')
    for col in ['delivery_general_status','campaign_budget','adset_budget','objective','purchase_type']:
        assert col in result.columns


def test_delivery_level_mapping(tmp_path):
    df = pd.DataFrame({
        'Día': ['2025-06-01', '2025-06-02'],
        'Nombre de la campaña': ['Camp', 'Camp'],
        'Nombre del conjunto de anuncios': ['Set', 'Set'],
        'Nivel de la entrega': ['Ad', 'Ad'],
    })
    file_path = tmp_path / 'data3.xlsx'
    df.to_excel(file_path, index=False)

    q = queue.SimpleQueue()
    result, _, _ = cargar_y_preparar_datos([str(file_path)], q, '__ALL__')
    assert 'delivery_level' in result.columns

