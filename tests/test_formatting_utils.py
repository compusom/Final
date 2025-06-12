import numpy as np
import pandas as pd
from formatting_utils import safe_division, safe_division_pct

def test_safe_division_scalar():
    assert safe_division(10, 2) == 5
    assert np.isnan(safe_division(1, 0))

def test_safe_division_series():
    result = safe_division(pd.Series([1, 2]), pd.Series([2, 0]))
    assert result.iloc[0] == 0.5
    assert np.isnan(result.iloc[1])

def test_safe_division_pct():
    assert safe_division_pct(1, 2) == 50.0

