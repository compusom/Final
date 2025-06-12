import numpy as np
from utils import normalize, robust_numeric_conversion

def test_normalize_basic():
    assert normalize("ÁdS Tést (demo)") == "ads test"

def test_robust_numeric_conversion():
    assert robust_numeric_conversion("1,234.56") == 1234.56
    assert robust_numeric_conversion("1.234,56") == 1234.56
    assert robust_numeric_conversion("$1,234.56") == 1234.56
    assert np.isnan(robust_numeric_conversion("nan"))
