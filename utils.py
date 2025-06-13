# report_generator_project/utils.py
import re
import unicodedata
import pandas as pd
import numpy as np

# ============================================================
# FUNCIONES DE UTILIDAD GENERAL
# ============================================================
def normalize(text):
    if not isinstance(text, str):
        text = str(text)
    s = re.sub(r"\s*\([^)]*\)$", "", text)
    s = s.replace('|', '')
    s = unicodedata.normalize('NFKD', s)
    s = "".join(c for c in s if not unicodedata.combining(c))
    return s.lower().strip()

def _split_clean_items(value):
    """Split a string on ``|`` or `,` and remove those characters from each part."""
    if value is None:
        return []
    parts = re.split(r"\s*[|,]\s*", str(value))
    cleaned = []
    for p in parts:
        name = p.strip().replace("|", "").replace(",", "")
        if name:
            cleaned.append(name)
    return cleaned


def aggregate_strings(series, separator=', ', max_len=70):
    if series.empty or series.isnull().all():
        return '-'
    items = []
    for val in series.astype(str).dropna():
        items.extend(_split_clean_items(val))
    strings = [s.strip() for s in items if s.strip()]
    unique_strings = pd.Index(strings).unique()
    if unique_strings.size == 0:
        return '-'
    result = separator.join(unique_strings)
    if max_len is not None and len(result) > max_len:
        result = result[:max_len-3] + '...'
    return result

def _sanitize_filename(name): # Mantenido como _sanitize si se usa internamente para algo específico
    s = str(name).strip().replace(' ', '_'); s = re.sub(r'[\\/*?:"<>|]', '', s)
    s = normalize(s); s = re.sub(r'[^a-z0-9_.\-]+', '', s); return s[:100]

def create_flexible_regex_pattern(normalized_key):
    escaped = re.escape(normalized_key); flexible = escaped.replace(r'\ ', r'\s+')
    parts = flexible.split(r'\s+'); pattern = r'\s*'.join(parts); return r'.*?' + pattern + r'.*?'

def robust_numeric_conversion(value_str):
    if pd.isna(value_str): return np.nan
    s = str(value_str).strip()
    if not s or s.lower() == 'nan': return np.nan

    s_no_curr = re.sub(r'[$\€£A-Z\s]+', '', s, flags=re.IGNORECASE)
    s_cleaned_for_sep = re.sub(r'[^\d.,\-eE]', '', s_no_curr)

    if ',' in s_cleaned_for_sep and '.' in s_cleaned_for_sep:
        last_comma = s_cleaned_for_sep.rfind(',')
        last_dot = s_cleaned_for_sep.rfind('.')
        if last_comma > last_dot:
            s_cleaned = s_cleaned_for_sep.replace('.', '').replace(',', '.')
        else:
            s_cleaned = s_cleaned_for_sep.replace(',', '')
    elif ',' in s_cleaned_for_sep:
        parts = s_cleaned_for_sep.split(',')
        if len(parts) > 1 and (1 <= len(parts[-1]) <= 3 and parts[-1].isdigit()):
             s_cleaned = s_cleaned_for_sep.replace(',', '.', len(parts)-2)
             s_cleaned = s_cleaned.replace(',', '.')
        else:
             s_cleaned = s_cleaned_for_sep.replace(',', '')
    elif '.' in s_cleaned_for_sep:
        parts = s_cleaned_for_sep.split('.')
        if len(parts) > 2:
            if len(parts[-1]) <= 3 and parts[-1].isdigit():
                 s_cleaned = "".join(parts[:-1]) + "." + parts[-1]
            else:
                 s_cleaned = s_cleaned_for_sep.replace('.', '', len(parts)-2)
        else:
            s_cleaned = s_cleaned_for_sep
    else:
        s_cleaned = s_cleaned_for_sep

    s_final_numeric_str = re.sub(r'[^\d.\-eE]', '', s_cleaned)
    if not s_final_numeric_str or s_final_numeric_str == '.' or s_final_numeric_str == '-': return np.nan

    try:
        return float(s_final_numeric_str)
    except ValueError:
        try:
            s_fixed = s_final_numeric_str.replace('..','.').replace('--','-')
            if len(s_fixed) > 1: s_fixed = s_fixed.strip('.')
            if s_fixed.count('.') > 1:
                 parts = s_fixed.split('.')
                 s_fixed = parts[0] + '.' + "".join(parts[1:])
            if s_fixed.count('-') > 1:
                s_fixed = '-' + s_fixed.replace('-','')
            if not s_fixed or s_fixed == '.': return np.nan
            return float(s_fixed)
        except ValueError:
            return np.nan
