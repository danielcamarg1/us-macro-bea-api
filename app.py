import os
import json
import unicodedata
from datetime import datetime, timezone

import pandas as pd
import requests
from flask import Flask, jsonify, request

app = Flask(__name__)

# ============================================================
# CONFIGURAÇÃO GERAL
# ============================================================
BASE_DIR = os.path.dirname(os.path.abspath(__file__))

DATA_DIR = os.path.join(BASE_DIR, "data")
BEA_DIR = os.path.join(DATA_DIR, "bea")
TRADE_DIR = os.path.join(DATA_DIR, "trade")

BEA_API_KEY = os.getenv("BEA_API_KEY", "").strip()
BLS_API_KEY = os.getenv("BLS_API_KEY", "").strip()
BLS_API_URL = "https://api.bls.gov/publicAPI/v2/timeseries/data/"
BLS_TIMEOUT_SECONDS = 60

# ============================================================
# ARQUIVOS BEA
# ============================================================
BEA_CORE_CSV = os.path.join(BEA_DIR, "us_macro_bea_v1.csv")
BEA_CORE_CATALOG_CSV = os.path.join(BEA_DIR, "series_catalog_v1.csv")
BEA_INDUSTRY_CSV = os.path.join(BEA_DIR, "us_macro_bea_industry_v2.csv")
BEA_INDUSTRY_CATALOG_CSV = os.path.join(BEA_DIR, "industry_series_catalog_v2.csv")

# ============================================================
# ARQUIVOS TRADE
# ============================================================
TRADE_COUNTRIES_CSV = os.path.join(TRADE_DIR, "api_trade_country_annual_countries_v3_3.csv")
TRADE_GROUPS_CSV = os.path.join(TRADE_DIR, "api_trade_country_annual_groups_v3_3.csv")
TRADE_TOTAL_CSV = os.path.join(TRADE_DIR, "api_trade_country_annual_total_v3_3.csv")
TRADE_PARTNER_MASTER_CSV = os.path.join(TRADE_DIR, "api_trade_partner_master_v3_3.csv")
TRADE_CATALOG_CSV = os.path.join(TRADE_DIR, "api_trade_catalog_v3_3.csv")
TRADE_ALIASES_CSV = os.path.join(TRADE_DIR, "api_trade_aliases_v3_3.csv")
TRADE_COUNTRY_LIST_CSV = os.path.join(TRADE_DIR, "api_trade_country_list_v3_3.csv")
TRADE_GROUP_LIST_CSV = os.path.join(TRADE_DIR, "api_trade_group_list_v3_3.csv")
TRADE_SUMMARY_JSON = os.path.join(TRADE_DIR, "api_trade_bundle_summary_v3_3.json")

# ============================================================
# BLS CPI CURADO (API AO VIVO)
# ============================================================
# Estes aliases usam códigos oficiais de séries CPI-U sazonalmente ajustadas do BLS.
BLS_CPI_SERIES_MAP = {
    "cpi_headline": {
        "series_id": "CUSR0000SA0",
        "display_name_pt": "CPI cheio",
        "theme": "inflation_cpi",
        "subcategory": "headline",
        "unit": "index_1982_84_100",
        "source_block": "bls_cpi",
        "notes": "All items, U.S. city average, seasonally adjusted",
    },
    "cpi_core": {
        "series_id": "CUSR0000SA0L1E",
        "display_name_pt": "Core CPI",
        "theme": "inflation_cpi",
        "subcategory": "core",
        "unit": "index_1982_84_100",
        "source_block": "bls_cpi",
        "notes": "All items less food and energy, seasonally adjusted",
    },
    "cpi_food": {
        "series_id": "CUSR0000SAF1",
        "display_name_pt": "CPI de alimentos",
        "theme": "inflation_cpi",
        "subcategory": "food",
        "unit": "index_1982_84_100",
        "source_block": "bls_cpi",
        "notes": "Food, seasonally adjusted",
    },
    "cpi_food_home": {
        "series_id": "CUSR0000SAF11",
        "display_name_pt": "CPI de alimentos no domicílio",
        "theme": "inflation_cpi",
        "subcategory": "food",
        "unit": "index_1982_84_100",
        "source_block": "bls_cpi",
        "notes": "Food at home, seasonally adjusted",
    },
    "cpi_food_away": {
        "series_id": "CUSR0000SEFV",
        "display_name_pt": "CPI de alimentação fora do domicílio",
        "theme": "inflation_cpi",
        "subcategory": "food",
        "unit": "index_1982_84_100",
        "source_block": "bls_cpi",
        "notes": "Food away from home, seasonally adjusted",
    },
    "cpi_energy": {
        "series_id": "CUSR0000SA0E",
        "display_name_pt": "CPI de energia",
        "theme": "inflation_cpi",
        "subcategory": "energy",
        "unit": "index_1982_84_100",
        "source_block": "bls_cpi",
        "notes": "Energy, seasonally adjusted",
    },
    "cpi_gasoline": {
        "series_id": "CUSR0000SETB01",
        "display_name_pt": "CPI de gasolina",
        "theme": "inflation_cpi",
        "subcategory": "energy",
        "unit": "index_1982_84_100",
        "source_block": "bls_cpi",
        "notes": "Gasoline (all types), seasonally adjusted",
    },
    "cpi_shelter": {
        "series_id": "CUSR0000SAH1",
        "display_name_pt": "CPI de shelter",
        "theme": "inflation_cpi",
        "subcategory": "housing",
        "unit": "index_1982_84_100",
        "source_block": "bls_cpi",
        "notes": "Shelter, seasonally adjusted",
    },
    "cpi_services": {
        "series_id": "CUSR0000SAS",
        "display_name_pt": "CPI de serviços",
        "theme": "inflation_cpi",
        "subcategory": "services",
        "unit": "index_1982_84_100",
        "source_block": "bls_cpi",
        "notes": "Services, seasonally adjusted",
    },
    "cpi_core_services": {
        "series_id": "CUSR0000SASLE",
        "display_name_pt": "CPI de serviços ex-energia",
        "theme": "inflation_cpi",
        "subcategory": "services",
        "unit": "index_1982_84_100",
        "source_block": "bls_cpi",
        "notes": "Services less energy services, seasonally adjusted",
    },
    "cpi_medical": {
        "series_id": "CUSR0000SAM",
        "display_name_pt": "CPI de medical care",
        "theme": "inflation_cpi",
        "subcategory": "medical",
        "unit": "index_1982_84_100",
        "source_block": "bls_cpi",
        "notes": "Medical care, seasonally adjusted",
    },
    "cpi_transportation": {
        "series_id": "CUSR0000SAT",
        "display_name_pt": "CPI de transportation",
        "theme": "inflation_cpi",
        "subcategory": "transportation",
        "unit": "index_1982_84_100",
        "source_block": "bls_cpi",
        "notes": "Transportation, seasonally adjusted",
    },
    "cpi_apparel": {
        "series_id": "CUSR0000SAA",
        "display_name_pt": "CPI de apparel",
        "theme": "inflation_cpi",
        "subcategory": "goods",
        "unit": "index_1982_84_100",
        "source_block": "bls_cpi",
        "notes": "Apparel, seasonally adjusted",
    },
    "cpi_used_cars": {
        "series_id": "CUSR0000SETA02",
        "display_name_pt": "CPI de used cars and trucks",
        "theme": "inflation_cpi",
        "subcategory": "goods",
        "unit": "index_1982_84_100",
        "source_block": "bls_cpi",
        "notes": "Used cars and trucks, seasonally adjusted",
    },
}

# ============================================================
# CACHE EM MEMÓRIA
# ============================================================
BEA_CORE_DF = None
BEA_CORE_CATALOG_DF = None
BEA_INDUSTRY_DF = None
BEA_INDUSTRY_CATALOG_DF = None

TRADE_COUNTRIES_DF = None
TRADE_GROUPS_DF = None
TRADE_TOTAL_DF = None
TRADE_PARTNER_MASTER_DF = None
TRADE_CATALOG_DF = None
TRADE_ALIASES_DF = None
TRADE_COUNTRY_LIST_DF = None
TRADE_GROUP_LIST_DF = None
TRADE_SUMMARY = None

# ============================================================
# ERROS DE CARGA
# ============================================================
LOAD_ERRORS = {
    "bea_core": None,
    "bea_core_catalog": None,
    "bea_industry": None,
    "bea_industry_catalog": None,
    "trade_countries": None,
    "trade_groups": None,
    "trade_total": None,
    "trade_partner_master": None,
    "trade_catalog": None,
    "trade_aliases": None,
    "trade_country_list": None,
    "trade_group_list": None,
    "trade_summary": None,
    "bls_cpi": None,
}

# ============================================================
# FUNÇÕES AUXILIARES
# ============================================================
def utc_now_iso():
    return datetime.now(timezone.utc).isoformat()


def normalize_text(value):
    if value is None:
        return ""
    text = str(value).strip().lower()
    text = unicodedata.normalize("NFKD", text)
    text = "".join(ch for ch in text if not unicodedata.combining(ch))
    return " ".join(text.split())


def safe_int(value):
    try:
        if value is None or str(value).strip() == "":
            return None
        return int(float(str(value).strip()))
    except Exception:
        return None


def safe_float(value):
    try:
        if value is None or str(value).strip() == "":
            return None
        return float(value)
    except Exception:
        return None


def parse_year_from_date(value):
    if value is None:
        return None

    if isinstance(value, (int, float)):
        return safe_int(value)

    text = str(value).strip()
    if len(text) >= 4 and text[:4].isdigit():
        return safe_int(text[:4])

    return None


def apply_year_filter(df, year_start=None, year_end=None):
    if df is None or df.empty:
        return df

    out = df.copy()
    out["_year_num"] = out["date"].map(parse_year_from_date)

    if year_start is not None:
        out = out[out["_year_num"] >= year_start]

    if year_end is not None:
        out = out[out["_year_num"] <= year_end]

    return out.drop(columns=["_year_num"], errors="ignore")


def df_to_records(df, max_rows=500):
    if df is None or df.empty:
        return []

    out = df.head(max_rows).copy()
    out = out.where(pd.notnull(out), None)
    return out.to_dict(orient="records")


def load_csv_if_exists(path):
    if not os.path.exists(path):
        return None

    encodings_to_try = ["utf-8", "utf-8-sig", "cp1252", "latin1"]

    last_error = None
    for enc in encodings_to_try:
        try:
            return pd.read_csv(path, encoding=enc)
        except Exception as e:
            last_error = e

    raise RuntimeError(f"Falha ao ler CSV {path}. Último erro: {last_error}")


def bea_files_status():
    return {
        "bea_core_csv": os.path.exists(BEA_CORE_CSV),
        "bea_core_catalog_csv": os.path.exists(BEA_CORE_CATALOG_CSV),
        "bea_industry_csv": os.path.exists(BEA_INDUSTRY_CSV),
        "bea_industry_catalog_csv": os.path.exists(BEA_INDUSTRY_CATALOG_CSV),
    }


def trade_files_status():
    return {
        "countries_csv": os.path.exists(TRADE_COUNTRIES_CSV),
        "groups_csv": os.path.exists(TRADE_GROUPS_CSV),
        "total_csv": os.path.exists(TRADE_TOTAL_CSV),
        "partner_master_csv": os.path.exists(TRADE_PARTNER_MASTER_CSV),
        "catalog_csv": os.path.exists(TRADE_CATALOG_CSV),
        "aliases_csv": os.path.exists(TRADE_ALIASES_CSV),
        "country_list_csv": os.path.exists(TRADE_COUNTRY_LIST_CSV),
        "group_list_csv": os.path.exists(TRADE_GROUP_LIST_CSV),
        "summary_json": os.path.exists(TRADE_SUMMARY_JSON),
    }


def bls_cpi_catalog_df():
    rows = []
    for alias, meta in BLS_CPI_SERIES_MAP.items():
        rows.append({
            "source_block": meta.get("source_block", "bls_cpi"),
            "dataset": "BLS_CPI",
            "display_name_pt": meta.get("display_name_pt"),
            "frequency": "M",
            "series_name": alias,
            "series_id": meta.get("series_id"),
            "subcategory": meta.get("subcategory"),
            "theme": meta.get("theme", "inflation_cpi"),
            "unit": meta.get("unit", "index_1982_84_100"),
            "notes": meta.get("notes"),
        })
    return pd.DataFrame(rows)


def bls_request_payload(series_ids, startyear=None, endyear=None):
    payload = {
        "seriesid": series_ids,
    }
    if startyear is not None:
        payload["startyear"] = str(startyear)
    if endyear is not None:
        payload["endyear"] = str(endyear)
    if BLS_API_KEY:
        payload["registrationkey"] = BLS_API_KEY
    return payload


def fetch_bls_series(series_ids, startyear=None, endyear=None):
    if not series_ids:
        raise RuntimeError("Nenhuma série BLS informada.")

    if startyear is not None and endyear is not None and (endyear - startyear) > 19:
        raise RuntimeError("A API pública do BLS permite janelas de até 20 anos por consulta.")

    payload = bls_request_payload(series_ids, startyear=startyear, endyear=endyear)

    response = requests.post(
        BLS_API_URL,
        json=payload,
        timeout=BLS_TIMEOUT_SECONDS,
        headers={"Content-Type": "application/json"},
    )
    response.raise_for_status()

    data = response.json()
    if data.get("status") != "REQUEST_SUCCEEDED":
        raise RuntimeError(f"BLS API retornou status inválido: {data}")

    series_list = data.get("Results", {}).get("series", [])
    if not isinstance(series_list, list):
        raise RuntimeError("Resposta inesperada da BLS API.")

    return series_list


def parse_bls_series_to_df(series_obj):
    rows = []
    series_id = str(series_obj.get("seriesID", "")).strip()

    for item in series_obj.get("data", []):
        period = str(item.get("period", "")).strip()
        if not period.startswith("M"):
            continue
        if period == "M13":
            continue

        year = safe_int(item.get("year"))
        month = safe_int(period[1:])
        if year is None or month is None:
            continue

        value = safe_float(item.get("value"))
        if value is None:
            continue

        rows.append({
            "series_id": series_id,
            "date": f"{year:04d}-{month:02d}",
            "year": year,
            "month": month,
            "period": period,
            "period_name": item.get("periodName"),
            "value": value,
        })

    df = pd.DataFrame(rows)
    if df.empty:
        return df

    return df.sort_values(["year", "month"]).reset_index(drop=True)


def compute_bls_calc(df, calc):
    if df is None or df.empty:
        return df, None

    out = df.copy().sort_values(["year", "month"]).reset_index(drop=True)
    calc = (calc or "index").strip().lower()

    if calc == "index":
        unit = "index_1982_84_100"
    elif calc == "mom":
        out["value"] = out["value"].pct_change(1) * 100.0
        unit = "percent_mom"
    elif calc == "yoy":
        out["value"] = out["value"].pct_change(12) * 100.0
        unit = "percent_yoy"
    else:
        raise RuntimeError("calc inválido. Use: index, mom ou yoy.")

    out = out.dropna(subset=["value"]).reset_index(drop=True)
    return out, unit


def resolve_bls_cpi_series_name(series_input=None, q=None):
    catalog = bls_cpi_catalog_df()

    if series_input:
        raw = str(series_input).strip()
        raw_upper = raw.upper()
        if raw in BLS_CPI_SERIES_MAP:
            return raw, BLS_CPI_SERIES_MAP[raw]

        for alias, meta in BLS_CPI_SERIES_MAP.items():
            if str(meta.get("series_id", "")).upper() == raw_upper:
                return alias, meta

        norm = normalize_text(raw)
        if norm in [normalize_text(x) for x in BLS_CPI_SERIES_MAP.keys()]:
            for alias in BLS_CPI_SERIES_MAP:
                if normalize_text(alias) == norm:
                    return alias, BLS_CPI_SERIES_MAP[alias]

    search = q if q else series_input
    if not search:
        return None, None

    q_norm = normalize_text(search)
    cat = catalog.copy()
    cat["_alias_norm"] = cat["series_name"].astype(str).map(normalize_text)
    cat["_display_norm"] = cat["display_name_pt"].astype(str).map(normalize_text)
    cat["_notes_norm"] = cat["notes"].astype(str).map(normalize_text)
    cat["_series_id_norm"] = cat["series_id"].astype(str).str.upper()

    exact_alias = cat[cat["_alias_norm"] == q_norm]
    if not exact_alias.empty:
        alias = str(exact_alias.iloc[0]["series_name"])
        return alias, BLS_CPI_SERIES_MAP[alias]

    exact_display = cat[cat["_display_norm"] == q_norm]
    if not exact_display.empty:
        alias = str(exact_display.iloc[0]["series_name"])
        return alias, BLS_CPI_SERIES_MAP[alias]

    exact_series_id = cat[cat["_series_id_norm"] == str(search).strip().upper()]
    if not exact_series_id.empty:
        alias = str(exact_series_id.iloc[0]["series_name"])
        return alias, BLS_CPI_SERIES_MAP[alias]

    partial_display = cat[cat["_display_norm"].str.contains(q_norm, na=False)]
    if not partial_display.empty:
        alias = str(partial_display.iloc[0]["series_name"])
        return alias, BLS_CPI_SERIES_MAP[alias]

    partial_notes = cat[cat["_notes_norm"].str.contains(q_norm, na=False)]
    if not partial_notes.empty:
        alias = str(partial_notes.iloc[0]["series_name"])
        return alias, BLS_CPI_SERIES_MAP[alias]

    return None, None


def default_bls_year_range(year_start=None, year_end=None):
    now_year = datetime.now(timezone.utc).year
    if year_end is None:
        year_end = now_year
    if year_start is None:
        year_start = max(year_end - 3, 1947)
    return year_start, year_end


def ensure_bea_loaded():
    global BEA_CORE_DF
    global BEA_CORE_CATALOG_DF
    global BEA_INDUSTRY_DF
    global BEA_INDUSTRY_CATALOG_DF

    if BEA_CORE_DF is None and LOAD_ERRORS["bea_core"] is None:
        try:
            BEA_CORE_DF = load_csv_if_exists(BEA_CORE_CSV)
        except Exception as e:
            LOAD_ERRORS["bea_core"] = str(e)

    if BEA_CORE_CATALOG_DF is None and LOAD_ERRORS["bea_core_catalog"] is None:
        try:
            BEA_CORE_CATALOG_DF = load_csv_if_exists(BEA_CORE_CATALOG_CSV)
        except Exception as e:
            LOAD_ERRORS["bea_core_catalog"] = str(e)

    if BEA_INDUSTRY_DF is None and LOAD_ERRORS["bea_industry"] is None:
        try:
            BEA_INDUSTRY_DF = load_csv_if_exists(BEA_INDUSTRY_CSV)
        except Exception as e:
            LOAD_ERRORS["bea_industry"] = str(e)

    if BEA_INDUSTRY_CATALOG_DF is None and LOAD_ERRORS["bea_industry_catalog"] is None:
        try:
            BEA_INDUSTRY_CATALOG_DF = load_csv_if_exists(BEA_INDUSTRY_CATALOG_CSV)
        except Exception as e:
            LOAD_ERRORS["bea_industry_catalog"] = str(e)


def ensure_trade_loaded():
    global TRADE_COUNTRIES_DF
    global TRADE_GROUPS_DF
    global TRADE_TOTAL_DF
    global TRADE_PARTNER_MASTER_DF
    global TRADE_CATALOG_DF
    global TRADE_ALIASES_DF
    global TRADE_COUNTRY_LIST_DF
    global TRADE_GROUP_LIST_DF
    global TRADE_SUMMARY

    if TRADE_COUNTRIES_DF is None and LOAD_ERRORS["trade_countries"] is None:
        try:
            TRADE_COUNTRIES_DF = load_csv_if_exists(TRADE_COUNTRIES_CSV)
        except Exception as e:
            LOAD_ERRORS["trade_countries"] = str(e)

    if TRADE_GROUPS_DF is None and LOAD_ERRORS["trade_groups"] is None:
        try:
            TRADE_GROUPS_DF = load_csv_if_exists(TRADE_GROUPS_CSV)
        except Exception as e:
            LOAD_ERRORS["trade_groups"] = str(e)

    if TRADE_TOTAL_DF is None and LOAD_ERRORS["trade_total"] is None:
        try:
            TRADE_TOTAL_DF = load_csv_if_exists(TRADE_TOTAL_CSV)
        except Exception as e:
            LOAD_ERRORS["trade_total"] = str(e)

    if TRADE_PARTNER_MASTER_DF is None and LOAD_ERRORS["trade_partner_master"] is None:
        try:
            TRADE_PARTNER_MASTER_DF = load_csv_if_exists(TRADE_PARTNER_MASTER_CSV)
        except Exception as e:
            LOAD_ERRORS["trade_partner_master"] = str(e)

    if TRADE_CATALOG_DF is None and LOAD_ERRORS["trade_catalog"] is None:
        try:
            TRADE_CATALOG_DF = load_csv_if_exists(TRADE_CATALOG_CSV)
        except Exception as e:
            LOAD_ERRORS["trade_catalog"] = str(e)

    if TRADE_ALIASES_DF is None and LOAD_ERRORS["trade_aliases"] is None:
        try:
            TRADE_ALIASES_DF = load_csv_if_exists(TRADE_ALIASES_CSV)
        except Exception as e:
            LOAD_ERRORS["trade_aliases"] = str(e)

    if TRADE_COUNTRY_LIST_DF is None and LOAD_ERRORS["trade_country_list"] is None:
        try:
            TRADE_COUNTRY_LIST_DF = load_csv_if_exists(TRADE_COUNTRY_LIST_CSV)
        except Exception as e:
            LOAD_ERRORS["trade_country_list"] = str(e)

    if TRADE_GROUP_LIST_DF is None and LOAD_ERRORS["trade_group_list"] is None:
        try:
            TRADE_GROUP_LIST_DF = load_csv_if_exists(TRADE_GROUP_LIST_CSV)
        except Exception as e:
            LOAD_ERRORS["trade_group_list"] = str(e)

    if TRADE_SUMMARY is None and LOAD_ERRORS["trade_summary"] is None and os.path.exists(TRADE_SUMMARY_JSON):
        try:
            with open(TRADE_SUMMARY_JSON, "r", encoding="utf-8") as f:
                TRADE_SUMMARY = json.load(f)
        except Exception as e:
            LOAD_ERRORS["trade_summary"] = str(e)


def ensure_all_loaded():
    ensure_bea_loaded()
    ensure_trade_loaded()


def build_combined_catalog():
    ensure_all_loaded()

    rows = []

    if BEA_CORE_CATALOG_DF is not None and not BEA_CORE_CATALOG_DF.empty:
        core = BEA_CORE_CATALOG_DF.copy()
        for _, row in core.iterrows():
            rows.append({
                "source_block": "bea_core",
                "dataset": row.get("dataset", "BEA"),
                "display_name_pt": row.get("display_name_pt"),
                "frequency": row.get("frequency"),
                "series_name": row.get("series_name"),
                "subcategory": row.get("subcategory"),
                "theme": row.get("theme"),
                "unit": row.get("unit"),
            })

    if BEA_INDUSTRY_CATALOG_DF is not None and not BEA_INDUSTRY_CATALOG_DF.empty:
        ind = BEA_INDUSTRY_CATALOG_DF.copy()
        for _, row in ind.iterrows():
            industry_name_pt = row.get("industry_name_pt")
            metric_name_pt = row.get("metric_name_pt")
            display_name_pt = row.get("display_name_pt")

            if not display_name_pt:
                display_name_pt = f"{industry_name_pt} - {metric_name_pt}"

            rows.append({
                "source_block": "bea_industry",
                "dataset": "GDPbyIndustry",
                "display_name_pt": display_name_pt,
                "frequency": row.get("frequency"),
                "series_name": f"industry::{row.get('industry_code')}::{row.get('metric_code')}::{row.get('frequency')}",
                "subcategory": row.get("bucket", "industry"),
                "theme": row.get("theme", "gdp_industry"),
                "unit": row.get("unit"),
            })

    if TRADE_CATALOG_DF is not None and not TRADE_CATALOG_DF.empty:
        trade = TRADE_CATALOG_DF.copy()
        for _, row in trade.iterrows():
            rows.append({
                "source_block": "trade",
                "dataset": row.get("dataset", "US Census International Trade API"),
                "display_name_pt": row.get("metric_name_pt"),
                "frequency": row.get("frequency"),
                "series_name": row.get("metric_code"),
                "subcategory": row.get("subcategory"),
                "theme": row.get("theme"),
                "unit": row.get("unit"),
            })

    bls_catalog = bls_cpi_catalog_df()
    if bls_catalog is not None and not bls_catalog.empty:
        for _, row in bls_catalog.iterrows():
            rows.append({
                "source_block": row.get("source_block", "bls_cpi"),
                "dataset": row.get("dataset", "BLS_CPI"),
                "display_name_pt": row.get("display_name_pt"),
                "frequency": row.get("frequency", "M"),
                "series_name": row.get("series_name"),
                "subcategory": row.get("subcategory"),
                "theme": row.get("theme", "inflation_cpi"),
                "unit": row.get("unit", "index_1982_84_100"),
            })

    if not rows:
        return pd.DataFrame(columns=[
            "source_block", "dataset", "display_name_pt", "frequency",
            "series_name", "subcategory", "theme", "unit"
        ])

    return pd.DataFrame(rows).drop_duplicates().reset_index(drop=True)

# ============================================================
# RESOLUÇÃO DE ALIASES BEA CORE
# ============================================================
BEA_CORE_ALIASES = {
    "pib nominal": "gdp_nominal",
    "gdp nominal": "gdp_nominal",
    "pib real": "gdp_real",
    "gdp real": "gdp_real",
    "deflator do pib": "gdp_implicit_deflator",
    "inflacao do pib": "gdp_price_change",
    "consumo": "consumption_q",
    "consumo pessoal": "consumption_q",
    "investimento privado": "private_investment_q",
    "exportacoes totais": "exports_total_q",
    "importacoes totais": "imports_total_q",
    "exportacoes liquidas": "net_exports_q",
    "indice pce": "pce_index",
    "pce": "pce_index",
    "core pce": "core_pce_index",
    "indice core pce": "core_pce_index",
    "pce mensal": "pce_mom",
    "core pce mensal": "core_pce_mom",
    "servicos pce": "services_pce_index",
    "indice pce de servicos": "services_pce_index",
    "habitacao e utilities": "housing_utils_pce_index",
    "saude": "healthcare_pce_index",
    "gasolina e energia": "gas_energy_goods_pce_index",
}


def resolve_bea_core_series_name(series_input):
    ensure_bea_loaded()

    if not series_input or BEA_CORE_CATALOG_DF is None or BEA_CORE_CATALOG_DF.empty:
        return None

    series_norm = normalize_text(series_input)

    if series_norm in BEA_CORE_ALIASES:
        return BEA_CORE_ALIASES[series_norm]

    catalog = BEA_CORE_CATALOG_DF.copy()
    catalog["_series_norm"] = catalog["series_name"].astype(str).map(normalize_text)
    catalog["_display_norm"] = catalog["display_name_pt"].astype(str).map(normalize_text)

    exact_series = catalog[catalog["_series_norm"] == series_norm]
    if not exact_series.empty:
        return str(exact_series.iloc[0]["series_name"])

    exact_display = catalog[catalog["_display_norm"] == series_norm]
    if not exact_display.empty:
        return str(exact_display.iloc[0]["series_name"])

    partial_display = catalog[catalog["_display_norm"].str.contains(series_norm, na=False)]
    if not partial_display.empty:
        return str(partial_display.iloc[0]["series_name"])

    partial_series = catalog[catalog["_series_norm"].str.contains(series_norm, na=False)]
    if not partial_series.empty:
        return str(partial_series.iloc[0]["series_name"])

    return None

# ============================================================
# RESOLUÇÃO DE ALIASES BEA INDÚSTRIA
# ============================================================
INDUSTRY_ALIASES = {
    "construcao": "CONSTRUÇÃO",
    "construction": "CONSTRUÇÃO",
    "manufatura": "MANUFATURA",
    "manufacturing": "MANUFATURA",
    "servicos privados": "SERVIÇOS PRIVADOS",
    "services": "SERVIÇOS PRIVADOS",
    "servicos": "SERVIÇOS PRIVADOS",
    "financas e seguros": "FINANÇAS E SEGUROS",
    "finance": "FINANÇAS E SEGUROS",
    "agropecuaria": "AGROPECUÁRIA, SILVICULTURA, PESCA E CAÇA",
    "agriculture": "AGROPECUÁRIA, SILVICULTURA, PESCA E CAÇA",
}


def resolve_bea_industry(industry_input):
    ensure_bea_loaded()

    if not industry_input or BEA_INDUSTRY_DF is None or BEA_INDUSTRY_DF.empty:
        return None

    industry_norm = normalize_text(industry_input)

    if industry_norm in INDUSTRY_ALIASES:
        industry_norm = normalize_text(INDUSTRY_ALIASES[industry_norm])

    base = (
        BEA_INDUSTRY_DF[
            ["industry_code", "industry_name_pt", "industry_desc_en"]
        ]
        .drop_duplicates()
        .copy()
    )
    base["_code_norm"] = base["industry_code"].astype(str).map(normalize_text)
    base["_name_pt_norm"] = base["industry_name_pt"].astype(str).map(normalize_text)
    base["_name_en_norm"] = base["industry_desc_en"].astype(str).map(normalize_text)

    exact_code = base[base["_code_norm"] == industry_norm]
    if not exact_code.empty:
        return {
            "industry_code": str(exact_code.iloc[0]["industry_code"]),
            "industry_name_pt": str(exact_code.iloc[0]["industry_name_pt"]),
        }

    exact_pt = base[base["_name_pt_norm"] == industry_norm]
    if not exact_pt.empty:
        return {
            "industry_code": str(exact_pt.iloc[0]["industry_code"]),
            "industry_name_pt": str(exact_pt.iloc[0]["industry_name_pt"]),
        }

    exact_en = base[base["_name_en_norm"] == industry_norm]
    if not exact_en.empty:
        return {
            "industry_code": str(exact_en.iloc[0]["industry_code"]),
            "industry_name_pt": str(exact_en.iloc[0]["industry_name_pt"]),
        }

    partial_pt = base[base["_name_pt_norm"].str.contains(industry_norm, na=False)]
    if not partial_pt.empty:
        return {
            "industry_code": str(partial_pt.iloc[0]["industry_code"]),
            "industry_name_pt": str(partial_pt.iloc[0]["industry_name_pt"]),
        }

    partial_en = base[base["_name_en_norm"].str.contains(industry_norm, na=False)]
    if not partial_en.empty:
        return {
            "industry_code": str(partial_en.iloc[0]["industry_code"]),
            "industry_name_pt": str(partial_en.iloc[0]["industry_name_pt"]),
        }

    return None


INDUSTRY_METRIC_ALIASES = {
    "value_added_nominal": "value_added_nominal",
    "valor adicionado nominal": "value_added_nominal",
    "nominal": "value_added_nominal",
    "value_added_real": "value_added_real",
    "valor adicionado real": "value_added_real",
    "real": "value_added_real",
    "value_added_price_index": "value_added_price_index",
    "indice de precos": "value_added_price_index",
    "índice de preços": "value_added_price_index",
    "price index": "value_added_price_index",
}


def resolve_bea_industry_metric(metric_input):
    ensure_bea_loaded()

    if not metric_input or BEA_INDUSTRY_DF is None or BEA_INDUSTRY_DF.empty:
        return None

    metric_norm = normalize_text(metric_input)

    if metric_norm in INDUSTRY_METRIC_ALIASES:
        return INDUSTRY_METRIC_ALIASES[metric_norm]

    base = (
        BEA_INDUSTRY_DF[
            ["metric_code", "metric_name_pt"]
        ]
        .drop_duplicates()
        .copy()
    )
    base["_metric_code_norm"] = base["metric_code"].astype(str).map(normalize_text)
    base["_metric_name_norm"] = base["metric_name_pt"].astype(str).map(normalize_text)

    exact_code = base[base["_metric_code_norm"] == metric_norm]
    if not exact_code.empty:
        return str(exact_code.iloc[0]["metric_code"])

    exact_name = base[base["_metric_name_norm"] == metric_norm]
    if not exact_name.empty:
        return str(exact_name.iloc[0]["metric_code"])

    partial_name = base[base["_metric_name_norm"].str.contains(metric_norm, na=False)]
    if not partial_name.empty:
        return str(partial_name.iloc[0]["metric_code"])

    return None

# ============================================================
# RESOLUÇÃO DE ALIASES TRADE
# ============================================================
def resolve_partner_name(partner_input):
    ensure_trade_loaded()

    if not partner_input:
        return None

    partner_norm = normalize_text(partner_input)

    if TRADE_ALIASES_DF is not None and not TRADE_ALIASES_DF.empty:
        aliases = TRADE_ALIASES_DF.copy()
        aliases["_alias_norm"] = aliases["alias_input"].astype(str).map(normalize_text)

        alias_match = aliases[aliases["_alias_norm"] == partner_norm]
        if not alias_match.empty:
            return str(alias_match.iloc[0]["country_name_norm"])

    if TRADE_PARTNER_MASTER_DF is not None and not TRADE_PARTNER_MASTER_DF.empty:
        master = TRADE_PARTNER_MASTER_DF.copy()
        master["_name_norm"] = master["country_name"].astype(str).map(normalize_text)

        exact = master[master["_name_norm"] == partner_norm]
        if not exact.empty:
            return str(exact.iloc[0]["country_name"])

        partial = master[master["_name_norm"].str.contains(partner_norm, na=False)]
        if not partial.empty:
            return str(partial.iloc[0]["country_name"])

    return None


def get_partner_type(partner_name):
    ensure_trade_loaded()

    if TRADE_PARTNER_MASTER_DF is None or TRADE_PARTNER_MASTER_DF.empty:
        return None

    match = TRADE_PARTNER_MASTER_DF[TRADE_PARTNER_MASTER_DF["country_name"] == partner_name]
    if match.empty:
        return None

    return str(match.iloc[0]["partner_type"])


def get_trade_df_by_partner_type(partner_type):
    ensure_trade_loaded()

    if partner_type == "country":
        return TRADE_COUNTRIES_DF
    if partner_type == "group_or_region":
        return TRADE_GROUPS_DF
    if partner_type == "total":
        return TRADE_TOTAL_DF
    return None


def pivot_trade_metrics(df):
    if df is None or df.empty:
        return pd.DataFrame()

    out = (
        df.pivot_table(
            index=["date", "country_code", "country_name"],
            columns="metric_code",
            values="value",
            aggfunc="first"
        )
        .reset_index()
        .sort_values(["date", "country_name"])
    )
    out.columns.name = None
    return out

# ============================================================
# ENDPOINTS BÁSICOS
# ============================================================
@app.route("/", methods=["GET"])
def root():
    return jsonify({
        "ok": True,
        "service": "us-macro-bea-api",
        "message": "API ativa",
        "available_endpoints": [
            "/health",
            "/catalog",
            "/bea/health",
            "/bea/catalog",
            "/bea/query",
            "/bea/industry/query",
            "/trade/health",
            "/trade/catalog",
            "/trade/partners",
            "/trade/query",
            "/trade/brazil",
            "/brazil",
            "/bls/cpi/health",
            "/bls/cpi/catalog",
            "/bls/cpi/query",
        ]
    })


@app.route("/health", methods=["GET"])
def health():
    ensure_all_loaded()
    combined_catalog = build_combined_catalog()

    return jsonify({
        "ok": True,
        "timestamp_utc": utc_now_iso(),
        "has_bea_key": bool(BEA_API_KEY),
        "has_bls_key": bool(BLS_API_KEY),
        "bea_files": bea_files_status(),
        "trade_files": trade_files_status(),
        "bls_cpi_catalog_count": int(len(bls_cpi_catalog_df())),
        "catalog_count": int(len(combined_catalog)),
        "load_errors": LOAD_ERRORS
    })


@app.route("/catalog", methods=["GET"])
def catalog():
    combined_catalog = build_combined_catalog()

    return jsonify({
        "ok": True,
        "count": int(len(combined_catalog)),
        "series": df_to_records(combined_catalog, max_rows=5000)
    })

# ============================================================
# ENDPOINTS BEA
# ============================================================
@app.route("/bea/health", methods=["GET"])
def bea_health():
    ensure_bea_loaded()

    return jsonify({
        "ok": True,
        "timestamp_utc": utc_now_iso(),
        "files": bea_files_status(),
        "core_rows": 0 if BEA_CORE_DF is None else int(len(BEA_CORE_DF)),
        "core_catalog_rows": 0 if BEA_CORE_CATALOG_DF is None else int(len(BEA_CORE_CATALOG_DF)),
        "industry_rows": 0 if BEA_INDUSTRY_DF is None else int(len(BEA_INDUSTRY_DF)),
        "industry_catalog_rows": 0 if BEA_INDUSTRY_CATALOG_DF is None else int(len(BEA_INDUSTRY_CATALOG_DF)),
    })


@app.route("/bea/catalog", methods=["GET"])
def bea_catalog():
    ensure_bea_loaded()

    return jsonify({
        "ok": True,
        "core_count": 0 if BEA_CORE_CATALOG_DF is None else int(len(BEA_CORE_CATALOG_DF)),
        "industry_count": 0 if BEA_INDUSTRY_CATALOG_DF is None else int(len(BEA_INDUSTRY_CATALOG_DF)),
        "core_catalog": df_to_records(BEA_CORE_CATALOG_DF, max_rows=1000),
        "industry_catalog": df_to_records(BEA_INDUSTRY_CATALOG_DF, max_rows=1000),
    })


@app.route("/bea/query", methods=["GET"])
def bea_query():
    ensure_bea_loaded()

    if BEA_CORE_DF is None or BEA_CORE_DF.empty:
        return jsonify({
            "ok": False,
            "error": "Base BEA principal não encontrada."
        }), 404

    series_input = request.args.get("series_name", "").strip()
    q = request.args.get("q", "").strip()
    theme = request.args.get("theme", "").strip().lower()
    subcategory = request.args.get("subcategory", "").strip().lower()
    frequency = request.args.get("frequency", "").strip().upper()
    year_start = safe_int(request.args.get("year_start"))
    year_end = safe_int(request.args.get("year_end"))
    max_rows = safe_int(request.args.get("max_rows")) or 5000

    resolved_series_name = None
    if series_input:
        resolved_series_name = resolve_bea_core_series_name(series_input)
    elif q:
        resolved_series_name = resolve_bea_core_series_name(q)

    df = BEA_CORE_DF.copy()

    if resolved_series_name:
        df = df[df["series_name"] == resolved_series_name].copy()

    if theme:
        df = df[df["theme"].astype(str).map(normalize_text) == normalize_text(theme)].copy()

    if subcategory:
        df = df[df["subcategory"].astype(str).map(normalize_text) == normalize_text(subcategory)].copy()

    if frequency:
        df = df[df["frequency"].astype(str).str.upper() == frequency].copy()

    df = apply_year_filter(df, year_start=year_start, year_end=year_end)

    if df.empty:
        return jsonify({
            "ok": False,
            "error": "Nenhum dado encontrado para os filtros informados.",
            "series_input": series_input or q or None,
            "series_resolved": resolved_series_name,
            "theme": theme or None,
            "subcategory": subcategory or None,
            "frequency": frequency or None,
            "year_start": year_start,
            "year_end": year_end
        }), 404

    sort_cols = [col for col in ["series_name", "date"] if col in df.columns]
    df = df.sort_values(sort_cols).reset_index(drop=True)

    return jsonify({
        "ok": True,
        "series_input": series_input or q or None,
        "series_resolved": resolved_series_name,
        "theme": theme or None,
        "subcategory": subcategory or None,
        "frequency": frequency or None,
        "year_start": year_start,
        "year_end": year_end,
        "rows": int(len(df)),
        "data": df_to_records(df, max_rows=max_rows)
    })


@app.route("/bea/industry/query", methods=["GET"])
def bea_industry_query():
    ensure_bea_loaded()

    if BEA_INDUSTRY_DF is None or BEA_INDUSTRY_DF.empty:
        return jsonify({
            "ok": False,
            "error": "Base BEA de indústria não encontrada."
        }), 404

    industry_input = request.args.get("industry", "").strip()
    metric_input = request.args.get("metric", "").strip() or request.args.get("metric_code", "").strip()
    frequency = request.args.get("frequency", "").strip().upper()
    year_start = safe_int(request.args.get("year_start"))
    year_end = safe_int(request.args.get("year_end"))
    max_rows = safe_int(request.args.get("max_rows")) or 5000

    resolved_industry = None
    resolved_metric_code = None

    if industry_input:
        resolved_industry = resolve_bea_industry(industry_input)
        if not resolved_industry:
            return jsonify({
                "ok": False,
                "error": "Indústria não encontrada.",
                "industry_input": industry_input
            }), 404

    if metric_input:
        resolved_metric_code = resolve_bea_industry_metric(metric_input)
        if not resolved_metric_code:
            return jsonify({
                "ok": False,
                "error": "Métrica de indústria não encontrada.",
                "metric_input": metric_input
            }), 404

    df = BEA_INDUSTRY_DF.copy()

    if resolved_industry:
        df = df[df["industry_code"].astype(str) == str(resolved_industry["industry_code"])].copy()

    if resolved_metric_code:
        df = df[df["metric_code"].astype(str) == str(resolved_metric_code)].copy()

    if frequency:
        df = df[df["frequency"].astype(str).str.upper() == frequency].copy()

    df = apply_year_filter(df, year_start=year_start, year_end=year_end)

    if df.empty:
        return jsonify({
            "ok": False,
            "error": "Nenhum dado encontrado para os filtros informados.",
            "industry_input": industry_input or None,
            "industry_resolved": resolved_industry,
            "metric_input": metric_input or None,
            "metric_resolved": resolved_metric_code,
            "frequency": frequency or None,
            "year_start": year_start,
            "year_end": year_end
        }), 404

    sort_cols = [col for col in ["industry_name_pt", "metric_code", "date"] if col in df.columns]
    df = df.sort_values(sort_cols).reset_index(drop=True)

    return jsonify({
        "ok": True,
        "industry_input": industry_input or None,
        "industry_resolved": resolved_industry,
        "metric_input": metric_input or None,
        "metric_resolved": resolved_metric_code,
        "frequency": frequency or None,
        "year_start": year_start,
        "year_end": year_end,
        "rows": int(len(df)),
        "data": df_to_records(df, max_rows=max_rows)
    })

# ============================================================
# ENDPOINTS TRADE
# ============================================================
@app.route("/trade/health", methods=["GET"])
def trade_health():
    ensure_trade_loaded()

    return jsonify({
        "ok": True,
        "timestamp_utc": utc_now_iso(),
        "files": trade_files_status(),
        "summary": TRADE_SUMMARY
    })


@app.route("/trade/catalog", methods=["GET"])
def trade_catalog():
    ensure_trade_loaded()

    if TRADE_CATALOG_DF is None or TRADE_CATALOG_DF.empty:
        return jsonify({
            "ok": False,
            "error": "Catálogo de trade não encontrado."
        }), 404

    return jsonify({
        "ok": True,
        "count": int(len(TRADE_CATALOG_DF)),
        "catalog": df_to_records(TRADE_CATALOG_DF, max_rows=1000)
    })


@app.route("/trade/partners", methods=["GET"])
def trade_partners():
    ensure_trade_loaded()

    partner_type = request.args.get("partner_type", "").strip().lower()
    q = request.args.get("q", "").strip()

    if TRADE_PARTNER_MASTER_DF is None or TRADE_PARTNER_MASTER_DF.empty:
        return jsonify({
            "ok": False,
            "error": "Tabela mestre de parceiros não encontrada."
        }), 404

    df = TRADE_PARTNER_MASTER_DF.copy()

    if partner_type:
        df = df[df["partner_type"] == partner_type]

    if q:
        q_norm = normalize_text(q)
        df = df[df["country_name"].astype(str).map(normalize_text).str.contains(q_norm, na=False)]

    df = df.sort_values(["partner_type", "country_name"]).reset_index(drop=True)

    return jsonify({
        "ok": True,
        "count": int(len(df)),
        "partners": df_to_records(df, max_rows=1000)
    })


@app.route("/trade/query", methods=["GET"])
def trade_query():
    ensure_trade_loaded()

    partner = request.args.get("partner", "").strip()
    partner_type = request.args.get("partner_type", "").strip().lower()
    metric = request.args.get("metric", "").strip().lower()
    year_start = safe_int(request.args.get("year_start"))
    year_end = safe_int(request.args.get("year_end"))
    pivot = request.args.get("pivot", "true").strip().lower() in {"1", "true", "yes", "y"}

    if partner:
        resolved_partner = resolve_partner_name(partner)
        if not resolved_partner:
            return jsonify({
                "ok": False,
                "error": "Parceiro não encontrado.",
                "partner_input": partner
            }), 404

        resolved_partner_type = get_partner_type(resolved_partner)
        df = get_trade_df_by_partner_type(resolved_partner_type)

        if df is None or df.empty:
            return jsonify({
                "ok": False,
                "error": "Base de trade não disponível para o tipo do parceiro."
            }), 404

        df = df[df["country_name"] == resolved_partner].copy()

    else:
        if not partner_type:
            return jsonify({
                "ok": False,
                "error": "Informe 'partner' ou 'partner_type'."
            }), 400

        df = get_trade_df_by_partner_type(partner_type)

        if df is None or df.empty:
            return jsonify({
                "ok": False,
                "error": "partner_type inválido ou base indisponível.",
                "allowed_partner_types": ["country", "group_or_region", "total"]
            }), 400

        resolved_partner = None
        resolved_partner_type = partner_type
        df = df.copy()

    df = apply_year_filter(df, year_start=year_start, year_end=year_end)

    if metric:
        df = df[df["metric_code"] == metric].copy()

    if df.empty:
        return jsonify({
            "ok": False,
            "error": "Nenhum dado encontrado para os filtros informados.",
            "partner": resolved_partner,
            "partner_type": resolved_partner_type,
            "metric": metric or None,
            "year_start": year_start,
            "year_end": year_end
        }), 404

    if pivot and partner:
        result_df = pivot_trade_metrics(df)
    else:
        result_df = df.sort_values(["date", "country_name", "metric_code"]).reset_index(drop=True)

    return jsonify({
        "ok": True,
        "partner_input": partner if partner else None,
        "partner_resolved": resolved_partner,
        "partner_type": resolved_partner_type,
        "metric": metric if metric else None,
        "year_start": year_start,
        "year_end": year_end,
        "rows": int(len(result_df)),
        "data": df_to_records(result_df, max_rows=5000)
    })


@app.route("/trade/brazil", methods=["GET"])
@app.route("/brazil", methods=["GET"])
def trade_brazil():
    ensure_trade_loaded()

    if TRADE_COUNTRIES_DF is None or TRADE_COUNTRIES_DF.empty:
        return jsonify({
            "ok": False,
            "error": "Base de países não encontrada."
        }), 404

    year_start = safe_int(request.args.get("year_start", 2015))
    year_end = safe_int(request.args.get("year_end", 2025))

    df = TRADE_COUNTRIES_DF.copy()
    df = df[df["country_name"] == "BRAZIL"].copy()
    df = apply_year_filter(df, year_start=year_start, year_end=year_end)

    result_df = pivot_trade_metrics(df)

    return jsonify({
        "ok": True,
        "partner_resolved": "BRAZIL",
        "partner_type": "country",
        "year_start": year_start,
        "year_end": year_end,
        "rows": int(len(result_df)),
        "data": df_to_records(result_df, max_rows=200)
    })

# ============================================================
# ENDPOINTS BLS CPI
# ============================================================
@app.route("/bls/cpi/health", methods=["GET"])
def bls_cpi_health():
    current_year = datetime.now(timezone.utc).year
    startyear = max(current_year - 1, 1947)

    out = {
        "ok": True,
        "timestamp_utc": utc_now_iso(),
        "has_bls_key": bool(BLS_API_KEY),
        "catalog_rows": int(len(bls_cpi_catalog_df())),
        "api_url": BLS_API_URL,
        "load_errors": {
            "bls_cpi": LOAD_ERRORS.get("bls_cpi")
        },
    }

    try:
        series_list = fetch_bls_series([
            BLS_CPI_SERIES_MAP["cpi_headline"]["series_id"]
        ], startyear=startyear, endyear=current_year)
        probe_df = parse_bls_series_to_df(series_list[0]) if series_list else pd.DataFrame()
        out["probe_rows"] = 0 if probe_df.empty else int(len(probe_df))
        out["last_available_date"] = None if probe_df.empty else str(probe_df.iloc[-1]["date"])
        LOAD_ERRORS["bls_cpi"] = None
    except Exception as e:
        LOAD_ERRORS["bls_cpi"] = str(e)
        out["ok"] = False
        out["error"] = str(e)
        out["probe_rows"] = 0
        out["last_available_date"] = None

    return jsonify(out)


@app.route("/bls/cpi/catalog", methods=["GET"])
def bls_cpi_catalog():
    q = request.args.get("q", "").strip()
    subcategory = request.args.get("subcategory", "").strip().lower()
    max_rows = safe_int(request.args.get("max_rows")) or 500

    df = bls_cpi_catalog_df().copy()

    if q:
        q_norm = normalize_text(q)
        search_blob = (
            df[["series_name", "series_id", "display_name_pt", "notes"]]
            .fillna("")
            .astype(str)
            .agg(" | ".join, axis=1)
            .map(normalize_text)
        )
        df = df[search_blob.str.contains(q_norm, na=False)].copy()

    if subcategory:
        df = df[df["subcategory"].astype(str).map(normalize_text) == normalize_text(subcategory)].copy()

    df = df.sort_values(["subcategory", "display_name_pt", "series_name"]).reset_index(drop=True)

    return jsonify({
        "ok": True,
        "count": int(len(df)),
        "q": q or None,
        "subcategory": subcategory or None,
        "catalog": df_to_records(df, max_rows=max_rows)
    })


@app.route("/bls/cpi/query", methods=["GET"])
def bls_cpi_query():
    series_input = request.args.get("series_name", "").strip()
    q = request.args.get("q", "").strip()
    calc = request.args.get("calc", "index").strip().lower()
    year_start = safe_int(request.args.get("year_start"))
    year_end = safe_int(request.args.get("year_end"))
    max_rows = safe_int(request.args.get("max_rows")) or 5000

    year_start, year_end = default_bls_year_range(year_start=year_start, year_end=year_end)

    alias, meta = resolve_bls_cpi_series_name(series_input=series_input or None, q=q or None)
    if not alias or not meta:
        return jsonify({
            "ok": False,
            "error": "Série CPI não encontrada no catálogo curado do backend.",
            "series_input": series_input or q or None,
            "allowed_series_names": sorted(BLS_CPI_SERIES_MAP.keys()),
        }), 404

    try:
        series_list = fetch_bls_series([meta["series_id"]], startyear=year_start, endyear=year_end)
        if not series_list:
            raise RuntimeError("A BLS API não retornou série para a consulta solicitada.")

        raw_df = parse_bls_series_to_df(series_list[0])
        if raw_df.empty:
            return jsonify({
                "ok": False,
                "error": "Nenhuma observação mensal encontrada para a série solicitada.",
                "series_input": series_input or q or None,
                "series_resolved": alias,
                "series_id": meta["series_id"],
                "year_start": year_start,
                "year_end": year_end,
            }), 404

        result_df, unit = compute_bls_calc(raw_df, calc=calc)
        if result_df.empty:
            return jsonify({
                "ok": False,
                "error": "Não há observações suficientes para o cálculo solicitado no intervalo informado.",
                "series_input": series_input or q or None,
                "series_resolved": alias,
                "series_id": meta["series_id"],
                "calc": calc,
                "year_start": year_start,
                "year_end": year_end,
            }), 404

        LOAD_ERRORS["bls_cpi"] = None
    except Exception as e:
        LOAD_ERRORS["bls_cpi"] = str(e)
        return jsonify({
            "ok": False,
            "error": str(e),
            "series_input": series_input or q or None,
            "series_resolved": alias,
            "series_id": meta["series_id"],
            "calc": calc,
            "year_start": year_start,
            "year_end": year_end,
        }), 502

    return jsonify({
        "ok": True,
        "series_input": series_input or q or None,
        "series_resolved": alias,
        "series_id": meta["series_id"],
        "display_name_pt": meta.get("display_name_pt"),
        "theme": meta.get("theme"),
        "subcategory": meta.get("subcategory"),
        "frequency": "M",
        "calc": calc,
        "unit": unit,
        "notes": meta.get("notes"),
        "year_start": year_start,
        "year_end": year_end,
        "rows": int(len(result_df)),
        "last_available_date": None if result_df.empty else str(result_df.iloc[-1]["date"]),
        "data": df_to_records(result_df[["date", "value", "series_id", "period_name"]], max_rows=max_rows)
    })

# ============================================================
# INICIALIZAÇÃO
# ============================================================
if __name__ == "__main__":
    port = int(os.environ.get("PORT", 10000))
    app.run(host="0.0.0.0", port=port)
