import os
import json
import time
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
BLS_CPI_META_DIR = os.path.join(DATA_DIR, "bls_cpi_meta")

BEA_API_KEY = os.getenv("BEA_API_KEY", "").strip()
BLS_API_KEY = os.getenv("BLS_API_KEY", "").strip()
BLS_CPI_META_REFRESH_HOURS = int(os.getenv("BLS_CPI_META_REFRESH_HOURS", "168") or 168)
BLS_API_CACHE_TTL_SECONDS = int(os.getenv("BLS_API_CACHE_TTL_SECONDS", "21600") or 21600)

BLS_PUBLIC_API_URL = "https://api.bls.gov/publicAPI/v2/timeseries/data/"
BLS_CPI_META_BASE_URLS = [
    "https://download.bls.gov/pub/time.series/cu/",
    "https://download.bls.gov/pub/time.series/CU/",
]

BLS_HTTP_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/123.0.0.0 Safari/537.36"
    ),
    "Accept": "text/plain,text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.9,pt-BR;q=0.8,pt;q=0.7",
    "Connection": "keep-alive",
}
BLS_CPI_META_FILES = {
    "series": "cu.series",
    "area": "cu.area",
    "item": "cu.item",
    "period": "cu.period",
    "seasonal": "cu.seasonal",
    "footnote": "cu.footnote",
}

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
# ARQUIVOS / CACHE LOCAL BLS CPI (METADADOS)
# ============================================================
BLS_CPI_SERIES_TXT = os.path.join(BLS_CPI_META_DIR, "cu.series")
BLS_CPI_AREA_TXT = os.path.join(BLS_CPI_META_DIR, "cu.area")
BLS_CPI_ITEM_TXT = os.path.join(BLS_CPI_META_DIR, "cu.item")
BLS_CPI_PERIOD_TXT = os.path.join(BLS_CPI_META_DIR, "cu.period")
BLS_CPI_SEASONAL_TXT = os.path.join(BLS_CPI_META_DIR, "cu.seasonal")
BLS_CPI_FOOTNOTE_TXT = os.path.join(BLS_CPI_META_DIR, "cu.footnote")

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

BLS_CPI_SERIES_DF = None
BLS_CPI_AREA_DF = None
BLS_CPI_ITEM_DF = None
BLS_CPI_PERIOD_DF = None
BLS_CPI_SEASONAL_DF = None
BLS_CPI_FOOTNOTE_DF = None
BLS_CPI_CATALOG_DF = None
BLS_API_CACHE = {}

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
    "bls_cpi_series": None,
    "bls_cpi_area": None,
    "bls_cpi_item": None,
    "bls_cpi_period": None,
    "bls_cpi_seasonal": None,
    "bls_cpi_footnote": None,
    "bls_cpi_catalog": None,
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
        return float(str(value).strip().replace(",", ""))
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


def ensure_dir(path):
    os.makedirs(path, exist_ok=True)


def file_age_hours(path):
    if not os.path.exists(path):
        return None
    age_seconds = time.time() - os.path.getmtime(path)
    return age_seconds / 3600.0


def should_refresh_file(path, max_age_hours):
    if not os.path.exists(path):
        return True
    age = file_age_hours(path)
    return age is None or age >= max_age_hours


def download_text_file(url, path, max_age_hours=168, force=False, timeout=60):
    ensure_dir(os.path.dirname(path))

    if not force and not should_refresh_file(path, max_age_hours):
        return path

    response = requests.get(url, headers=BLS_HTTP_HEADERS, timeout=timeout)
    response.raise_for_status()

    with open(path, "w", encoding="utf-8") as f:
        f.write(response.text)

    return path


def load_delimited_text_if_exists(path):
    if not os.path.exists(path):
        return None

    encodings_to_try = ["utf-8", "utf-8-sig", "cp1252", "latin1"]
    last_error = None

    for enc in encodings_to_try:
        try:
            df = pd.read_csv(path, sep="\t", dtype=str, encoding=enc)
            if len(df.columns) > 1:
                df.columns = [str(c).strip() for c in df.columns]
                return df
        except Exception as e:
            last_error = e

    for enc in encodings_to_try:
        try:
            df = pd.read_csv(path, sep=r"\s{2,}|\t", engine="python", dtype=str, encoding=enc)
            if len(df.columns) > 1:
                df.columns = [str(c).strip() for c in df.columns]
                return df
        except Exception as e:
            last_error = e

    raise RuntimeError(f"Falha ao ler arquivo de texto delimitado {path}. Último erro: {last_error}")


def normalize_df_string_columns(df):
    if df is None or df.empty:
        return df

    out = df.copy()
    out.columns = [str(c).strip() for c in out.columns]
    for col in out.columns:
        if out[col].dtype == object:
            out[col] = out[col].astype(str).str.strip()
            out.loc[out[col].isin(["", "nan", "None", "NaN"]), col] = None
    return out


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


def bls_cpi_files_status():
    return {
        "meta_dir_exists": os.path.isdir(BLS_CPI_META_DIR),
        "series_txt": os.path.exists(BLS_CPI_SERIES_TXT),
        "area_txt": os.path.exists(BLS_CPI_AREA_TXT),
        "item_txt": os.path.exists(BLS_CPI_ITEM_TXT),
        "period_txt": os.path.exists(BLS_CPI_PERIOD_TXT),
        "seasonal_txt": os.path.exists(BLS_CPI_SEASONAL_TXT),
        "footnote_txt": os.path.exists(BLS_CPI_FOOTNOTE_TXT),
    }


def standardize_bls_metadata_columns(df, kind):
    if df is None or df.empty:
        return df

    out = normalize_df_string_columns(df)
    cols = {c.lower(): c for c in out.columns}
    rename_map = {}

    if kind == "area":
        if "area_text" in cols and "area_name" not in cols:
            rename_map[cols["area_text"]] = "area_name"
    elif kind == "item":
        if "item_text" in cols and "item_name" not in cols:
            rename_map[cols["item_text"]] = "item_name"
    elif kind == "seasonal":
        if "seasonal_name" in cols and "seasonal_text" not in cols:
            rename_map[cols["seasonal_name"]] = "seasonal_text"
    elif kind == "period":
        if "period_name" not in cols and "period_text" in cols:
            rename_map[cols["period_text"]] = "period_name"
    elif kind == "footnote":
        if "footnote_text" not in cols and "footnote" in cols:
            rename_map[cols["footnote"]] = "footnote_text"

    if rename_map:
        out = out.rename(columns=rename_map)

    return out


def download_bls_cpi_metadata_if_needed(force=False):
    ensure_dir(BLS_CPI_META_DIR)

    last_error = None

    for key, filename in BLS_CPI_META_FILES.items():
        path = os.path.join(BLS_CPI_META_DIR, filename)

        if not force and not should_refresh_file(path, BLS_CPI_META_REFRESH_HOURS):
            continue

        downloaded = False

        for base_url in BLS_CPI_META_BASE_URLS:
            url = f"{base_url}{filename}"
            try:
                download_text_file(
                    url=url,
                    path=path,
                    max_age_hours=BLS_CPI_META_REFRESH_HOURS,
                    force=True,
                    timeout=60,
                )
                downloaded = True
                break
            except Exception as e:
                last_error = e

        if not downloaded:
            raise RuntimeError(f"Falha ao baixar {filename}. Último erro: {last_error}")


def build_bls_cpi_catalog():
    if BLS_CPI_SERIES_DF is None or BLS_CPI_SERIES_DF.empty:
        return pd.DataFrame()

    catalog = BLS_CPI_SERIES_DF.copy()

    if BLS_CPI_AREA_DF is not None and not BLS_CPI_AREA_DF.empty and "area_code" in catalog.columns:
        area_cols = [c for c in ["area_code", "area_name"] if c in BLS_CPI_AREA_DF.columns]
        catalog = catalog.merge(BLS_CPI_AREA_DF[area_cols].drop_duplicates(), on="area_code", how="left")

    if BLS_CPI_ITEM_DF is not None and not BLS_CPI_ITEM_DF.empty and "item_code" in catalog.columns:
        item_cols = [c for c in ["item_code", "item_name"] if c in BLS_CPI_ITEM_DF.columns]
        catalog = catalog.merge(BLS_CPI_ITEM_DF[item_cols].drop_duplicates(), on="item_code", how="left")

    if BLS_CPI_SEASONAL_DF is not None and not BLS_CPI_SEASONAL_DF.empty and "seasonal" in catalog.columns:
        seasonal_cols = [c for c in ["seasonal", "seasonal_text"] if c in BLS_CPI_SEASONAL_DF.columns]
        catalog = catalog.merge(BLS_CPI_SEASONAL_DF[seasonal_cols].drop_duplicates(), on="seasonal", how="left")

    periodicity_map = {
        "R": "M",
        "S": "S",
    }
    catalog["frequency"] = catalog["periodicity_code"].map(periodicity_map).fillna(catalog.get("periodicity_code"))
    catalog["dataset"] = "BLS_CPI"
    catalog["source_block"] = "bls_cpi"
    catalog["theme"] = "inflation_cpi"
    catalog["unit"] = "index"
    catalog["series_name"] = catalog["series_id"]

    if "series_title" in catalog.columns:
        catalog["display_name_pt"] = catalog["series_title"]
    else:
        catalog["display_name_pt"] = catalog["series_id"]

    if "item_name" in catalog.columns:
        catalog["subcategory"] = catalog["item_name"]
    elif "item_code" in catalog.columns:
        catalog["subcategory"] = catalog["item_code"]
    else:
        catalog["subcategory"] = "cpi"

    preferred_cols = [
        "source_block",
        "dataset",
        "display_name_pt",
        "frequency",
        "series_name",
        "subcategory",
        "theme",
        "unit",
        "series_id",
        "series_title",
        "area_code",
        "area_name",
        "item_code",
        "item_name",
        "seasonal",
        "seasonal_text",
        "base_code",
        "base_period",
        "begin_year",
        "begin_period",
        "end_year",
        "end_period",
    ]
    existing_cols = [c for c in preferred_cols if c in catalog.columns]
    return catalog[existing_cols].drop_duplicates().reset_index(drop=True)


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


def ensure_bls_cpi_loaded(force_refresh=False):
    global BLS_CPI_SERIES_DF
    global BLS_CPI_AREA_DF
    global BLS_CPI_ITEM_DF
    global BLS_CPI_PERIOD_DF
    global BLS_CPI_SEASONAL_DF
    global BLS_CPI_FOOTNOTE_DF
    global BLS_CPI_CATALOG_DF

    try:
        download_bls_cpi_metadata_if_needed(force=force_refresh)
    except Exception as e:
        if LOAD_ERRORS["bls_cpi_series"] is None:
            LOAD_ERRORS["bls_cpi_series"] = str(e)
        raise

    if BLS_CPI_SERIES_DF is None and LOAD_ERRORS["bls_cpi_series"] is None:
        try:
            BLS_CPI_SERIES_DF = standardize_bls_metadata_columns(load_delimited_text_if_exists(BLS_CPI_SERIES_TXT), "series")
        except Exception as e:
            LOAD_ERRORS["bls_cpi_series"] = str(e)

    if BLS_CPI_AREA_DF is None and LOAD_ERRORS["bls_cpi_area"] is None:
        try:
            BLS_CPI_AREA_DF = standardize_bls_metadata_columns(load_delimited_text_if_exists(BLS_CPI_AREA_TXT), "area")
        except Exception as e:
            LOAD_ERRORS["bls_cpi_area"] = str(e)

    if BLS_CPI_ITEM_DF is None and LOAD_ERRORS["bls_cpi_item"] is None:
        try:
            BLS_CPI_ITEM_DF = standardize_bls_metadata_columns(load_delimited_text_if_exists(BLS_CPI_ITEM_TXT), "item")
        except Exception as e:
            LOAD_ERRORS["bls_cpi_item"] = str(e)

    if BLS_CPI_PERIOD_DF is None and LOAD_ERRORS["bls_cpi_period"] is None:
        try:
            BLS_CPI_PERIOD_DF = standardize_bls_metadata_columns(load_delimited_text_if_exists(BLS_CPI_PERIOD_TXT), "period")
        except Exception as e:
            LOAD_ERRORS["bls_cpi_period"] = str(e)

    if BLS_CPI_SEASONAL_DF is None and LOAD_ERRORS["bls_cpi_seasonal"] is None:
        try:
            BLS_CPI_SEASONAL_DF = standardize_bls_metadata_columns(load_delimited_text_if_exists(BLS_CPI_SEASONAL_TXT), "seasonal")
        except Exception as e:
            LOAD_ERRORS["bls_cpi_seasonal"] = str(e)

    if BLS_CPI_FOOTNOTE_DF is None and LOAD_ERRORS["bls_cpi_footnote"] is None:
        try:
            BLS_CPI_FOOTNOTE_DF = standardize_bls_metadata_columns(load_delimited_text_if_exists(BLS_CPI_FOOTNOTE_TXT), "footnote")
        except Exception as e:
            LOAD_ERRORS["bls_cpi_footnote"] = str(e)

    if BLS_CPI_CATALOG_DF is None and LOAD_ERRORS["bls_cpi_catalog"] is None:
        try:
            BLS_CPI_CATALOG_DF = build_bls_cpi_catalog()
        except Exception as e:
            LOAD_ERRORS["bls_cpi_catalog"] = str(e)


def ensure_all_loaded():
    ensure_bea_loaded()
    ensure_trade_loaded()
    ensure_bls_cpi_loaded()


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

    if BLS_CPI_CATALOG_DF is not None and not BLS_CPI_CATALOG_DF.empty:
        bls = BLS_CPI_CATALOG_DF.copy()
        for _, row in bls.iterrows():
            rows.append({
                "source_block": "bls_cpi",
                "dataset": row.get("dataset", "BLS_CPI"),
                "display_name_pt": row.get("display_name_pt"),
                "frequency": row.get("frequency"),
                "series_name": row.get("series_name"),
                "subcategory": row.get("subcategory"),
                "theme": row.get("theme"),
                "unit": row.get("unit"),
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
# RESOLUÇÃO / CONSULTA BLS CPI
# ============================================================
BLS_CPI_SERIES_ALIASES = {
    "cpi": "CUSR0000SA0",
    "headline cpi": "CUSR0000SA0",
    "all items cpi": "CUSR0000SA0",
    "core cpi": "CUSR0000SA0L1E",
    "core cpi mom": "CUSR0000SA0L1E",
    "nucleo cpi": "CUSR0000SA0L1E",
    "núcleo cpi": "CUSR0000SA0L1E",
    "core inflation cpi": "CUSR0000SA0L1E",
    "gasoline cpi": "CUSR0000SETB01",
    "gasoline": "CUSR0000SETB01",
    "gasolina": "CUSR0000SETB01",
    "inflacao de gasolina": "CUSR0000SETB01",
    "inflação de gasolina": "CUSR0000SETB01",
    "energy cpi": "CUSR0000SA0E",
    "shelter cpi": "CUSR0000SAH1",
    "services cpi": "CUSR0000SAS",
}


def normalize_seasonal_input(value):
    norm = normalize_text(value)
    mapping = {
        "s": "S",
        "seasonally adjusted": "S",
        "seasonallyadjusted": "S",
        "ajustado sazonalmente": "S",
        "u": "U",
        "unadjusted": "U",
        "not seasonally adjusted": "U",
        "nao ajustado": "U",
        "não ajustado": "U",
        "nao ajustado sazonalmente": "U",
        "não ajustado sazonalmente": "U",
    }
    return mapping.get(norm, value)


def search_bls_cpi_catalog(q=None, area=None, item=None, seasonal=None, monthly_only=True):
    ensure_bls_cpi_loaded()

    if BLS_CPI_CATALOG_DF is None or BLS_CPI_CATALOG_DF.empty:
        return pd.DataFrame()

    df = BLS_CPI_CATALOG_DF.copy()

    if monthly_only and "frequency" in df.columns:
        df = df[df["frequency"] == "M"].copy()

    if seasonal:
        seasonal_code = normalize_seasonal_input(seasonal)
        seasonal_norm = normalize_text(seasonal_code)
        raw_norm = normalize_text(seasonal)
        if "seasonal" in df.columns and "seasonal_text" in df.columns:
            df = df[
                (df["seasonal"].astype(str).map(normalize_text) == seasonal_norm)
                | (df["seasonal_text"].astype(str).map(normalize_text) == raw_norm)
            ].copy()

    if area and "area_name" in df.columns:
        area_norm = normalize_text(area)
        df = df[df["area_name"].astype(str).map(normalize_text).str.contains(area_norm, na=False)].copy()

    if item and "item_name" in df.columns:
        item_norm = normalize_text(item)
        df = df[df["item_name"].astype(str).map(normalize_text).str.contains(item_norm, na=False)].copy()

    if q:
        q_norm = normalize_text(q)
        alias_sid = BLS_CPI_SERIES_ALIASES.get(q_norm)
        if alias_sid:
            alias_match = df[df["series_id"].astype(str).str.upper() == alias_sid.upper()].copy()
            if not alias_match.empty:
                return alias_match

        search_cols = [c for c in ["series_id", "series_title", "display_name_pt", "item_name", "area_name", "seasonal_text"] if c in df.columns]
        blob = df[search_cols].fillna("").astype(str).agg(" | ".join, axis=1).map(normalize_text)
        exact_sid = df[df["series_id"].astype(str).str.upper() == str(q).strip().upper()].copy()
        if not exact_sid.empty:
            return exact_sid

        exact = df[blob == q_norm].copy()
        if not exact.empty:
            return exact

        df = df[blob.str.contains(q_norm, na=False)].copy()

    return df


def resolve_bls_cpi_series_id(series_id=None, q=None, area=None, item=None, seasonal=None, monthly_only=True):
    if series_id:
        df = search_bls_cpi_catalog(q=series_id, area=area, item=item, seasonal=seasonal, monthly_only=monthly_only)
        if not df.empty:
            sid = str(df.iloc[0]["series_id"])
            return sid, df.iloc[0].to_dict(), df
        return None, None, df

    if q:
        df = search_bls_cpi_catalog(q=q, area=area, item=item, seasonal=seasonal, monthly_only=monthly_only)
        if len(df) == 1:
            sid = str(df.iloc[0]["series_id"])
            return sid, df.iloc[0].to_dict(), df
        if len(df) > 1:
            exact_title = df[df["series_title"].astype(str).map(normalize_text) == normalize_text(q)] if "series_title" in df.columns else pd.DataFrame()
            if len(exact_title) == 1:
                sid = str(exact_title.iloc[0]["series_id"])
                return sid, exact_title.iloc[0].to_dict(), exact_title
        return None, None, df

    return None, None, pd.DataFrame()


def get_bls_api_cache_key(payload):
    return json.dumps(payload, sort_keys=True, ensure_ascii=False)


def fetch_bls_series_raw(series_id, startyear=None, endyear=None, include_catalog=True):
    payload = {"seriesid": [series_id]}
    if startyear is not None:
        payload["startyear"] = str(startyear)
    if endyear is not None:
        payload["endyear"] = str(endyear)
    if include_catalog:
        payload["catalog"] = True
    if BLS_API_KEY:
        payload["registrationkey"] = BLS_API_KEY

    cache_key = get_bls_api_cache_key(payload)
    cache_entry = BLS_API_CACHE.get(cache_key)
    now_ts = time.time()
    if cache_entry and (now_ts - cache_entry["ts"] <= BLS_API_CACHE_TTL_SECONDS):
        return cache_entry["data"]

    response = requests.post(BLS_PUBLIC_API_URL, json=payload, timeout=90)
    response.raise_for_status()
    data = response.json()

    if data.get("status") != "REQUEST_SUCCEEDED":
        raise RuntimeError(f"BLS API retornou status inesperado: {data.get('status')} | {data.get('message')}")

    results = data.get("Results")
    series_list = []
    if isinstance(results, dict):
        series_list = results.get("series", [])
    elif isinstance(results, list):
        for item in results:
            if isinstance(item, dict) and "series" in item:
                series_list.extend(item.get("series", []))

    target = None
    for entry in series_list:
        if str(entry.get("seriesID", "")).upper() == str(series_id).upper():
            target = entry
            break

    if target is None:
        raise RuntimeError("BLS API não retornou a série solicitada.")

    BLS_API_CACHE[cache_key] = {"ts": now_ts, "data": target}
    return target


def bls_series_payload_to_df(series_payload):
    rows = []
    for obs in series_payload.get("data", []):
        period = str(obs.get("period", "")).strip()
        if not period.startswith("M") or len(period) != 3:
            continue
        month_num = safe_int(period[1:])
        if month_num is None or month_num < 1 or month_num > 12:
            continue

        year_num = safe_int(obs.get("year"))
        if year_num is None:
            continue

        footnote_texts = []
        for ft in obs.get("footnotes", []):
            if isinstance(ft, dict) and ft.get("text"):
                footnote_texts.append(str(ft.get("text")))

        rows.append({
            "series_id": series_payload.get("seriesID"),
            "date": f"{year_num:04d}-{month_num:02d}",
            "year": str(year_num),
            "period": period,
            "period_name": obs.get("periodName"),
            "value": safe_float(obs.get("value")),
            "footnotes": " | ".join(footnote_texts) if footnote_texts else None,
        })

    if not rows:
        return pd.DataFrame(columns=["series_id", "date", "year", "period", "period_name", "value", "footnotes"])

    df = pd.DataFrame(rows)
    df["year_num"] = df["year"].map(safe_int)
    df["month_num"] = df["period"].astype(str).str[1:].map(safe_int)
    df = df.sort_values(["year_num", "month_num"]).reset_index(drop=True)
    return df


def compute_bls_calc(df, calc="index"):
    calc = (calc or "index").strip().lower()
    out = df.copy().sort_values(["year_num", "month_num"]).reset_index(drop=True)
    out["value"] = pd.to_numeric(out["value"], errors="coerce")

    if calc == "index":
        unit = "index"
    elif calc == "mom":
        out["value"] = out["value"].pct_change(1) * 100.0
        unit = "percent_mom"
    elif calc == "yoy":
        out["value"] = out["value"].pct_change(12) * 100.0
        unit = "percent_yoy"
    else:
        raise ValueError("calc inválido. Use: index, mom ou yoy.")

    out = out.dropna(subset=["value"]).reset_index(drop=True)
    return out, unit

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
        "bls_cpi_files": bls_cpi_files_status(),
        "catalog_count": int(len(combined_catalog)),
        "bls_cpi_catalog_rows": 0 if BLS_CPI_CATALOG_DF is None else int(len(BLS_CPI_CATALOG_DF)),
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
    force_refresh = request.args.get("refresh", "false").strip().lower() in {"1", "true", "yes", "y"}

    try:
        ensure_bls_cpi_loaded(force_refresh=force_refresh)
        ok = True
        error = None
    except Exception as e:
        ok = False
        error = str(e)

    return jsonify({
        "ok": ok,
        "timestamp_utc": utc_now_iso(),
        "has_bls_key": bool(BLS_API_KEY),
        "files": bls_cpi_files_status(),
        "meta_refresh_hours": BLS_CPI_META_REFRESH_HOURS,
        "catalog_rows": 0 if BLS_CPI_CATALOG_DF is None else int(len(BLS_CPI_CATALOG_DF)),
        "load_errors": {
            "bls_cpi_series": LOAD_ERRORS.get("bls_cpi_series"),
            "bls_cpi_area": LOAD_ERRORS.get("bls_cpi_area"),
            "bls_cpi_item": LOAD_ERRORS.get("bls_cpi_item"),
            "bls_cpi_period": LOAD_ERRORS.get("bls_cpi_period"),
            "bls_cpi_seasonal": LOAD_ERRORS.get("bls_cpi_seasonal"),
            "bls_cpi_footnote": LOAD_ERRORS.get("bls_cpi_footnote"),
            "bls_cpi_catalog": LOAD_ERRORS.get("bls_cpi_catalog"),
        },
        "error": error,
    }), 200 if ok else 500


@app.route("/bls/cpi/catalog", methods=["GET"])
def bls_cpi_catalog():
    ensure_bls_cpi_loaded()

    if BLS_CPI_CATALOG_DF is None or BLS_CPI_CATALOG_DF.empty:
        return jsonify({
            "ok": False,
            "error": "Catálogo BLS CPI não encontrado."
        }), 404

    q = request.args.get("q", "").strip()
    area = request.args.get("area", "").strip()
    item = request.args.get("item", "").strip()
    seasonal = request.args.get("seasonal", "").strip()
    monthly_only = request.args.get("monthly_only", "true").strip().lower() in {"1", "true", "yes", "y"}
    max_rows = safe_int(request.args.get("max_rows")) or 500

    df = search_bls_cpi_catalog(q=q or None, area=area or None, item=item or None, seasonal=seasonal or None, monthly_only=monthly_only)
    df = df.sort_values([c for c in ["item_name", "area_name", "seasonal", "series_id"] if c in df.columns]).reset_index(drop=True)

    return jsonify({
        "ok": True,
        "count": int(len(df)),
        "monthly_only": monthly_only,
        "q": q or None,
        "area": area or None,
        "item": item or None,
        "seasonal": seasonal or None,
        "catalog": df_to_records(df, max_rows=max_rows)
    })


@app.route("/bls/cpi/query", methods=["GET"])
def bls_cpi_query():
    ensure_bls_cpi_loaded()

    series_id_input = request.args.get("series_id", "").strip()
    q = request.args.get("q", "").strip()
    area = request.args.get("area", "").strip()
    item = request.args.get("item", "").strip()
    seasonal = request.args.get("seasonal", "").strip()
    calc = request.args.get("calc", "index").strip().lower()
    year_start = safe_int(request.args.get("year_start"))
    year_end = safe_int(request.args.get("year_end"))
    last_n = safe_int(request.args.get("last_n"))
    max_rows = safe_int(request.args.get("max_rows")) or 5000

    resolved_series_id, series_meta, candidates = resolve_bls_cpi_series_id(
        series_id=series_id_input or None,
        q=q or None,
        area=area or None,
        item=item or None,
        seasonal=seasonal or None,
        monthly_only=True,
    )

    if not resolved_series_id:
        suggestions = df_to_records(candidates, max_rows=20) if candidates is not None and not candidates.empty else []
        return jsonify({
            "ok": False,
            "error": "Série CPI não encontrada ou consulta ambígua. Use /bls/cpi/catalog para localizar a série exata.",
            "series_id_input": series_id_input or None,
            "q": q or None,
            "area": area or None,
            "item": item or None,
            "seasonal": seasonal or None,
            "suggestion_count": len(suggestions),
            "suggestions": suggestions,
        }), 404

    if year_end is None:
        year_end = datetime.now(timezone.utc).year
    if year_start is None:
        year_start = max(year_end - 10, 1913)

    try:
        raw_payload = fetch_bls_series_raw(
            series_id=resolved_series_id,
            startyear=year_start,
            endyear=year_end,
            include_catalog=True,
        )
    except Exception as e:
        return jsonify({
            "ok": False,
            "error": f"Falha ao consultar a BLS API: {e}",
            "series_resolved": resolved_series_id,
        }), 502

    df = bls_series_payload_to_df(raw_payload)
    if df.empty:
        return jsonify({
            "ok": False,
            "error": "Nenhuma observação mensal retornada pela BLS API para a série solicitada.",
            "series_resolved": resolved_series_id,
            "year_start": year_start,
            "year_end": year_end,
        }), 404

    try:
        result_df, unit = compute_bls_calc(df, calc=calc)
    except Exception as e:
        return jsonify({
            "ok": False,
            "error": str(e),
            "allowed_calc": ["index", "mom", "yoy"],
        }), 400

    if last_n is not None and last_n > 0:
        result_df = result_df.tail(last_n).reset_index(drop=True)

    local_meta = series_meta or {}
    api_catalog = raw_payload.get("catalog", {}) if isinstance(raw_payload.get("catalog"), dict) else {}

    response_meta = {
        "series_title": api_catalog.get("series_title") or local_meta.get("series_title") or local_meta.get("display_name_pt"),
        "area_name": local_meta.get("area_name"),
        "item_name": local_meta.get("item_name"),
        "seasonal": local_meta.get("seasonal"),
        "seasonal_text": local_meta.get("seasonal_text"),
        "base_period": api_catalog.get("base_period") or local_meta.get("base_period"),
    }

    return jsonify({
        "ok": True,
        "series_id_input": series_id_input or None,
        "series_resolved": resolved_series_id,
        "q": q or None,
        "area": area or None,
        "item": item or None,
        "seasonal": seasonal or None,
        "calc": calc,
        "unit": unit,
        "frequency": "M",
        "year_start": year_start,
        "year_end": year_end,
        "last_n": last_n,
        "series_meta": response_meta,
        "rows": int(len(result_df)),
        "data": df_to_records(result_df, max_rows=max_rows)
    })

# ============================================================
# INICIALIZAÇÃO
# ============================================================
if __name__ == "__main__":
    port = int(os.environ.get("PORT", 10000))
    app.run(host="0.0.0.0", port=port)
