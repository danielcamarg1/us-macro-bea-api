import os
import json
import unicodedata
from datetime import datetime, timezone

import pandas as pd
from flask import Flask, jsonify, request

app = Flask(__name__)

# ============================================================
# CONFIGURAÇÃO GERAL
# ============================================================
BASE_DIR = os.path.dirname(os.path.abspath(__file__))

DATA_DIR = os.path.join(BASE_DIR, "data")
SILVER_DIR = os.path.join(DATA_DIR, "silver")
META_DIR = os.path.join(DATA_DIR, "meta")
TRADE_DIR = os.path.join(DATA_DIR, "trade")

BEA_API_KEY = os.getenv("BEA_API_KEY", "").strip()

# --------------------------
# Arquivos BEA já existentes
# --------------------------
BEA_CORE_CSV = os.path.join(SILVER_DIR, "us_macro_bea_v1.csv")
BEA_CORE_CATALOG_CSV = os.path.join(META_DIR, "series_catalog_v1.csv")

BEA_INDUSTRY_CSV = os.path.join(SILVER_DIR, "us_macro_bea_industry_v2.csv")
BEA_INDUSTRY_CATALOG_CSV = os.path.join(META_DIR, "industry_series_catalog_v2.csv")

# --------------------------
# Arquivos Trade (V3.3)
# --------------------------
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
# CONSTANTES
# ============================================================
CATALOG_OUTPUT_COLUMNS = [
    "dataset",
    "display_name_pt",
    "frequency",
    "series_name",
    "subcategory",
    "theme",
    "unit",
    "scope",
    "api_endpoint",
    "available_partner_types",
]

INDUSTRY_ALIAS_TO_CODE = {
    "construcao": "23",
    "construction": "23",
    "manufatura": "31G",
    "manufacturing": "31G",
    "servicos privados": "PSERV",
    "private services": "PSERV",
    "services": "PSERV",
    "servicos": "PSERV",
    "financas e seguros": "52",
    "finance and insurance": "52",
    "finance": "52",
    "financas": "52",
    "agropecuaria": "11",
    "agriculture": "11",
    "informacao": "51",
    "information": "51",
    "utilities": "22",
    "utilidades": "22",
    "mineracao": "21",
    "mining": "21",
    "transporte e armazenagem": "48TW",
    "transportation and warehousing": "48TW",
}

# ============================================================
# FUNÇÕES AUXILIARES
# ============================================================
def utc_now_iso():
    return datetime.now(timezone.utc).isoformat()


def normalize_text(value: str) -> str:
    if value is None:
        return ""

    text = str(value).strip().lower()
    text = unicodedata.normalize("NFKD", text)
    text = "".join(ch for ch in text if not unicodedata.combining(ch))
    text = " ".join(text.split())
    return text


def safe_int(value):
    try:
        if value is None:
            return None
        value = str(value).strip()
        if value == "":
            return None
        return int(value)
    except Exception:
        return None


def safe_float(value):
    try:
        if value is None:
            return None
        if str(value).strip() == "":
            return None
        return float(value)
    except Exception:
        return None


def file_exists(path: str) -> bool:
    return os.path.exists(path)


def read_csv_if_exists(path: str):
    if os.path.exists(path):
        return pd.read_csv(path)
    return None


def json_load_if_exists(path: str):
    if os.path.exists(path):
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f)
    return None


def limit_param(default_value=5000, hard_cap=50000):
    value = safe_int(request.args.get("limit"))
    if value is None:
        return default_value
    return max(1, min(value, hard_cap))


def df_to_records(df, max_rows=5000):
    if df is None or df.empty:
        return []

    out = df.head(max_rows).copy()
    out = out.where(pd.notnull(out), None)
    return out.to_dict(orient="records")


def apply_string_date_filter(df, date_col="date", start_date=None, end_date=None):
    if df is None or df.empty:
        return df

    out = df.copy()
    out[date_col] = out[date_col].astype(str)

    if start_date:
        out = out[out[date_col] >= str(start_date)]

    if end_date:
        out = out[out[date_col] <= str(end_date)]

    return out


def apply_year_filter(df, year_start=None, year_end=None):
    if df is None or df.empty:
        return df

    out = df.copy()
    out["_year_num"] = out["date"].astype(str).str[:4].map(safe_int)

    if year_start is not None:
        out = out[out["_year_num"] >= year_start]

    if year_end is not None:
        out = out[out["_year_num"] <= year_end]

    return out.drop(columns=["_year_num"], errors="ignore")


def apply_frequency_filter(df, frequency=None):
    if df is None or df.empty or not frequency:
        return df
    return df[df["frequency"].astype(str).str.upper() == str(frequency).upper()].copy()


def standardize_catalog_df(df, default_dataset, default_scope, default_api_endpoint, default_partner_types=None):
    if df is None or df.empty:
        return pd.DataFrame(columns=CATALOG_OUTPUT_COLUMNS)

    out = df.copy()

    if "dataset" not in out.columns:
        out["dataset"] = default_dataset
    if "scope" not in out.columns:
        out["scope"] = default_scope
    if "api_endpoint" not in out.columns:
        out["api_endpoint"] = default_api_endpoint
    if "available_partner_types" not in out.columns:
        out["available_partner_types"] = default_partner_types

    for col in CATALOG_OUTPUT_COLUMNS:
        if col not in out.columns:
            out[col] = None

    out = out[CATALOG_OUTPUT_COLUMNS].copy()
    out = out.where(pd.notnull(out), None)
    return out


# ============================================================
# LOADERS
# ============================================================
def ensure_bea_loaded():
    global BEA_CORE_DF
    global BEA_CORE_CATALOG_DF
    global BEA_INDUSTRY_DF
    global BEA_INDUSTRY_CATALOG_DF

    if BEA_CORE_DF is None:
        BEA_CORE_DF = read_csv_if_exists(BEA_CORE_CSV)

    if BEA_CORE_CATALOG_DF is None:
        BEA_CORE_CATALOG_DF = read_csv_if_exists(BEA_CORE_CATALOG_CSV)

    if BEA_INDUSTRY_DF is None:
        BEA_INDUSTRY_DF = read_csv_if_exists(BEA_INDUSTRY_CSV)

    if BEA_INDUSTRY_CATALOG_DF is None:
        BEA_INDUSTRY_CATALOG_DF = read_csv_if_exists(BEA_INDUSTRY_CATALOG_CSV)


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

    if TRADE_COUNTRIES_DF is None:
        TRADE_COUNTRIES_DF = read_csv_if_exists(TRADE_COUNTRIES_CSV)

    if TRADE_GROUPS_DF is None:
        TRADE_GROUPS_DF = read_csv_if_exists(TRADE_GROUPS_CSV)

    if TRADE_TOTAL_DF is None:
        TRADE_TOTAL_DF = read_csv_if_exists(TRADE_TOTAL_CSV)

    if TRADE_PARTNER_MASTER_DF is None:
        TRADE_PARTNER_MASTER_DF = read_csv_if_exists(TRADE_PARTNER_MASTER_CSV)

    if TRADE_CATALOG_DF is None:
        TRADE_CATALOG_DF = read_csv_if_exists(TRADE_CATALOG_CSV)

    if TRADE_ALIASES_DF is None:
        TRADE_ALIASES_DF = read_csv_if_exists(TRADE_ALIASES_CSV)

    if TRADE_COUNTRY_LIST_DF is None:
        TRADE_COUNTRY_LIST_DF = read_csv_if_exists(TRADE_COUNTRY_LIST_CSV)

    if TRADE_GROUP_LIST_DF is None:
        TRADE_GROUP_LIST_DF = read_csv_if_exists(TRADE_GROUP_LIST_CSV)

    if TRADE_SUMMARY is None:
        TRADE_SUMMARY = json_load_if_exists(TRADE_SUMMARY_JSON)


def bea_files_status():
    return {
        "bea_core_csv": file_exists(BEA_CORE_CSV),
        "bea_core_catalog_csv": file_exists(BEA_CORE_CATALOG_CSV),
        "bea_industry_csv": file_exists(BEA_INDUSTRY_CSV),
        "bea_industry_catalog_csv": file_exists(BEA_INDUSTRY_CATALOG_CSV),
    }


def trade_files_status():
    return {
        "countries_csv": file_exists(TRADE_COUNTRIES_CSV),
        "groups_csv": file_exists(TRADE_GROUPS_CSV),
        "total_csv": file_exists(TRADE_TOTAL_CSV),
        "partner_master_csv": file_exists(TRADE_PARTNER_MASTER_CSV),
        "catalog_csv": file_exists(TRADE_CATALOG_CSV),
        "aliases_csv": file_exists(TRADE_ALIASES_CSV),
        "country_list_csv": file_exists(TRADE_COUNTRY_LIST_CSV),
        "group_list_csv": file_exists(TRADE_GROUP_LIST_CSV),
        "summary_json": file_exists(TRADE_SUMMARY_JSON),
    }


# ============================================================
# CATÁLOGOS
# ============================================================
def build_bea_core_catalog():
    ensure_bea_loaded()

    if BEA_CORE_CATALOG_DF is not None and not BEA_CORE_CATALOG_DF.empty:
        out = BEA_CORE_CATALOG_DF.copy()
        out["scope"] = "series"
        out["api_endpoint"] = "/bea/query"
        out["available_partner_types"] = None
        return standardize_catalog_df(
            out,
            default_dataset="BEA",
            default_scope="series",
            default_api_endpoint="/bea/query"
        )

    if BEA_CORE_DF is None or BEA_CORE_DF.empty:
        return pd.DataFrame(columns=CATALOG_OUTPUT_COLUMNS)

    out = (
        BEA_CORE_DF[
            ["dataset", "display_name_pt", "frequency", "series_name", "subcategory", "theme", "unit"]
        ]
        .drop_duplicates()
        .sort_values(["theme", "subcategory", "display_name_pt"])
        .reset_index(drop=True)
    )
    out["scope"] = "series"
    out["api_endpoint"] = "/bea/query"
    out["available_partner_types"] = None

    return standardize_catalog_df(
        out,
        default_dataset="BEA",
        default_scope="series",
        default_api_endpoint="/bea/query"
    )


def build_bea_industry_catalog():
    ensure_bea_loaded()

    if BEA_INDUSTRY_DF is None or BEA_INDUSTRY_DF.empty:
        return pd.DataFrame(columns=CATALOG_OUTPUT_COLUMNS)

    out = BEA_INDUSTRY_DF.copy()

    if "industry_name_pt" not in out.columns:
        out["industry_name_pt"] = out.get("industry_code", "").astype(str)

    if "metric_name_pt" not in out.columns:
        out["metric_name_pt"] = out.get("metric_code", "").astype(str)

    out["dataset"] = out.get("dataset", "GDPbyIndustry")
    out["display_name_pt"] = out["industry_name_pt"].astype(str) + " - " + out["metric_name_pt"].astype(str)
    out["series_name"] = (
        "gdp_industry::"
        + out["industry_code"].astype(str)
        + "::"
        + out["metric_code"].astype(str)
        + "::"
        + out["frequency"].astype(str)
    )
    out["subcategory"] = out["metric_type"] if "metric_type" in out.columns else "industry"
    out["theme"] = out.get("theme", "gdp_industry")
    out["scope"] = "industry_metric"
    out["api_endpoint"] = "/bea/industry/query"
    out["available_partner_types"] = None

    out = (
        out[
            ["dataset", "display_name_pt", "frequency", "series_name", "subcategory", "theme", "unit", "scope", "api_endpoint", "available_partner_types"]
        ]
        .drop_duplicates()
        .sort_values(["display_name_pt", "frequency"])
        .reset_index(drop=True)
    )

    return standardize_catalog_df(
        out,
        default_dataset="GDPbyIndustry",
        default_scope="industry_metric",
        default_api_endpoint="/bea/industry/query"
    )


def build_trade_catalog():
    ensure_trade_loaded()

    if TRADE_CATALOG_DF is None or TRADE_CATALOG_DF.empty:
        return pd.DataFrame(columns=CATALOG_OUTPUT_COLUMNS)

    out = TRADE_CATALOG_DF.copy()
    out["series_name"] = out.get("metric_code")
    out["display_name_pt"] = out.get("metric_name_pt")
    out["api_endpoint"] = "/trade/query"

    return standardize_catalog_df(
        out,
        default_dataset="US Census International Trade API",
        default_scope="country_annual",
        default_api_endpoint="/trade/query",
        default_partner_types="country, group_or_region, total"
    )


def build_combined_catalog():
    parts = [
        build_bea_core_catalog(),
        build_bea_industry_catalog(),
        build_trade_catalog(),
    ]

    valid_parts = [p for p in parts if p is not None and not p.empty]
    if not valid_parts:
        return pd.DataFrame(columns=CATALOG_OUTPUT_COLUMNS)

    out = pd.concat(valid_parts, ignore_index=True)
    out = out.drop_duplicates().reset_index(drop=True)
    return out


# ============================================================
# RESOLVERS - TRADE
# ============================================================
def resolve_partner_name(partner_input: str):
    ensure_trade_loaded()

    if not partner_input:
        return None

    partner_norm = normalize_text(partner_input)

    if TRADE_ALIASES_DF is not None and not TRADE_ALIASES_DF.empty:
        aliases = TRADE_ALIASES_DF.copy()
        aliases["alias_input_norm"] = aliases["alias_input"].astype(str).map(normalize_text)

        alias_match = aliases[aliases["alias_input_norm"] == partner_norm]
        if not alias_match.empty:
            return str(alias_match.iloc[0]["country_name_norm"])

    if TRADE_PARTNER_MASTER_DF is not None and not TRADE_PARTNER_MASTER_DF.empty:
        master = TRADE_PARTNER_MASTER_DF.copy()
        master["country_name_norm_local"] = master["country_name"].astype(str).map(normalize_text)

        exact = master[master["country_name_norm_local"] == partner_norm]
        if not exact.empty:
            return str(exact.iloc[0]["country_name"])

        partial = master[master["country_name_norm_local"].str.contains(partner_norm, na=False)]
        if not partial.empty:
            return str(partial.iloc[0]["country_name"])

    return None


def get_partner_type(partner_name: str):
    ensure_trade_loaded()

    if TRADE_PARTNER_MASTER_DF is None or TRADE_PARTNER_MASTER_DF.empty:
        return None

    match = TRADE_PARTNER_MASTER_DF[TRADE_PARTNER_MASTER_DF["country_name"] == partner_name]
    if match.empty:
        return None

    return str(match.iloc[0]["partner_type"])


def get_trade_df_by_partner_type(partner_type: str):
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
# RESOLVERS - GDP BY INDUSTRY
# ============================================================
def resolve_industry_code(industry_input=None, industry_code=None):
    ensure_bea_loaded()

    if BEA_INDUSTRY_DF is None or BEA_INDUSTRY_DF.empty:
        return None

    df = BEA_INDUSTRY_DF.copy()

    if industry_code:
        code_norm = str(industry_code).strip().upper()
        match = df[df["industry_code"].astype(str).str.upper() == code_norm]
        if not match.empty:
            row = match.iloc[0]
            return {
                "industry_code": str(row["industry_code"]),
                "industry_name_pt": row.get("industry_name_pt"),
                "industry_desc_en": row.get("industry_desc_en"),
            }

    if not industry_input:
        return None

    industry_norm = normalize_text(industry_input)

    if industry_norm in INDUSTRY_ALIAS_TO_CODE:
        alias_code = INDUSTRY_ALIAS_TO_CODE[industry_norm]
        match = df[df["industry_code"].astype(str).str.upper() == alias_code.upper()]
        if not match.empty:
            row = match.iloc[0]
            return {
                "industry_code": str(row["industry_code"]),
                "industry_name_pt": row.get("industry_name_pt"),
                "industry_desc_en": row.get("industry_desc_en"),
            }

    work = df.copy()
    work["industry_name_pt_norm"] = work["industry_name_pt"].astype(str).map(normalize_text) if "industry_name_pt" in work.columns else ""
    work["industry_desc_en_norm"] = work["industry_desc_en"].astype(str).map(normalize_text) if "industry_desc_en" in work.columns else ""
    work["industry_code_norm"] = work["industry_code"].astype(str).str.upper()

    exact = work[
        (work["industry_name_pt_norm"] == industry_norm)
        | (work["industry_desc_en_norm"] == industry_norm)
        | (work["industry_code_norm"] == str(industry_input).strip().upper())
    ]
    if not exact.empty:
        row = exact.iloc[0]
        return {
            "industry_code": str(row["industry_code"]),
            "industry_name_pt": row.get("industry_name_pt"),
            "industry_desc_en": row.get("industry_desc_en"),
        }

    partial = work[
        work["industry_name_pt_norm"].str.contains(industry_norm, na=False)
        | work["industry_desc_en_norm"].str.contains(industry_norm, na=False)
    ]
    if not partial.empty:
        row = partial.iloc[0]
        return {
            "industry_code": str(row["industry_code"]),
            "industry_name_pt": row.get("industry_name_pt"),
            "industry_desc_en": row.get("industry_desc_en"),
        }

    return None


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
            "/bea/catalog",
            "/bea/query",
            "/bea/industry/query",
            "/trade/health",
            "/trade/catalog",
            "/trade/partners",
            "/trade/query",
            "/trade/brazil",
        ]
    })


@app.route("/health", methods=["GET"])
def health():
    ensure_bea_loaded()
    ensure_trade_loaded()

    combined_catalog = build_combined_catalog()

    return jsonify({
        "ok": True,
        "timestamp_utc": utc_now_iso(),
        "has_bea_key": bool(BEA_API_KEY),
        "bea_files": bea_files_status(),
        "trade_files": trade_files_status(),
        "catalog_count": int(len(combined_catalog)),
    })


@app.route("/catalog", methods=["GET"])
def catalog():
    limit = limit_param(default_value=5000, hard_cap=20000)
    combined_catalog = build_combined_catalog()

    return jsonify({
        "ok": True,
        "count": int(len(combined_catalog)),
        "series": df_to_records(combined_catalog, max_rows=limit)
    })


# ============================================================
# ENDPOINTS BEA
# ============================================================
@app.route("/bea/catalog", methods=["GET"])
def bea_catalog():
    limit = limit_param(default_value=5000, hard_cap=20000)

    core_catalog = build_bea_core_catalog()
    industry_catalog = build_bea_industry_catalog()
    combined = pd.concat(
        [df for df in [core_catalog, industry_catalog] if df is not None and not df.empty],
        ignore_index=True
    ) if (not core_catalog.empty or not industry_catalog.empty) else pd.DataFrame(columns=CATALOG_OUTPUT_COLUMNS)

    return jsonify({
        "ok": True,
        "count": int(len(combined)),
        "core_count": int(len(core_catalog)),
        "industry_count": int(len(industry_catalog)),
        "series": df_to_records(combined, max_rows=limit)
    })


@app.route("/bea/query", methods=["GET"])
def bea_query():
    """
    Consulta da base BEA principal (V1).
    Exemplos:
    /bea/query?series_name=gdp_real
    /bea/query?theme=inflation&frequency=M
    /bea/query?series_name=core_pce_yoy&start_date=2020-01&end_date=2025-12
    /bea/query?series_name=gdp_real&latest=true
    """
    ensure_bea_loaded()

    if BEA_CORE_DF is None or BEA_CORE_DF.empty:
        return jsonify({
            "ok": False,
            "error": "Base BEA principal não encontrada."
        }), 404

    df = BEA_CORE_DF.copy()

    series_name = request.args.get("series_name", "").strip()
    theme = request.args.get("theme", "").strip()
    subcategory = request.args.get("subcategory", "").strip()
    frequency = request.args.get("frequency", "").strip()
    q = request.args.get("q", "").strip()
    start_date = request.args.get("start_date", "").strip()
    end_date = request.args.get("end_date", "").strip()
    latest = request.args.get("latest", "false").strip().lower() in {"1", "true", "yes", "y"}
    limit = limit_param(default_value=5000, hard_cap=50000)

    if series_name:
        df = df[df["series_name"].astype(str) == series_name].copy()

    if theme:
        df = df[df["theme"].astype(str) == theme].copy()

    if subcategory:
        df = df[df["subcategory"].astype(str) == subcategory].copy()

    if frequency:
        df = apply_frequency_filter(df, frequency)

    if q:
        q_norm = normalize_text(q)
        df = df[
            df["display_name_pt"].astype(str).map(normalize_text).str.contains(q_norm, na=False)
            | df["series_name"].astype(str).map(normalize_text).str.contains(q_norm, na=False)
        ].copy()

    df = apply_string_date_filter(df, date_col="date", start_date=start_date or None, end_date=end_date or None)

    if df.empty:
        return jsonify({
            "ok": False,
            "error": "Nenhum dado encontrado para os filtros informados."
        }), 404

    if latest:
        df = (
            df.sort_values(["series_name", "date"])
            .groupby("series_name", as_index=False)
            .tail(1)
            .reset_index(drop=True)
        )
    else:
        df = df.sort_values(["series_name", "date"]).reset_index(drop=True)

    return jsonify({
        "ok": True,
        "rows": int(len(df)),
        "filters": {
            "series_name": series_name or None,
            "theme": theme or None,
            "subcategory": subcategory or None,
            "frequency": frequency or None,
            "q": q or None,
            "start_date": start_date or None,
            "end_date": end_date or None,
            "latest": latest,
        },
        "data": df_to_records(df, max_rows=limit)
    })


@app.route("/bea/industry/query", methods=["GET"])
def bea_industry_query():
    """
    Consulta da base setorial GDPbyIndustry (V2).
    Exemplos:
    /bea/industry/query?industry=construction&metric_code=value_added_real&frequency=Q
    /bea/industry/query?industry=construcao&metric_code=value_added_nominal&year_start=2015&year_end=2025
    /bea/industry/query?industry_code=23&frequency=Q&pivot=true
    """
    ensure_bea_loaded()

    if BEA_INDUSTRY_DF is None or BEA_INDUSTRY_DF.empty:
        return jsonify({
            "ok": False,
            "error": "Base setorial GDPbyIndustry não encontrada."
        }), 404

    df = BEA_INDUSTRY_DF.copy()

    industry = request.args.get("industry", "").strip()
    industry_code = request.args.get("industry_code", "").strip()
    metric_code = request.args.get("metric_code", "").strip()
    frequency = request.args.get("frequency", "").strip()
    year_start = safe_int(request.args.get("year_start"))
    year_end = safe_int(request.args.get("year_end"))
    latest = request.args.get("latest", "false").strip().lower() in {"1", "true", "yes", "y"}
    pivot = request.args.get("pivot", "false").strip().lower() in {"1", "true", "yes", "y"}
    limit = limit_param(default_value=5000, hard_cap=50000)

    resolved = None
    if industry or industry_code:
        resolved = resolve_industry_code(industry_input=industry or None, industry_code=industry_code or None)
        if not resolved:
            return jsonify({
                "ok": False,
                "error": "Indústria não encontrada.",
                "industry_input": industry or None,
                "industry_code_input": industry_code or None
            }), 404

        df = df[df["industry_code"].astype(str) == str(resolved["industry_code"])].copy()

    if metric_code:
        df = df[df["metric_code"].astype(str) == metric_code].copy()

    if frequency:
        df = apply_frequency_filter(df, frequency)

    df = apply_year_filter(df, year_start=year_start, year_end=year_end)

    if df.empty:
        return jsonify({
            "ok": False,
            "error": "Nenhum dado setorial encontrado para os filtros informados."
        }), 404

    if latest:
        group_cols = ["industry_code", "metric_code", "frequency"]
        df = (
            df.sort_values(group_cols + ["date"])
            .groupby(group_cols, as_index=False)
            .tail(1)
            .reset_index(drop=True)
        )
    else:
        df = df.sort_values(["industry_code", "metric_code", "frequency", "date"]).reset_index(drop=True)

    if pivot and resolved is not None:
        pivot_df = (
            df.pivot_table(
                index=["date", "industry_code", "industry_name_pt"],
                columns="metric_code",
                values="value",
                aggfunc="first"
            )
            .reset_index()
            .sort_values("date")
        )
        pivot_df.columns.name = None
        result_df = pivot_df
    else:
        result_df = df

    return jsonify({
        "ok": True,
        "industry_input": industry or None,
        "industry_code_input": industry_code or None,
        "industry_resolved": resolved,
        "metric_code": metric_code or None,
        "frequency": frequency or None,
        "year_start": year_start,
        "year_end": year_end,
        "latest": latest,
        "pivot": pivot,
        "rows": int(len(result_df)),
        "data": df_to_records(result_df, max_rows=limit)
    })


# ============================================================
# ENDPOINTS DE TRADE
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

    limit = limit_param(default_value=1000, hard_cap=10000)

    return jsonify({
        "ok": True,
        "count": int(len(TRADE_CATALOG_DF)),
        "catalog": df_to_records(TRADE_CATALOG_DF, max_rows=limit)
    })


@app.route("/trade/partners", methods=["GET"])
def trade_partners():
    ensure_trade_loaded()

    partner_type = request.args.get("partner_type", "").strip().lower()
    q = request.args.get("q", "").strip()
    limit = limit_param(default_value=2000, hard_cap=10000)

    if TRADE_PARTNER_MASTER_DF is None or TRADE_PARTNER_MASTER_DF.empty:
        return jsonify({
            "ok": False,
            "error": "Tabela mestre de parceiros não encontrada."
        }), 404

    df = TRADE_PARTNER_MASTER_DF.copy()

    if partner_type:
        df = df[df["partner_type"] == partner_type].copy()

    if q:
        q_norm = normalize_text(q)
        df = df[df["country_name"].astype(str).map(normalize_text).str.contains(q_norm, na=False)].copy()

    df = df.sort_values(["partner_type", "country_name"]).reset_index(drop=True)

    return jsonify({
        "ok": True,
        "count": int(len(df)),
        "partners": df_to_records(df, max_rows=limit)
    })


@app.route("/trade/query", methods=["GET"])
def trade_query():
    """
    Endpoint principal de comércio.
    Exemplos:
    /trade/query?partner=brazil&year_start=2015&year_end=2025
    /trade/query?partner=brasil&metric=trade_balance&year_start=2015&year_end=2025
    /trade/query?partner=oecd&year_start=2020&year_end=2025
    /trade/query?partner_type=total&year_start=2024&year_end=2025
    """
    ensure_trade_loaded()

    partner = request.args.get("partner", "").strip()
    partner_type = request.args.get("partner_type", "").strip().lower()
    metric = request.args.get("metric", "").strip().lower()
    year_start = safe_int(request.args.get("year_start"))
    year_end = safe_int(request.args.get("year_end"))
    pivot = request.args.get("pivot", "true").strip().lower() in {"1", "true", "yes", "y"}
    limit = limit_param(default_value=5000, hard_cap=50000)

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
        "data": df_to_records(result_df, max_rows=limit)
    })


@app.route("/trade/brazil", methods=["GET"])
def trade_brazil():
    ensure_trade_loaded()

    if TRADE_COUNTRIES_DF is None or TRADE_COUNTRIES_DF.empty:
        return jsonify({
            "ok": False,
            "error": "Base de países não encontrada."
        }), 404

    year_start = safe_int(request.args.get("year_start", 2015))
    year_end = safe_int(request.args.get("year_end", 2025))
    limit = limit_param(default_value=500, hard_cap=5000)

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
        "data": df_to_records(result_df, max_rows=limit)
    })


# ============================================================
# INICIALIZAÇÃO
# ============================================================
ensure_bea_loaded()
ensure_trade_loaded()

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 10000))
    app.run(host="0.0.0.0", port=port)
