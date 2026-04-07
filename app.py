import os
import json
from datetime import datetime, timezone

import pandas as pd
from flask import Flask, jsonify, request

app = Flask(__name__)

# ============================================================
# CONFIGURAÇÃO GERAL
# ============================================================
BASE_DIR = os.path.dirname(os.path.abspath(__file__))

DATA_DIR = os.path.join(BASE_DIR, "data")
TRADE_DIR = os.path.join(DATA_DIR, "trade")

BEA_API_KEY = os.getenv("BEA_API_KEY", "").strip()

# Arquivos de trade (V3.3)
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
# FUNÇÕES AUXILIARES
# ============================================================
def utc_now_iso():
    return datetime.now(timezone.utc).isoformat()


def normalize_text(value: str) -> str:
    if value is None:
        return ""
    return " ".join(str(value).strip().lower().split())


def safe_int(value):
    try:
        return int(str(value).strip())
    except Exception:
        return None


def safe_float(value):
    try:
        if value is None:
            return None
        return float(value)
    except Exception:
        return None


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

    if TRADE_COUNTRIES_DF is None and os.path.exists(TRADE_COUNTRIES_CSV):
        TRADE_COUNTRIES_DF = pd.read_csv(TRADE_COUNTRIES_CSV)

    if TRADE_GROUPS_DF is None and os.path.exists(TRADE_GROUPS_CSV):
        TRADE_GROUPS_DF = pd.read_csv(TRADE_GROUPS_CSV)

    if TRADE_TOTAL_DF is None and os.path.exists(TRADE_TOTAL_CSV):
        TRADE_TOTAL_DF = pd.read_csv(TRADE_TOTAL_CSV)

    if TRADE_PARTNER_MASTER_DF is None and os.path.exists(TRADE_PARTNER_MASTER_CSV):
        TRADE_PARTNER_MASTER_DF = pd.read_csv(TRADE_PARTNER_MASTER_CSV)

    if TRADE_CATALOG_DF is None and os.path.exists(TRADE_CATALOG_CSV):
        TRADE_CATALOG_DF = pd.read_csv(TRADE_CATALOG_CSV)

    if TRADE_ALIASES_DF is None and os.path.exists(TRADE_ALIASES_CSV):
        TRADE_ALIASES_DF = pd.read_csv(TRADE_ALIASES_CSV)

    if TRADE_COUNTRY_LIST_DF is None and os.path.exists(TRADE_COUNTRY_LIST_CSV):
        TRADE_COUNTRY_LIST_DF = pd.read_csv(TRADE_COUNTRY_LIST_CSV)

    if TRADE_GROUP_LIST_DF is None and os.path.exists(TRADE_GROUP_LIST_CSV):
        TRADE_GROUP_LIST_DF = pd.read_csv(TRADE_GROUP_LIST_CSV)

    if TRADE_SUMMARY is None and os.path.exists(TRADE_SUMMARY_JSON):
        with open(TRADE_SUMMARY_JSON, "r", encoding="utf-8") as f:
            TRADE_SUMMARY = json.load(f)


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


def resolve_partner_name(partner_input: str):
    """
    Resolve um nome digitado pelo usuário usando:
    1) aliases
    2) match exato no partner master
    3) contains simples no partner master
    """
    ensure_trade_loaded()

    if not partner_input:
        return None

    partner_norm = normalize_text(partner_input)

    # 1) Alias
    if TRADE_ALIASES_DF is not None and not TRADE_ALIASES_DF.empty:
        aliases = TRADE_ALIASES_DF.copy()
        aliases["alias_input_norm"] = aliases["alias_input"].astype(str).map(normalize_text)

        alias_match = aliases[aliases["alias_input_norm"] == partner_norm]
        if not alias_match.empty:
            return str(alias_match.iloc[0]["country_name_norm"])

    # 2) Match exato no master
    if TRADE_PARTNER_MASTER_DF is not None and not TRADE_PARTNER_MASTER_DF.empty:
        master = TRADE_PARTNER_MASTER_DF.copy()
        master["country_name_norm_local"] = master["country_name"].astype(str).map(normalize_text)

        exact = master[master["country_name_norm_local"] == partner_norm]
        if not exact.empty:
            return str(exact.iloc[0]["country_name"])

        # 3) Contains simples
        partial = master[master["country_name_norm_local"].str.contains(partner_norm, na=False)]
        if not partial.empty:
            return str(partial.iloc[0]["country_name"])

    return None


def get_partner_type(partner_name: str):
    ensure_trade_loaded()

    if TRADE_PARTNER_MASTER_DF is None or TRADE_PARTNER_MASTER_DF.empty:
        return None

    master = TRADE_PARTNER_MASTER_DF.copy()
    match = master[master["country_name"] == partner_name]
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


def apply_year_filter(df, year_start=None, year_end=None):
    if df is None or df.empty:
        return df

    out = df.copy()
    out["year_num"] = out["date"].map(safe_int)

    if year_start is not None:
        out = out[out["year_num"] >= year_start]

    if year_end is not None:
        out = out[out["year_num"] <= year_end]

    out = out.drop(columns=["year_num"], errors="ignore")
    return out


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


def df_to_records(df, max_rows=500):
    if df is None or df.empty:
        return []

    out = df.head(max_rows).copy()

    for col in out.columns:
        if str(out[col].dtype).startswith("float") or str(out[col].dtype).startswith("int"):
            out[col] = out[col].where(pd.notnull(out[col]), None)

    return out.to_dict(orient="records")


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
            "/trade/health",
            "/trade/catalog",
            "/trade/partners",
            "/trade/query",
            "/trade/brazil"
        ]
    })


@app.route("/health", methods=["GET"])
def health():
    ensure_trade_loaded()

    return jsonify({
        "ok": True,
        "timestamp_utc": utc_now_iso(),
        "has_bea_key": bool(BEA_API_KEY),
        "trade_files": trade_files_status()
    })


@app.route("/catalog", methods=["GET"])
def catalog():
    """
    Catálogo combinado simples:
    - mantém compatibilidade com a ideia do endpoint antigo
    - adiciona séries de trade se disponíveis
    """
    ensure_trade_loaded()

    series = []

    # Catálogo de trade
    if TRADE_CATALOG_DF is not None and not TRADE_CATALOG_DF.empty:
        for _, row in TRADE_CATALOG_DF.iterrows():
            series.append({
                "dataset": "US Census International Trade API",
                "display_name_pt": row.get("metric_name_pt"),
                "frequency": row.get("frequency"),
                "series_name": row.get("metric_code"),
                "subcategory": row.get("subcategory"),
                "theme": row.get("theme"),
                "unit": row.get("unit"),
                "scope": row.get("scope"),
                "available_partner_types": row.get("available_partner_types"),
            })

    return jsonify({
        "ok": True,
        "count": len(series),
        "series": series
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
    """
    Endpoint principal para o GPT.
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
            "metric": metric,
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
# INICIALIZAÇÃO
# ============================================================
ensure_trade_loaded()

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 10000))
    app.run(host="0.0.0.0", port=port)
