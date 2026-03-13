import os
import re
from datetime import datetime, timezone

import requests
from flask import Flask, jsonify, request

app = Flask(__name__)

BEA_API_KEY = os.getenv("BEA_API_KEY", "").strip()
BEA_BASE_URL = "https://apps.bea.gov/api/data"
REQUEST_TIMEOUT = 60

# =========================================================
# CATÁLOGO INICIAL DE SÉRIES
# MVP: headline macro + alguns setores-chave
# =========================================================
SERIES_CATALOG = {
    # ---------------------------
    # NIPA - PIB e componentes
    # ---------------------------
    "gdp_nominal": {
        "dataset": "NIPA",
        "table_name": "T10105",
        "frequency": "Q",
        "line_number": "1",
        "display_name_pt": "PIB nominal",
        "unit": "billions_usd_saar",
        "theme": "gdp",
        "subcategory": "headline",
    },
    "gdp_real": {
        "dataset": "NIPA",
        "table_name": "T10106",
        "frequency": "Q",
        "line_number": "1",
        "display_name_pt": "PIB real",
        "unit": "billions_chained_2017_usd_saar",
        "theme": "gdp",
        "subcategory": "headline",
    },
    "gdp_price_change": {
        "dataset": "NIPA",
        "table_name": "T10107",
        "frequency": "Q",
        "line_number": "1",
        "display_name_pt": "Inflação do PIB (variação trimestral anualizada)",
        "unit": "percent",
        "theme": "gdp",
        "subcategory": "price",
    },
    "consumption_q": {
        "dataset": "NIPA",
        "table_name": "T10105",
        "frequency": "Q",
        "line_number": "2",
        "display_name_pt": "Consumo pessoal",
        "unit": "billions_usd_saar",
        "theme": "gdp",
        "subcategory": "expenditure",
    },
    "private_investment_q": {
        "dataset": "NIPA",
        "table_name": "T10105",
        "frequency": "Q",
        "line_number": "7",
        "display_name_pt": "Investimento privado bruto",
        "unit": "billions_usd_saar",
        "theme": "gdp",
        "subcategory": "expenditure",
    },
    "net_exports_q": {
        "dataset": "NIPA",
        "table_name": "T10105",
        "frequency": "Q",
        "line_number": "15",
        "display_name_pt": "Exportações líquidas",
        "unit": "billions_usd_saar",
        "theme": "trade",
        "subcategory": "headline_quarterly",
    },
    "exports_total_q": {
        "dataset": "NIPA",
        "table_name": "T10105",
        "frequency": "Q",
        "line_number": "16",
        "display_name_pt": "Exportações totais",
        "unit": "billions_usd_saar",
        "theme": "trade",
        "subcategory": "headline_quarterly",
    },
    "imports_total_q": {
        "dataset": "NIPA",
        "table_name": "T10105",
        "frequency": "Q",
        "line_number": "19",
        "display_name_pt": "Importações totais",
        "unit": "billions_usd_saar",
        "theme": "trade",
        "subcategory": "headline_quarterly",
    },

    # ---------------------------
    # NIPA - inflação PCE
    # ---------------------------
    "pce_index": {
        "dataset": "NIPA",
        "table_name": "T20804",
        "frequency": "M",
        "line_number": "1",
        "display_name_pt": "Índice PCE",
        "unit": "index_2017_100",
        "theme": "inflation",
        "subcategory": "headline",
    },
    "pce_mom": {
        "dataset": "NIPA",
        "table_name": "T20807",
        "frequency": "M",
        "line_number": "1",
        "display_name_pt": "PCE mensal",
        "unit": "percent",
        "theme": "inflation",
        "subcategory": "headline",
    },
    "core_pce_index": {
        "dataset": "NIPA",
        "table_name": "T20804",
        "frequency": "M",
        "line_number": "25",
        "display_name_pt": "Índice Core PCE",
        "unit": "index_2017_100",
        "theme": "inflation",
        "subcategory": "core",
    },
    "core_pce_mom": {
        "dataset": "NIPA",
        "table_name": "T20807",
        "frequency": "M",
        "line_number": "25",
        "display_name_pt": "Core PCE mensal",
        "unit": "percent",
        "theme": "inflation",
        "subcategory": "core",
    },
    "gas_energy_goods_pce_index": {
        "dataset": "NIPA",
        "table_name": "T20804",
        "frequency": "M",
        "line_number": "11",
        "display_name_pt": "Índice de gasolina e bens energéticos",
        "unit": "index_2017_100",
        "theme": "inflation",
        "subcategory": "energy",
    },
    "gas_energy_goods_pce_mom": {
        "dataset": "NIPA",
        "table_name": "T20807",
        "frequency": "M",
        "line_number": "11",
        "display_name_pt": "Inflação mensal de gasolina e bens energéticos",
        "unit": "percent",
        "theme": "inflation",
        "subcategory": "energy",
    },
    "services_pce_index": {
        "dataset": "NIPA",
        "table_name": "T20804",
        "frequency": "M",
        "line_number": "13",
        "display_name_pt": "Índice PCE de serviços",
        "unit": "index_2017_100",
        "theme": "inflation",
        "subcategory": "sector",
    },
    "services_pce_mom": {
        "dataset": "NIPA",
        "table_name": "T20807",
        "frequency": "M",
        "line_number": "13",
        "display_name_pt": "Inflação mensal de serviços",
        "unit": "percent",
        "theme": "inflation",
        "subcategory": "sector",
    },
    "housing_utils_pce_index": {
        "dataset": "NIPA",
        "table_name": "T20804",
        "frequency": "M",
        "line_number": "15",
        "display_name_pt": "Índice PCE de habitação e utilities",
        "unit": "index_2017_100",
        "theme": "inflation",
        "subcategory": "sector",
    },
    "housing_utils_pce_mom": {
        "dataset": "NIPA",
        "table_name": "T20807",
        "frequency": "M",
        "line_number": "15",
        "display_name_pt": "Inflação mensal de habitação e utilities",
        "unit": "percent",
        "theme": "inflation",
        "subcategory": "sector",
    },

    # ---------------------------
    # GDPbyIndustry - setores
    # ---------------------------
    "construction_value_added_nominal_q": {
        "dataset": "GDPbyIndustry",
        "table_id": "1",
        "frequency": "Q",
        "industry": "23",
        "display_name_pt": "Construção - valor adicionado nominal",
        "unit": "billions_usd",
        "theme": "gdp_industry",
        "subcategory": "construction",
    },
    "construction_value_added_real_q": {
        "dataset": "GDPbyIndustry",
        "table_id": "10",
        "frequency": "Q",
        "industry": "23",
        "display_name_pt": "Construção - valor adicionado real",
        "unit": "billions_chained_2017_usd",
        "theme": "gdp_industry",
        "subcategory": "construction",
    },
    "construction_value_added_price_index_q": {
        "dataset": "GDPbyIndustry",
        "table_id": "11",
        "frequency": "Q",
        "industry": "23",
        "display_name_pt": "Construção - índice de preços",
        "unit": "index",
        "theme": "gdp_industry",
        "subcategory": "construction",
    },
    "services_producing_value_added_real_q": {
        "dataset": "GDPbyIndustry",
        "table_id": "10",
        "frequency": "Q",
        "industry": "PSERV",
        "display_name_pt": "Serviços privados - valor adicionado real",
        "unit": "billions_chained_2017_usd",
        "theme": "gdp_industry",
        "subcategory": "services",
    },
    "manufacturing_value_added_real_q": {
        "dataset": "GDPbyIndustry",
        "table_id": "10",
        "frequency": "Q",
        "industry": "31G",
        "display_name_pt": "Manufatura - valor adicionado real",
        "unit": "billions_chained_2017_usd",
        "theme": "gdp_industry",
        "subcategory": "manufacturing",
    },
    "finance_insurance_value_added_real_q": {
        "dataset": "GDPbyIndustry",
        "table_id": "10",
        "frequency": "Q",
        "industry": "52",
        "display_name_pt": "Finanças e seguros - valor adicionado real",
        "unit": "billions_chained_2017_usd",
        "theme": "gdp_industry",
        "subcategory": "finance",
    },
}

# =========================================================
# UTILITÁRIOS
# =========================================================
def utc_now_iso():
    return datetime.now(timezone.utc).isoformat()

def ensure_key():
    if not BEA_API_KEY:
        raise RuntimeError("BEA_API_KEY não configurada nas variáveis de ambiente.")

def bea_request(params: dict):
    ensure_key()
    final_params = {
        "UserID": BEA_API_KEY,
        "ResultFormat": "JSON",
        **params
    }
    resp = requests.get(BEA_BASE_URL, params=final_params, timeout=REQUEST_TIMEOUT)
    resp.raise_for_status()
    return resp.json()

def to_float(x):
    if x is None:
        return None
    if isinstance(x, (int, float)):
        return float(x)
    s = str(x).strip().replace(",", "")
    if s in {"", "NA", "(NA)", "null", "None"}:
        return None
    try:
        return float(s)
    except Exception:
        return None

def parse_nipa_timeperiod(tp: str):
    tp = str(tp).strip()
    if re.fullmatch(r"\d{4}", tp):
        return tp, "A"
    if re.fullmatch(r"\d{4}Q[1-4]", tp):
        return f"{tp[:4]}-{tp[4:]}", "Q"
    if re.fullmatch(r"\d{4}M\d{2}", tp):
        return f"{tp[:4]}-{tp[5:]}", "M"
    return tp, None

def normalize_quarter(q):
    if q is None:
        return None
    s = str(q).strip().upper()
    mapping = {
        "I": "Q1", "II": "Q2", "III": "Q3", "IV": "Q4",
        "1": "Q1", "2": "Q2", "3": "Q3", "4": "Q4",
        "Q1": "Q1", "Q2": "Q2", "Q3": "Q3", "Q4": "Q4",
    }
    return mapping.get(s)

def pick_first(row: dict, candidates):
    for c in candidates:
        if c in row and row[c] not in [None, ""]:
            return row[c]
    return None

def filter_dates(rows, start=None, end=None):
    if not start and not end:
        return rows

    out = []
    for r in rows:
        d = r.get("date")
        if not d:
            continue
        if start and d < start:
            continue
        if end and d > end:
            continue
        out.append(r)
    return out

# =========================================================
# EXTRAÇÃO NIPA
# =========================================================
def fetch_nipa_series(spec, start=None, end=None):
    payload = bea_request({
        "method": "GetData",
        "DataSetName": "NIPA",
        "TableName": spec["table_name"],
        "Frequency": spec["frequency"],
        "Year": "ALL",
    })

    results = payload.get("BEAAPI", {}).get("Results", {})
    data = results.get("Data", []) if isinstance(results, dict) else []

    rows = []
    for row in data:
        if str(row.get("LineNumber")) != str(spec["line_number"]):
            continue

        date_norm, detected_freq = parse_nipa_timeperiod(row.get("TimePeriod", ""))
        value = to_float(row.get("DataValue"))

        if value is None:
            continue

        rows.append({
            "date": date_norm,
            "frequency": detected_freq or spec["frequency"],
            "value": value,
            "series_code": row.get("SeriesCode"),
            "line_number": row.get("LineNumber"),
            "line_description": row.get("LineDescription"),
            "metric_name": row.get("METRIC_NAME"),
            "cl_unit": row.get("CL_UNIT"),
            "unit_mult": row.get("UNIT_MULT"),
        })

    rows = filter_dates(rows, start=start, end=end)
    rows.sort(key=lambda x: x["date"])
    return rows

# =========================================================
# EXTRAÇÃO GDPbyIndustry
# =========================================================
def unwrap_results(payload: dict):
    beaapi = payload.get("BEAAPI", {})
    results = beaapi.get("Results", {})
    if isinstance(results, dict):
        return results.get("Data", [])
    if isinstance(results, list):
        out = []
        for item in results:
            if isinstance(item, dict):
                if "DataValue" in item:
                    out.append(item)
                elif "Data" in item and isinstance(item["Data"], list):
                    out.extend(item["Data"])
        return out
    return []

def parse_gdp_by_industry_row_date(row: dict, requested_freq: str):
    year_val = pick_first(row, ["Year", "year"])
    quarter_val = pick_first(row, ["Quarter", "quarter"])
    time_period = pick_first(row, ["TimePeriod", "timePeriod", "Time", "time"])

    if requested_freq == "Q":
        qnorm = normalize_quarter(quarter_val)
        if year_val not in [None, ""] and qnorm is not None:
            return f"{str(year_val).strip()}-{qnorm}", "Q"

        if time_period:
            tp = str(time_period).strip().upper()
            if re.fullmatch(r"\d{4}Q[1-4]", tp):
                return f"{tp[:4]}-{tp[4:]}", "Q"
            if re.fullmatch(r"\d{4}-Q[1-4]", tp):
                return tp, "Q"

    if requested_freq == "A":
        if year_val not in [None, ""]:
            return str(year_val).strip(), "A"

    if time_period:
        tp = str(time_period).strip().upper()
        if re.fullmatch(r"\d{4}", tp):
            return tp, "A"
        if re.fullmatch(r"\d{4}Q[1-4]", tp):
            return f"{tp[:4]}-{tp[4:]}", "Q"

    return None, None

def fetch_gdpbyindustry_series(spec, start=None, end=None):
    payload = bea_request({
        "method": "GetData",
        "DataSetName": "GDPbyIndustry",
        "TableID": spec["table_id"],
        "Frequency": spec["frequency"],
        "Year": "ALL",
        "Industry": spec["industry"],
    })

    data = unwrap_results(payload)

    rows = []
    for row in data:
        industry = str(pick_first(row, ["Industry", "industry"]) or "").strip()
        if industry != spec["industry"]:
            continue

        date_norm, detected_freq = parse_gdp_by_industry_row_date(row, spec["frequency"])
        value = to_float(pick_first(row, ["DataValue", "dataValue", "Value"]))
        if not date_norm or value is None:
            continue

        rows.append({
            "date": date_norm,
            "frequency": detected_freq or spec["frequency"],
            "value": value,
            "industry_code": industry,
            "industry_desc_en": pick_first(row, ["IndustryDescription", "Description", "Desc"]),
            "table_id": spec["table_id"],
        })

    rows = filter_dates(rows, start=start, end=end)
    rows = list({(r["date"], r["frequency"], r["value"]): r for r in rows}.values())
    rows.sort(key=lambda x: x["date"])
    return rows

# =========================================================
# DERIVADOS YOY
# =========================================================
def add_yoy(rows):
    """
    Para séries mensais: compara com 12 meses antes.
    Para séries trimestrais: compara com 4 trimestres antes.
    """
    if not rows:
        return rows

    freq = rows[0].get("frequency")
    lag = 12 if freq == "M" else 4 if freq == "Q" else None
    if lag is None:
        return rows

    out = []
    for i, row in enumerate(rows):
        row_copy = dict(row)
        if i >= lag:
            prev = rows[i - lag]["value"]
            cur = row["value"]
            if prev not in [None, 0]:
                row_copy["yoy"] = ((cur / prev) - 1.0) * 100.0
            else:
                row_copy["yoy"] = None
        else:
            row_copy["yoy"] = None
        out.append(row_copy)
    return out

# =========================================================
# ROTAS
# =========================================================
@app.get("/")
def root():
    return jsonify({
        "ok": True,
        "service": "us-macro-bea-api",
        "timestamp_utc": utc_now_iso()
    })

@app.get("/health")
def health():
    return jsonify({
        "ok": True,
        "has_bea_key": bool(BEA_API_KEY),
        "timestamp_utc": utc_now_iso()
    })

@app.get("/catalog")
def catalog():
    rows = []
    for name, spec in SERIES_CATALOG.items():
        rows.append({
            "series_name": name,
            "display_name_pt": spec["display_name_pt"],
            "dataset": spec["dataset"],
            "frequency": spec["frequency"],
            "theme": spec["theme"],
            "subcategory": spec["subcategory"],
            "unit": spec["unit"],
        })
    rows = sorted(rows, key=lambda x: x["series_name"])
    return jsonify({
        "ok": True,
        "count": len(rows),
        "series": rows
    })

@app.get("/bea/series")
def bea_series():
    series_name = request.args.get("series_name", "").strip()
    start = request.args.get("start", "").strip() or None
    end = request.args.get("end", "").strip() or None
    include_yoy = request.args.get("include_yoy", "false").strip().lower() == "true"

    if not series_name:
        return jsonify({"ok": False, "error": "Parâmetro obrigatório: series_name"}), 400

    spec = SERIES_CATALOG.get(series_name)
    if not spec:
        return jsonify({"ok": False, "error": f"Série não encontrada: {series_name}"}), 404

    try:
        if spec["dataset"] == "NIPA":
            rows = fetch_nipa_series(spec, start=start, end=end)
        elif spec["dataset"] == "GDPbyIndustry":
            rows = fetch_gdpbyindustry_series(spec, start=start, end=end)
        else:
            return jsonify({"ok": False, "error": "Dataset não suportado nesta versão"}), 400

        if include_yoy:
            rows = add_yoy(rows)

        return jsonify({
            "ok": True,
            "series_name": series_name,
            "display_name_pt": spec["display_name_pt"],
            "dataset": spec["dataset"],
            "frequency_requested": spec["frequency"],
            "theme": spec["theme"],
            "subcategory": spec["subcategory"],
            "unit": spec["unit"],
            "count": len(rows),
            "data": rows,
            "source": "BEA",
            "fetched_at_utc": utc_now_iso(),
        })

    except requests.HTTPError as e:
        return jsonify({
            "ok": False,
            "error": "Erro HTTP ao consultar a BEA",
            "details": str(e)
        }), 502
    except Exception as e:
        return jsonify({
            "ok": False,
            "error": "Erro interno",
            "details": str(e)
        }), 500

if __name__ == "__main__":
    port = int(os.getenv("PORT", "8080"))
    app.run(host="0.0.0.0", port=port)