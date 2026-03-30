"""
10X Digital Economy Programme — ESO Portfolio Analysis + HTML Generator
=======================================================================
Outbox Uganda · March 2026

Reads the raw KoboToolbox EOI Excel export, extracts per-ESO statistics,
and writes a fully self-contained HTML portfolio dashboard.

Requirements:
    pip install pandas openpyxl

Usage:
    python 10x_portfolio_analysis.py

Output:
    eso_stats.json              — raw extracted numbers
    10x_eso_portfolio.html      — standalone dashboard (open in any browser)
"""

import pandas as pd
import numpy as np
import json
from pathlib import Path

# ─────────────────────────────────────────────────────────────────────────────
# CONFIG  ── change these if the file name or date changes
# ─────────────────────────────────────────────────────────────────────────────

INPUT_FILE      = "10X_Digital_Economy_Expression_of_interest_Form_-_all_versions_-_English_-_2026-03-29-05-18-21.xlsx"

# Outputs write to the same folder as this script
_HERE       = Path(__file__).parent
OUTPUT_JSON = str(_HERE / "eso_stats.json")
OUTPUT_HTML = str(_HERE / "10x_eso_portfolio.html")
REFERENCE_DATE  = pd.Timestamp("2026-01-01")

REVENUE_CAP = 5_000_000_000   # UGX — caps runaway data-entry errors
FTE_CAP     = 500
PTE_CAP     = 200

TARGET_ESOS = [
    "DFCU Foundation",
    "MUBS EIIC",
    "Mkazipreneur",
    "ExcelHort",
    "Stanbic Bank Incubator",
    "PEDN",
    "Challenges Uganda",
]

SECTOR_MAP = {
    "trade and services":                    "Trade & Services",
    "fashion and design":                    "Fashion & Design",
    "Agriculture":                           "Agriculture",
    "Light manufacturing":                   "Light Manufacturing",
    "Others":                                "Others / Events",
    "Health":                                "Health",
    "meetings, incentives and  conferences": "Meetings & Events",
}

# ESO display config: (css-var-name, gradient-start, gradient-end, region-label)
ESO_STYLE = {
    "DFCU Foundation":        ("dfcu",  "#92400e", "#b45309", "Eastern Uganda"),
    "MUBS EIIC":              ("mubs",  "#075985", "#0369a1", "Eastern Uganda"),
    "Mkazipreneur":           ("mkazi", "#6d28d9", "#7c3aed", "Central Uganda"),
    "ExcelHort":              ("excel", "#14532d", "#15803d", "Western Uganda"),
    "Stanbic Bank Incubator": ("stan",  "#1d4ed8", "#2563eb", "Central Uganda"),
    "PEDN":                   ("pedn",  "#15803d", "#16a34a", "Eastern Uganda"),
    "Challenges Uganda":      ("chal",  "#991b1b", "#dc2626", "Northern Uganda"),
}

# ─────────────────────────────────────────────────────────────────────────────
# COLUMN NAMES  (as exported by KoboToolbox)
# ─────────────────────────────────────────────────────────────────────────────

COL = {
    "ip":       "Implementing_Partner_Support_Organization",
    "sector":   "what sector is your Enterprise/Business operating?",
    "district": "In which district is your Enterprise/Business located?",
    "region":   "In which Region is your Enterprise located?",
    "revenue":  "How much revenue (in UGX) has your Business generated in the last two years (24 months)?",
    "fte":      "How many full-time staff do you have?",
    "pte":      "How many part-time employees do you have?",
    "ursb":     "Is your business/enterprise formally registered with URSB?",
    "index":    "_index",
}

FCOL = {
    "gender":      "Please select ${first_name_f}'s gender.",
    "pwd":         "Is ${first_name_f} a person with disabilities?",
    "dob":         "Please enter ${first_name_f}'s date of birth.",
    "citizenship": "What is ${first_name_f}'s citizenship?",
    "parent_idx":  "_parent_index",
}


# ─────────────────────────────────────────────────────────────────────────────
# STEP 1 — LOAD
# ─────────────────────────────────────────────────────────────────────────────

def load_data(filepath):
    print(f"\n📂  Loading: {filepath}")
    main     = pd.read_excel(filepath, sheet_name=0)
    founders = pd.read_excel(filepath, sheet_name="founders")
    main[COL["ip"]] = main[COL["ip"]].str.strip()   # strip KoboToolbox trailing spaces
    print(f"    Main sheet   : {main.shape[0]:,} rows")
    print(f"    Founders sheet: {founders.shape[0]:,} rows")
    return main, founders


# ─────────────────────────────────────────────────────────────────────────────
# STEP 2 — MERGE FOUNDERS → MAIN
# ─────────────────────────────────────────────────────────────────────────────

def merge_founders(main, founders):
    """Join founders sub-table back to main using _parent_index → _index."""
    return founders.merge(
        main[[COL["index"], COL["ip"]]],
        left_on=FCOL["parent_idx"],
        right_on=COL["index"],
        how="left"
    )


# ─────────────────────────────────────────────────────────────────────────────
# STEP 3 — PER-ESO STATS
# ─────────────────────────────────────────────────────────────────────────────

def extract_eso_stats(eso_name, main, founders_merged):
    g  = main[main[COL["ip"]] == eso_name].copy()
    gf = founders_merged[founders_merged[COL["ip"]] == eso_name].copy()
    n  = len(g)
    if n == 0:
        print(f"    ⚠  No rows found for '{eso_name}'")
        return {}

    # Employment (cap outliers)
    fte = pd.to_numeric(g[COL["fte"]], errors="coerce")
    pte = pd.to_numeric(g[COL["pte"]], errors="coerce")
    fte_total = int(fte[fte <= FTE_CAP].sum())
    pte_total = int(pte[pte <= PTE_CAP].sum())

    # Revenue
    rev       = pd.to_numeric(g[COL["revenue"]], errors="coerce")
    rev_clean = rev[rev <= REVENUE_CAP]
    avg_rev   = float(rev_clean.mean()) if rev_clean.count() > 0 else 0.0

    # Formality
    ursb = int((g[COL["ursb"]].str.lower().str.strip() == "yes").sum())

    # Gender (founders sheet)
    tf         = len(gf)
    female_cnt = int((gf[FCOL["gender"]].str.lower() == "female").sum())
    female_pct = round(female_cnt / tf * 100, 1) if tf > 0 else 0.0
    male_pct   = round(100 - female_pct, 1)

    # Age (founders sheet)
    dob        = pd.to_datetime(gf[FCOL["dob"]], errors="coerce")
    age        = (REFERENCE_DATE - dob).dt.days / 365.25
    youth_cnt  = int(((age >= 18) & (age <= 35)).sum())
    youth_pct  = round(youth_cnt / tf * 100, 1) if tf > 0 else 0.0
    age_bands  = {
        "18-25": int(((age >= 18) & (age <= 25)).sum()),
        "26-35": int(((age >= 26) & (age <= 35)).sum()),
        "36-45": int(((age >= 36) & (age <= 45)).sum()),
        "46-55": int(((age >= 46) & (age <= 55)).sum()),
        "56+":   int((age >= 56).sum()),
    }

    # PWD & Refugees (founders sheet)
    pwd      = int((gf[FCOL["pwd"]].str.lower() == "yes").sum())
    refugees = int((gf[FCOL["citizenship"]].str.lower() == "refugee").sum())

    # Sectors
    raw_sectors = g[COL["sector"]].value_counts().head(6).to_dict()
    sectors     = {SECTOR_MAP.get(k, k): int(v) for k, v in raw_sectors.items()}

    # Districts
    districts = {str(k): int(v) for k, v in g[COL["district"]].value_counts().head(7).items()}

    # Region
    region = g[COL["region"]].value_counts().idxmax() if n > 0 else "-"

    return {
        "name":       eso_name,
        "n":          n,
        "region":     region,
        "fte":        fte_total,
        "pte":        pte_total,
        "avg_rev":    round(avg_rev),
        "ursb":       ursb,
        "female_pct": female_pct,
        "male_pct":   male_pct,
        "youth_pct":  youth_pct,
        "age_bands":  age_bands,
        "pwd":        pwd,
        "refugees":   refugees,
        "sectors":    sectors,
        "districts":  districts,
    }


# ─────────────────────────────────────────────────────────────────────────────
# STEP 4 — AGGREGATES
# ─────────────────────────────────────────────────────────────────────────────

def compute_aggregates(results):
    total_n   = sum(v["n"]   for v in results.values())
    total_fte = sum(v["fte"] for v in results.values())
    total_pte = sum(v["pte"] for v in results.values())
    total_ursb   = sum(v["ursb"]     for v in results.values())
    total_pwd    = sum(v["pwd"]      for v in results.values())
    total_ref    = sum(v["refugees"] for v in results.values())
    avg_female   = round(sum(v["female_pct"] * v["n"] for v in results.values()) / total_n, 1) if total_n > 0 else 0
    avg_youth    = round(sum(v["youth_pct"]  * v["n"] for v in results.values()) / total_n, 1) if total_n > 0 else 0
    return {
        "total_n": total_n, "total_fte": total_fte, "total_pte": total_pte,
        "total_ursb": total_ursb, "total_pwd": total_pwd, "total_ref": total_ref,
        "avg_female": avg_female, "avg_youth": avg_youth,
    }


# ─────────────────────────────────────────────────────────────────────────────
# STEP 5 — PRINT CONSOLE SUMMARY
# ─────────────────────────────────────────────────────────────────────────────

def print_summary(results, agg):
    print("\n" + "=" * 95)
    print("10X DIGITAL ECONOMY — ESO PORTFOLIO SUMMARY")
    print("=" * 95)
    print(f"{'ESO':<26} {'n':>6} {'Region':<10} {'FTE':>6} {'PTE':>5} {'♀%':>7} {'Youth%':>8} {'PWD':>5} {'Ref':>5} {'URSB':>5}")
    print("-" * 95)
    for v in results.values():
        print(f"{v['name']:<26} {v['n']:>6,} {v['region']:<10} {v['fte']:>6,} {v['pte']:>5,} "
              f"{v['female_pct']:>6.1f}% {v['youth_pct']:>7.1f}% {v['pwd']:>5} "
              f"{v['refugees']:>5} {v['ursb']:>5}")
    print("-" * 95)
    print(f"{'TOTAL / WTD AVG':<26} {agg['total_n']:>6,} {'':10} {agg['total_fte']:>6,} "
          f"{agg['total_pte']:>5,} {agg['avg_female']:>6.1f}% {agg['avg_youth']:>7.1f}% "
          f"{agg['total_pwd']:>5} {agg['total_ref']:>5} {agg['total_ursb']:>5}")
    print("=" * 95)


# ─────────────────────────────────────────────────────────────────────────────
# STEP 6 — HTML HELPERS
# ─────────────────────────────────────────────────────────────────────────────

def fmt_rev(v):
    """Format UGX revenue as short string, e.g. 14,298,804 → 14.3M"""
    if v == 0:
        return "N/A"
    if v >= 1_000_000:
        return f"{v/1_000_000:.1f}M"
    if v >= 1_000:
        return f"{v/1_000:.0f}K"
    return str(v)

def fmt_n(v):
    return f"{v:,}"

def hbar(label, count, max_count, color, label_cls="hl", extra_opacity=""):
    """Render one horizontal bar row."""
    pct_display = count / max_count * 100 if max_count > 0 else 0
    pct_label   = count / max_count * 100 if max_count > 0 else 0
    total_pct   = round(count / max_count * 100, 1) if max_count > 0 else 0
    opacity     = f"opacity:.{extra_opacity};" if extra_opacity else ""
    return (
        f'<div class="hr">'
        f'<div class="{label_cls}">{label}</div>'
        f'<div class="ht"><div class="hf" style="width:{pct_display:.1f}%;background:{color};{opacity}"></div></div>'
        f'<div class="hn">{fmt_n(count)}</div>'
        f'</div>'
    )

def loc_tile(label, pct, color):
    return (
        f'<div class="lt">'
        f'<div class="ltl">{label}</div>'
        f'<div class="ltt"><div class="ltf" style="width:{pct:.1f}%;background:{color}"></div></div>'
        f'<div class="ltv">{pct:.1f}%</div>'
        f'</div>'
    )

def null_box(label="No formal collectives identified", sub="Individual / SME submissions only"):
    return (
        f'<div class="null-box"><div class="null-v">0</div>'
        f'<div>{label}</div><div>{sub}</div></div>'
    )

def smr_item(val, label, color):
    return (
        f'<div><div class="sm-v" style="color:{color}">{val}</div>'
        f'<div class="sm-l">{label}</div></div>'
    )


# ─────────────────────────────────────────────────────────────────────────────
# STEP 7 — HTML GENERATORS  (one function per section)
# ─────────────────────────────────────────────────────────────────────────────

CSS = """
*,*::before,*::after{box-sizing:border-box;margin:0;padding:0}
:root{
  --bg:#f4f5f9;--white:#fff;--border:#e3e6ef;--text:#1a1d2e;--muted:#6b7490;
  --dfcu:#b45309;--dfcu-lt:#fffbeb;--dfcu-bd:#fde68a;
  --mubs:#0369a1;--mubs-lt:#eff6ff;--mubs-bd:#bae6fd;
  --mkazi:#7c3aed;--mkazi-lt:#f5f3ff;--mkazi-bd:#ddd6fe;
  --excel:#15803d;--excel-lt:#f0fdf4;--excel-bd:#bbf7d0;
  --stan:#2563eb;--stan-lt:#eff4ff;--stan-bd:#bfdbfe;
  --pedn:#16803c;--pedn-lt:#f0fdf4;--pedn-bd:#bbf7d0;
  --chal:#dc2626;--chal-lt:#fef2f2;--chal-bd:#fecaca;
  --gold:#d97706;--gold-lt:#fffbeb;--red:#dc2626;--green:#059669;--orange:#ea580c;
}
html{scroll-behavior:smooth}
body{font-family:'Segoe UI',system-ui,sans-serif;background:var(--bg);color:var(--text);min-height:100vh}
nav{background:var(--white);border-bottom:2px solid var(--border);padding:0 1.25rem;display:flex;align-items:center;justify-content:space-between;height:52px;position:sticky;top:0;z-index:100;box-shadow:0 1px 6px rgba(0,0,0,.07);flex-wrap:wrap;gap:.5rem}
.nav-logo{width:32px;height:32px;border-radius:6px;background:linear-gradient(135deg,#f0b429,#ea580c);display:flex;align-items:center;justify-content:center;font-weight:800;font-size:.7rem;color:#fff;flex-shrink:0}
.nav-brand{display:flex;align-items:center;gap:.6rem}
.nav-title{font-size:.9rem;font-weight:700}
.nav-sub{font-size:.62rem;color:var(--muted);margin-top:1px}
.nav-tabs{display:flex;gap:3px;flex-wrap:wrap}
.nav-tab{font-size:.7rem;font-weight:600;padding:5px 11px;border-radius:5px;border:1.5px solid transparent;cursor:pointer;background:none;color:var(--muted);transition:all .15s;white-space:nowrap}
.nav-tab:hover{color:var(--text);background:var(--bg)}
.t-ov{color:var(--gold);background:var(--gold-lt);border-color:#fde68a}
.t-dfcu{color:var(--dfcu);background:var(--dfcu-lt);border-color:var(--dfcu-bd)}
.t-mubs{color:var(--mubs);background:var(--mubs-lt);border-color:var(--mubs-bd)}
.t-mkazi{color:var(--mkazi);background:var(--mkazi-lt);border-color:var(--mkazi-bd)}
.t-excel{color:var(--excel);background:var(--excel-lt);border-color:var(--excel-bd)}
.t-stan{color:var(--stan);background:var(--stan-lt);border-color:var(--stan-bd)}
.t-pedn{color:var(--pedn);background:var(--pedn-lt);border-color:var(--pedn-bd)}
.t-chal{color:var(--chal);background:var(--chal-lt);border-color:var(--chal-bd)}
.section{display:none}.section.active{display:block}
.page{padding:1rem 1.25rem 2rem;max-width:1440px;margin:0 auto}
.tbar{border-radius:8px 8px 0 0;padding:.6rem 1.25rem;display:flex;align-items:center;justify-content:space-between;flex-wrap:wrap;gap:.5rem}
.tbar h1{font-size:1rem;font-weight:800;color:#fff;letter-spacing:-.02em}
.tbar-meta{font-size:.65rem;color:rgba(255,255,255,.75)}
.tbar-accent{height:4px;border-radius:0 0 4px 4px;margin-bottom:.75rem}
.kpi-strip{display:grid;grid-template-columns:repeat(7,1fr);border:1px solid var(--border);border-radius:8px;overflow:hidden;margin-bottom:.85rem;background:var(--white);box-shadow:0 1px 4px rgba(0,0,0,.05)}
.kpi{padding:.85rem .7rem;border-right:1px solid var(--border);text-align:center;position:relative}
.kpi:last-child{border-right:none}
.kpi::after{content:'';position:absolute;top:0;left:0;right:0;height:3px}
.kv{font-size:1.75rem;font-weight:800;line-height:1;margin-bottom:.25rem;letter-spacing:-.04em}
.kl{font-size:.58rem;font-weight:500;color:var(--muted);text-transform:uppercase;letter-spacing:.05em;line-height:1.3}
.tcol{display:grid;grid-template-columns:1.1fr 1fr 1fr;gap:.85rem}
@media(max-width:1100px){.tcol{grid-template-columns:1fr}}
.panel{background:var(--white);border:1px solid var(--border);border-radius:8px;overflow:hidden;box-shadow:0 1px 3px rgba(0,0,0,.04)}
.ph{padding:.6rem .9rem;font-size:.65rem;font-weight:700;letter-spacing:.08em;text-transform:uppercase;color:#fff}
.pb{padding:.8rem .9rem}
.sh{font-size:.58rem;font-weight:700;letter-spacing:.1em;text-transform:uppercase;color:var(--muted);margin-bottom:.5rem;margin-top:.15rem}
.hr{display:flex;align-items:center;gap:7px;margin-bottom:.48rem}
.hl{font-size:.69rem;color:var(--text);flex:0 0 140px;white-space:nowrap;overflow:hidden;text-overflow:ellipsis}
.hl.sm{flex:0 0 108px}
.ht{flex:1;height:8px;background:#edf0f7;border-radius:2px;overflow:hidden}
.hf{height:100%;border-radius:2px}
.hn{font-family:monospace;font-size:.6rem;color:var(--muted);flex:0 0 52px;text-align:right;white-space:nowrap}
.lt{display:flex;align-items:center;gap:7px;margin-bottom:.42rem}
.ltl{font-size:.69rem;flex:0 0 72px}
.ltt{flex:1;height:9px;background:#edf0f7;border-radius:2px;overflow:hidden}
.ltf{height:100%;border-radius:2px}
.ltv{font-family:monospace;font-size:.6rem;color:var(--muted);flex:0 0 56px;text-align:right}
.dv{height:1px;background:var(--border);margin:.6rem 0}
.gb{height:13px;border-radius:6px;overflow:hidden;display:flex;margin:.35rem 0}
.smr{display:flex;gap:.6rem;flex-wrap:wrap;margin-top:.65rem;padding-top:.65rem;border-top:1px solid var(--border)}
.sm-v{font-size:1.15rem;font-weight:800;letter-spacing:-.03em;line-height:1}
.sm-l{font-size:.55rem;color:var(--muted);text-transform:uppercase;letter-spacing:.06em;margin-top:2px}
.null-box{background:var(--bg);border-radius:6px;padding:.7rem;text-align:center;font-size:.63rem;color:var(--muted)}
.null-v{font-size:1.2rem;font-weight:800;color:var(--muted);margin-bottom:2px}
.tag{font-size:.58rem;padding:2px 7px;border-radius:3px;font-weight:600}
.ov-grid{display:grid;grid-template-columns:repeat(4,1fr);gap:.85rem;margin-bottom:.85rem}
.ov-row{display:grid;grid-template-columns:repeat(3,1fr);gap:.85rem}
@media(max-width:1100px){.ov-grid{grid-template-columns:repeat(2,1fr)}.ov-row{grid-template-columns:1fr}}
.ecc{background:var(--white);border:1px solid var(--border);border-radius:8px;overflow:hidden;box-shadow:0 1px 3px rgba(0,0,0,.04)}
.ecc-h{padding:.7rem .9rem;color:#fff}
.ecc-hn{font-size:.88rem;font-weight:800}
.ecc-hr{font-size:.58rem;opacity:.8;text-transform:uppercase;letter-spacing:.08em;margin-bottom:2px}
.ecc-b{padding:.75rem .9rem}
.ecc-ks{display:flex;flex-wrap:wrap;gap:.4rem;margin-bottom:.65rem}
.ecc-k{text-align:center;background:var(--bg);border-radius:5px;padding:.4rem .5rem;flex:1;min-width:48px}
.ecc-kv{font-size:1rem;font-weight:800;line-height:1}
.ecc-kl{font-size:.5rem;color:var(--muted);text-transform:uppercase;letter-spacing:.06em;margin-top:2px}
.highlight-box{border-radius:6px;padding:.65rem;text-align:center;margin-bottom:.5rem}
.highlight-val{font-size:1.5rem;font-weight:800}
.highlight-sub{font-size:.6rem;color:var(--muted)}
"""

JS = """
const IDS=['ov','dfcu','mubs','mkazi','excel','stan','pedn','chal'];
const CLS=['t-ov','t-dfcu','t-mubs','t-mkazi','t-excel','t-stan','t-pedn','t-chal'];
function ST(id){
  IDS.forEach(t=>document.getElementById('tab-'+t).classList.remove('active'));
  document.querySelectorAll('.nav-tab').forEach(el=>CLS.forEach(c=>el.classList.remove(c)));
  document.getElementById('tab-'+id).classList.add('active');
  const i=IDS.indexOf(id);
  document.querySelectorAll('.nav-tab')[i].classList.add(CLS[i]);
  window.scrollTo({top:0,behavior:'smooth'});
}
"""


def build_nav():
    tabs = [
        ("ov",    "Overview"),
        ("dfcu",  "DFCU Foundation"),
        ("mubs",  "MUBS EIIC"),
        ("mkazi", "Mkazipreneur"),
        ("excel", "ExcelHort"),
        ("stan",  "Stanbic Incubator"),
        ("pedn",  "PEDN"),
        ("chal",  "Challenges Uganda"),
    ]
    tabs_html = "\n    ".join(
        f'<button class="nav-tab{" t-ov" if k == "ov" else ""}" onclick="ST(\'{k}\')">{label}</button>'
        for k, label in tabs
    )
    return f"""
<nav>
  <div class="nav-brand">
    <div class="nav-logo">10X</div>
    <div>
      <div class="nav-title">ESO Portfolio Intelligence</div>
      <div class="nav-sub">10X Digital Economy Programme · Outbox Uganda · March 2026</div>
    </div>
  </div>
  <div class="nav-tabs">
    {tabs_html}
  </div>
</nav>"""


def build_overview(results, agg):
    # KPI strip
    kpis = [
        (fmt_n(agg["total_n"]),     "Total Submissions",  "var(--gold)",   "#fde68a"),
        (fmt_n(agg["total_fte"]),   "FTE Workers",        "var(--green)",  "#bbf7d0"),
        (fmt_n(agg["total_pte"]),   "PTE Workers",        "var(--orange)", "#fed7aa"),
        (f"{agg['avg_female']}%",   "Female Founders",    "var(--gold)",   "#fde68a"),
        (fmt_n(agg["total_pwd"]),   "PWD Founders",       "var(--red)",    "#fecaca"),
        (fmt_n(agg["total_ursb"]),  "URSB Registered",    "#7c3aed",       "#ddd6fe"),
        (f"{agg['avg_youth']}%",    "Youth (18–35)",      "var(--mubs)",   "#bae6fd"),
    ]
    kpi_html = ""
    colors = ["#fde68a","#bbf7d0","#fed7aa","#fde68a","#fecaca","#ddd6fe","#bae6fd"]
    for i, (val, label, color, bar_color) in enumerate(kpis):
        kpi_html += (
            f'<div class="kpi" style="--bar:{bar_color}">'
            f'<div class="kv" style="color:{color}">{val}</div>'
            f'<div class="kl">{label}</div>'
            f'</div>'
        )
    # inject bar colours via inline style override
    kpi_html = kpi_html.replace('style="--bar:', 'style="').replace(
        '">', '" data-bar>', 1)

    # rebuild KPI strip with proper after pseudo-element via style tag
    kpi_items = []
    bar_colors = ["#d97706","#059669","#ea580c","#d97706","#dc2626","#7c3aed","#0369a1"]
    for (val, label, color, _), bc in zip(kpis, bar_colors):
        kpi_items.append(
            f'<div class="kpi" style="position:relative">'
            f'<div style="position:absolute;top:0;left:0;right:0;height:3px;background:{bc};border-radius:0"></div>'
            f'<div class="kv" style="color:{color}">{val}</div>'
            f'<div class="kl">{label}</div>'
            f'</div>'
        )
    kpi_strip = f'<div class="kpi-strip">{"".join(kpi_items)}</div>'

    # ESO cards
    # Row 1: DFCU, MUBS, Mkazi, ExcelHort
    row1_esos = ["DFCU Foundation","MUBS EIIC","Mkazipreneur","ExcelHort"]
    row2_esos = ["Stanbic Bank Incubator","PEDN","Challenges Uganda"]

    def eso_card(eso_name, v):
        cfg = ESO_STYLE[eso_name]
        color_var, g1, g2, region = cfg
        key = color_var

        # top 3 sectors with bars
        sector_items = list(v["sectors"].items())[:3]
        max_s = sector_items[0][1] if sector_items else 1
        sector_bars = ""
        shades = [f"var(--{key})", f"var(--{key})", f"var(--{key})"]
        for i, (s, c) in enumerate(sector_items):
            pct = c / max_s * 100
            op  = "" if i == 0 else f";opacity:{0.75 - i * 0.2:.2f}"
            sector_bars += (
                f'<div class="hr"><div class="hl sm">{s}</div>'
                f'<div class="ht"><div class="hf" style="width:{pct:.1f}%;background:var(--{key}){op}"></div></div>'
                f'<div class="hn">{fmt_n(c)}</div></div>'
            )
        district_top = list(v["districts"].items())[:2]
        district_str = " · ".join(d for d, _ in district_top)

        return f"""
<div class="ecc">
  <div class="ecc-h" style="background:linear-gradient(135deg,{g1},{g2})">
    <div class="ecc-hr">{region}</div>
    <div class="ecc-hn">{eso_name}</div>
  </div>
  <div class="ecc-b">
    <div class="ecc-ks">
      <div class="ecc-k"><div class="ecc-kv" style="color:var(--{key})">{fmt_n(v['n'])}</div><div class="ecc-kl">Submissions</div></div>
      <div class="ecc-k"><div class="ecc-kv" style="color:var(--gold)">{v['female_pct']}%</div><div class="ecc-kl">Female</div></div>
      <div class="ecc-k"><div class="ecc-kv">{fmt_n(v['fte'])}</div><div class="ecc-kl">FTE</div></div>
    </div>
    <div class="sh">Top Sectors</div>
    {sector_bars}
    <div class="dv"></div>
    <div style="display:flex;gap:.3rem;flex-wrap:wrap">
      <span class="tag" style="background:var(--{key}-lt);color:var(--{key});border:1px solid var(--{key}-bd)">{district_str}</span>
      <span class="tag" style="background:var(--bg);color:var(--muted)">UGX {fmt_rev(v['avg_rev'])} avg rev</span>
    </div>
  </div>
</div>"""

    row1 = "\n".join(eso_card(e, results[e]) for e in row1_esos if e in results)
    row2 = "\n".join(eso_card(e, results[e]) for e in row2_esos if e in results)

    tag_html = ""
    for eso in TARGET_ESOS:
        if eso not in results:
            continue
        cfg = ESO_STYLE[eso]
        k, _, _, region = cfg
        label = f"{eso.split()[0]} · {region.split()[0]}"
        tag_html += (
            f'<span class="tag" style="background:var(--{k}-lt);color:var(--{k});'
            f'border:1px solid var(--{k}-bd)">{label}</span>\n        '
        )

    return f"""
<div id="tab-ov" class="section active">
<div class="page">
  <div style="background:var(--white);border:1px solid var(--border);border-radius:8px;padding:1.25rem 1.4rem;margin-bottom:.85rem;box-shadow:0 1px 4px rgba(0,0,0,.05)">
    <div style="display:flex;justify-content:space-between;flex-wrap:wrap;gap:.75rem;align-items:flex-start">
      <div>
        <div style="font-size:.58rem;font-weight:700;letter-spacing:.12em;text-transform:uppercase;color:var(--gold);margin-bottom:.5rem">10X Digital Economy · Outbox Uganda · March 2026</div>
        <div style="font-size:1.6rem;font-weight:800;letter-spacing:-.04em;margin-bottom:.3rem">Full ESO Portfolio Summary</div>
        <div style="font-size:.8rem;color:var(--muted);max-width:560px;line-height:1.65">Raw EOI data across 7 ESO partners — {fmt_n(agg['total_n'])} submissions analysed. Covers MSME profiles, employment generated, sector distribution, and founder demographics.</div>
      </div>
      <div style="display:flex;gap:.35rem;flex-wrap:wrap;align-items:center">
        {tag_html}
      </div>
    </div>
  </div>
  {kpi_strip}
  <div class="ov-grid">{row1}</div>
  <div class="ov-row">{row2}</div>
</div>
</div>"""


def build_eso_page(eso_name, v):
    """Build the full 3-column dashboard page for one ESO."""
    cfg   = ESO_STYLE[eso_name]
    key, g1, g2, region_label = cfg
    tab_id = key

    # ── KPI STRIP ──
    kpi_defs = [
        (fmt_n(v["n"]),          "Total Submissions",  g2),
        (str(v["pwd"]),          "PWD Founders",       "#059669"),
        (str(v["refugees"]),     "Refugee Founders",   "#dc2626"),
        (f"{v['female_pct']}%",  "Female Founders",    "#d97706"),
        (f"{v['youth_pct']}%",   "Youth (18–35)",      g2),
        (region_label.split()[0],"Primary Region",     "#ea580c"),
        (fmt_n(v["fte"]),        "FTE Workers",        "#059669"),
    ]
    kpis_html = ""
    for val, label, bar_color in kpi_defs:
        text_color = bar_color
        kpis_html += (
            f'<div class="kpi" style="position:relative">'
            f'<div style="position:absolute;top:0;left:0;right:0;height:3px;background:{bar_color}"></div>'
            f'<div class="kv" style="color:{text_color}">{val}</div>'
            f'<div class="kl">{label}</div>'
            f'</div>'
        )

    # ── COL 1: SECTORS ──
    sector_items = list(v["sectors"].items())
    max_s = sector_items[0][1] if sector_items else 1
    sector_bars = ""
    for i, (s, c) in enumerate(sector_items):
        pct = c / max_s * 100
        total_pct = c / v["n"] * 100
        opacity = "" if i == 0 else f";opacity:{max(0.35, 0.9 - i*0.15):.2f}"
        sector_bars += (
            f'<div class="hr"><div class="hl">{s}</div>'
            f'<div class="ht"><div class="hf" style="width:{pct:.1f}%;background:var(--{key}){opacity}"></div></div>'
            f'<div class="hn">{fmt_n(c)}·{total_pct:.1f}%</div></div>'
        )

    col1 = f"""
<div class="panel">
  <div class="ph" style="background:var(--{key})">Sector &amp; Subsector Breakdown</div>
  <div class="pb">
    <div class="sh">Top Sectors</div>
    {sector_bars}
    <div class="dv"></div>
    <div class="sh">Context</div>
    <div style="font-size:.68rem;color:var(--muted);line-height:1.55">
      {eso_name} serves <strong>{region_label}</strong>. {fmt_n(v['n'])} EOI submissions with
      {v['female_pct']}% female founders and {v['youth_pct']}% youth (18–35).
      Average biennial revenue: UGX {fmt_rev(v['avg_rev'])}.
    </div>
  </div>
</div>"""

    # ── COL 2: LOCATION ──
    district_items = list(v["districts"].items())
    max_d = district_items[0][1] if district_items else 1
    district_bars = ""
    for i, (d, c) in enumerate(district_items):
        pct = c / max_d * 100
        opacity = "" if i == 0 else f";opacity:{max(0.35, 0.9 - i*0.12):.2f}"
        district_bars += (
            f'<div class="hr"><div class="hl">{d}</div>'
            f'<div class="ht"><div class="hf" style="width:{pct:.1f}%;background:var(--{key}){opacity}"></div></div>'
            f'<div class="hn">{fmt_n(c)}</div></div>'
        )

    collectives_html = null_box()

    smr = (
        smr_item(str(v["ursb"]),    "URSB Reg.",   f"var(--{key})")
        + smr_item(str(v["pwd"]),    "PWD",         "var(--green)")
        + smr_item(str(v["refugees"]),"Refugees",   "var(--red)")
        + smr_item(fmt_rev(v["avg_rev"]),"Avg Rev UGX","var(--gold)")
        + smr_item(fmt_n(v["pte"]),  "PTE Workers", "var(--orange)")
    )

    col2 = f"""
<div class="panel">
  <div class="ph" style="background:var(--{key})">Location &amp; Groups</div>
  <div class="pb">
    <div class="sh">Top Districts</div>
    {district_bars}
    <div class="dv"></div>
    <div class="sh">Groups &amp; Collectives</div>
    {collectives_html}
    <div class="smr">{smr}</div>
  </div>
</div>"""

    # ── COL 3: DEMOGRAPHICS ──
    gender_bar = (
        f'<div class="gb">'
        f'<div class="gf" style="width:{v["female_pct"]}%"></div>'
        f'<div style="flex:1;background:var(--{key})"></div>'
        f'</div>'
    )

    age_bands = v["age_bands"]
    max_ab = max(age_bands.values()) if age_bands else 1
    age_bars = ""
    for band, cnt in age_bands.items():
        pct = cnt / max_ab * 100
        total_pct = cnt / sum(age_bands.values()) * 100 if sum(age_bands.values()) > 0 else 0
        age_bars += (
            f'<div class="hr"><div class="hl sm">{band}</div>'
            f'<div class="ht"><div class="hf" style="width:{pct:.1f}%;background:var(--{key})"></div></div>'
            f'<div class="hn">{fmt_n(cnt)}·{total_pct:.1f}%</div></div>'
        )

    col3 = f"""
<div class="panel">
  <div class="ph" style="background:var(--{key})">Archetypes &amp; Demographics</div>
  <div class="pb">
    <div style="text-align:center;font-size:.8rem;font-weight:700;margin-bottom:.3rem">
      <span style="color:var(--gold)">♀ {v['female_pct']}% Female</span>&nbsp;·&nbsp;
      <span style="color:var(--{key})">♂ {v['male_pct']}% Male</span>
    </div>
    {gender_bar}
    <div class="dv"></div>
    <div style="display:grid;grid-template-columns:1fr 1fr;gap:.6rem">
      <div>
        <div class="sh">Age of Lead Founder</div>
        {age_bars}
      </div>
      <div>
        <div class="sh">Youth Highlight</div>
        <div class="highlight-box" style="background:var(--{key}-lt)">
          <div class="highlight-val" style="color:var(--{key})">{v['youth_pct']}%</div>
          <div class="highlight-sub">Youth founders (18–35)</div>
        </div>
        <div class="sh">Employment</div>
        <div class="hr"><div class="hl sm">FTE</div>
          <div class="ht"><div class="hf" style="width:100%;background:var(--{key})"></div></div>
          <div class="hn">{fmt_n(v['fte'])}</div></div>
        <div class="hr"><div class="hl sm">PTE</div>
          <div class="ht"><div class="hf" style="width:{v['pte']/max(v['fte'],1)*100:.1f}%;background:var(--{key});opacity:.6"></div></div>
          <div class="hn">{fmt_n(v['pte'])}</div></div>
      </div>
    </div>
  </div>
</div>"""

    return f"""
<div id="tab-{tab_id}" class="section">
<div class="page">
  <div class="tbar" style="background:linear-gradient(135deg,{g1},{g2})">
    <h1>{eso_name} · MSME Portfolio Summary</h1>
    <span class="tbar-meta">10X EOI Raw Data · {region_label} · March 2026</span>
  </div>
  <div class="tbar-accent" style="background:linear-gradient(90deg,{g2},rgba(255,255,255,.3),transparent)"></div>
  <div class="kpi-strip">{kpis_html}</div>
  <div class="tcol">
    {col1}
    {col2}
    {col3}
  </div>
</div>
</div>"""


# ─────────────────────────────────────────────────────────────────────────────
# STEP 8 — ASSEMBLE FULL HTML
# ─────────────────────────────────────────────────────────────────────────────

def build_html(results, agg):
    nav      = build_nav()
    overview = build_overview(results, agg)
    eso_pages = "\n".join(
        build_eso_page(eso, results[eso])
        for eso in TARGET_ESOS
        if eso in results
    )
    return f"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>10X Digital Economy · ESO Portfolio</title>
<style>
{CSS}
</style>
</head>
<body>
{nav}
{overview}
{eso_pages}
<script>
{JS}
</script>
</body>
</html>"""


# ─────────────────────────────────────────────────────────────────────────────
# MAIN
# ─────────────────────────────────────────────────────────────────────────────

def main():
    # 1. Load
    main_df, founders_df = load_data(INPUT_FILE)

    # 2. Merge founders
    founders_merged = merge_founders(main_df, founders_df)

    # 3. Extract per-ESO stats
    print("\n📊  Extracting per-ESO statistics...")
    results = {}
    for eso in TARGET_ESOS:
        print(f"    → {eso}")
        stats = extract_eso_stats(eso, main_df, founders_merged)
        if stats:
            results[eso] = stats

    # 4. Aggregates
    agg = compute_aggregates(results)

    # 5. Console summary
    print_summary(results, agg)

    # 6. Save JSON
    with open(OUTPUT_JSON, "w") as f:
        json.dump({"aggregates": agg, "esos": results}, f, indent=2)
    print(f"\n✅  Saved: {OUTPUT_JSON}")

    # 7. Generate and save HTML
    print(f"🌐  Building HTML dashboard...")
    html = build_html(results, agg)
    with open(OUTPUT_HTML, "w", encoding="utf-8") as f:
        f.write(html)
    print(f"✅  Saved: {OUTPUT_HTML}")
    print(f"\n Done! Open {OUTPUT_HTML} in any browser.")

    return results, agg


if __name__ == "__main__":
    main()