#!/usr/bin/env python3
"""
Portfolio Data Extractor
========================
Scans all .xlsx files in this folder, detects their type (segmentation or growth plans),
extracts key metrics, and writes data.js for the dashboard.html interactive dashboard.

Run this script every time you add new Excel files:
    python extract_data.py

Or double-click run_dashboard.bat to extract and open the dashboard automatically.
"""

import pandas as pd
import json
import re
import traceback
from pathlib import Path

BASE_DIR = Path(__file__).parent

# Folders that contain portfolio Excel files (relative to BASE_DIR).
# Files starting with '~$' (Excel temp/lock files) are always skipped.
PORTFOLIO_DIRS = [
    BASE_DIR / 'EOI' / '_cleaned',   # segmentation portfolios
    BASE_DIR / 'EOI' / '_eoi_eso',   # EOI application files
    BASE_DIR / 'YIW',                # Youth in Work assessments
    BASE_DIR / 'Buz_needs',          # Business needs assessments
    BASE_DIR / 'Devices',            # Device financing data
    BASE_DIR,                         # any .xlsx placed directly in root
]

SECTOR_NORMALIZE = {
    'trade and services': 'Trade & Services',
    'trade & services': 'Trade & Services',
    'fashion and design': 'Fashion & Design',
    'fashion & design': 'Fashion & Design',
    'light manufacturing': 'Light Manufacturing',
    'agriculture': 'Agriculture',
    'health': 'Health',
    'others': 'Others',
    'others / events': 'Others / Events',
    'meetings & events': 'Meetings & Events',
    'meetings and events': 'Meetings & Events',
}


def normalize_sector(s):
    if pd.isna(s) or str(s).strip() == '':
        return 'Unknown'
    s = str(s).strip()
    return SECTOR_NORMALIZE.get(s.lower(), s)


def find_col(df, *candidates):
    """Find the first matching column name (case-insensitive)."""
    lower_map = {str(c).lower().strip(): c for c in df.columns}
    for candidate in candidates:
        key = candidate.lower().strip()
        if key in lower_map:
            return lower_map[key]
    return None


def find_header_row(xl, sheet_name):
    """Find the row index that contains '#' and 'Business Name' as column headers."""
    df = xl.parse(sheet_name, header=None, nrows=10)
    for i, row in df.iterrows():
        vals = [str(v).strip() for v in row]
        if '#' in vals and 'Business Name' in vals:
            return i
    return 0


def safe_int(val, default=0):
    try:
        f = float(str(val).replace(',', '').strip())
        if pd.isna(f):
            return default
        return int(f)
    except Exception:
        return default


def value_counts_dict(series, top_n=None):
    """Return {value: count} dict, dropping nulls/empty/nan strings."""
    vc = series.dropna().astype(str).str.strip()
    vc = vc[(vc.str.len() > 0) & (vc != 'nan') & (vc != 'NaN')]
    result = {k: int(v) for k, v in vc.value_counts().items()}
    if top_n:
        result = dict(list(result.items())[:top_n])
    return result


def safe_sum(series):
    return int(pd.to_numeric(series, errors='coerce').fillna(0).sum())


def find_col_like(df, *keywords):
    """Find first column whose name contains ALL keywords (case-insensitive)."""
    kw = [k.lower() for k in keywords]
    for c in df.columns:
        cl = c.lower()
        if all(k in cl for k in kw):
            return c
    return None


def parse_summary_extras(xl):
    """Scan Summary sheet for PWD, refugees, URSB, avg revenue, collectives."""
    result = {'pwd': 0, 'refugees': 0, 'ursb': 0, 'avg_revenue_str': '', 'collectives_from_summary': 0}
    try:
        df = xl.parse('Summary', header=None, nrows=30)
        rows = {i: row for i, row in df.iterrows()}
        for i, row in df.iterrows():
            row_str = ' '.join(str(v) for v in row if str(v) != 'nan').upper()
            if 'URSB' in row_str and 'PWD' in row_str and 'REFUGEE' in row_str and i > 0:
                data_row = rows[i - 1]
                nums = [str(v).strip() for v in data_row if str(v).strip() not in ('nan', '')]
                if len(nums) >= 1: result['ursb']            = safe_int(nums[0])
                if len(nums) >= 2: result['pwd']             = safe_int(nums[1])
                if len(nums) >= 3: result['refugees']        = safe_int(nums[2])
                if len(nums) >= 4: result['avg_revenue_str'] = str(nums[3]).strip()
            if 'COLLECTIVES' in row_str and 'FTE' in row_str and i > 0:
                data_row = rows[i - 1]
                nums = [str(v).strip() for v in data_row if str(v).strip() not in ('nan', '')]
                if len(nums) >= 3:
                    result['collectives_from_summary'] = safe_int(nums[2])
    except Exception:
        pass
    return result


def parse_groups_collectives(xl):
    """Parse group type breakdown from Cooperatives & Groups sheet."""
    groups = {}
    collectives_total = 0
    try:
        if 'Cooperatives & Groups' not in xl.sheet_names:
            return groups, collectives_total
        df = xl.parse('Cooperatives & Groups', header=None, nrows=8)
        for _, row in df.iterrows():
            for val in row:
                s = str(val)
                if 'Total:' in s and '|' in s:
                    for part in s.split('|'):
                        m = re.match(r'\s*(.+?):\s*(\d+)', part.strip())
                        if m:
                            key, n = m.group(1).strip(), int(m.group(2))
                            if key.lower() == 'total':
                                collectives_total = n
                            elif n > 0:
                                groups[key] = n
    except Exception:
        pass
    return groups, collectives_total


def detect_file_type(xl):
    """Detect file type from sheet names and first-row column names."""
    sheets = set(xl.sheet_names)
    if {'MSME List', 'Summary', 'Segmentation Matrix'}.issubset(sheets):
        return 'segmentation'
    if any('founder' in s.lower() for s in xl.sheet_names):
        return 'eoi'
    if any('youth' in s.lower() for s in xl.sheet_names):
        return 'yiw'
    try:
        peek = xl.parse(xl.sheet_names[0], nrows=0)
        cols_lower = [str(c).lower() for c in peek.columns]
        if any(c.startswith('1.implementing_partner') for c in cols_lower):
            return 'buz_needs'
        # Devices: check before EOI since device forms also ask about business names
        if any(k in cols_lower for k in ['has_disability', 'mtn_number']) or \
           any('mtn telephone' in c or 'mtn number' in c for c in cols_lower):
            return 'devices'
        # EOI forms always ask for business/enterprise name — use as fallback signal
        if any('name of your business' in c or 'name of your enterprise' in c
               for c in cols_lower):
            return 'eoi'
    except Exception:
        pass
    return 'growth_plans'


def parse_portfolio_name(filename, summary_df):
    """Extract a clean portfolio name from the Summary sheet row 0."""
    try:
        row0 = summary_df.iloc[0]
        for val in row0:
            s = str(val).strip()
            if s and s != 'nan' and len(s) > 10 and 'PORTFOLIO' in s.upper():
                parts = s.split('·')
                name = parts[-1].strip() if len(parts) > 1 else s
                # Remove "10X DIGITAL ECONOMY" prefix if present
                return re.sub(r'^10X DIGITAL ECONOMY\s*', '', name, flags=re.IGNORECASE).strip()
    except Exception:
        pass
    # Fallback: clean up filename
    stem = Path(filename).stem
    stem = re.sub(r'[-_]+', ' ', stem)
    stem = re.sub(r'\s+', ' ', stem).strip()
    return stem


def parse_segmentation_file(filename, xl):
    print(f'  Detected: segmentation')

    # Portfolio name from Summary sheet
    summary = xl.parse('Summary', header=None, nrows=6)
    portfolio_name = parse_portfolio_name(filename, summary)

    # Stats from summary row 4 (0-indexed): [total, clean, duplicates, ...]
    total, clean, dupes = 0, 0, 0
    if len(summary) > 4:
        row4 = summary.iloc[4]
        total = safe_int(row4.iloc[0]) if len(row4) > 0 else 0
        clean = safe_int(row4.iloc[1]) if len(row4) > 1 else 0
        dupes = safe_int(row4.iloc[2]) if len(row4) > 2 else 0

    # Parse MSME List — locate header row automatically
    hrow = find_header_row(xl, 'MSME List')
    df = xl.parse('MSME List', header=hrow)
    df = df[[c for c in df.columns if not str(c).startswith('Unnamed')]]

    biz_name_col = find_col(df, 'Business Name')
    if biz_name_col:
        df = df.dropna(subset=[biz_name_col])
        df = df[df[biz_name_col].astype(str).str.strip().str.len() > 0]
        df = df[df[biz_name_col].astype(str).str.strip() != 'nan']

    # Normalize sectors
    sector_col = find_col(df, 'Sector (Clean)', 'Sector')
    if sector_col:
        df['_Sector'] = df[sector_col].apply(normalize_sector)
    else:
        df['_Sector'] = 'Unknown'

    subsector_col = find_col(df, 'Subsector', 'Subsector (auto)')
    location_col  = find_col(df, 'Location', 'Location Type')
    archetype_col = find_col(df, 'Archetype', 'MSME Archetype')
    district_col  = find_col(df, 'District')
    age_col       = find_col(df, 'Age Band')
    gender_col    = find_col(df, 'Gender')
    edu_col       = find_col(df, 'Education Level')
    biz_type_col  = find_col(df, 'Business Type')
    fnd_col       = find_col(df, '# Founders')
    fem_col       = find_col(df, '# Female F.', '# Female Founders')
    fte_col       = find_col(df, 'FTE')
    pte_col       = find_col(df, 'PTE')
    rev_col       = find_col(df, 'Annual Revenue (UGX)', 'Revenue')
    fund_col      = find_col(df, 'Funding Need (UGX)', 'Funding Need')

    sectors    = value_counts_dict(df['_Sector'])
    subsectors = value_counts_dict(df[subsector_col], top_n=15) if subsector_col else {}
    gender     = value_counts_dict(df[gender_col])   if gender_col   else {}
    location   = value_counts_dict(df[location_col]) if location_col else {}
    archetypes = value_counts_dict(df[archetype_col]) if archetype_col else {}
    districts  = value_counts_dict(df[district_col], top_n=15) if district_col else {}
    age_bands  = value_counts_dict(df[age_col])      if age_col      else {}
    education  = value_counts_dict(df[edu_col])      if edu_col      else {}
    biz_types  = value_counts_dict(df[biz_type_col]) if biz_type_col else {}

    total_founders  = safe_sum(df[fnd_col]) if fnd_col else 0
    female_founders = safe_sum(df[fem_col]) if fem_col else 0
    fte = safe_sum(df[fte_col]) if fte_col else 0
    pte = safe_sum(df[pte_col]) if pte_col else 0

    rev_series   = pd.to_numeric(df[rev_col],  errors='coerce') if rev_col  else pd.Series(dtype=float)
    fund_series  = pd.to_numeric(df[fund_col], errors='coerce') if fund_col else pd.Series(dtype=float)
    rev_nonzero  = rev_series[rev_series > 0].dropna()
    fund_nonzero = fund_series[fund_series > 0].dropna()

    # ── Extra fields ──────────────────────────────────────────────
    extras = parse_summary_extras(xl)
    groups, coll_total = parse_groups_collectives(xl)

    emp_col = find_col(df, 'Employment Status')
    employment_status = value_counts_dict(df[emp_col]) if emp_col else {}

    stats_total = total if total > 0 else len(df)

    # Youth: age bands 18-25 + 26-35
    youth_count = sum(age_bands.get(b, 0) for b in ['18–25', '26–35'])
    youth_pct   = round(youth_count / max(stats_total, 1) * 100, 1)

    # Rural
    rural_count = int((df[location_col].astype(str).str.strip() == 'Rural').sum()) if location_col else 0
    rural_pct   = round(rural_count / max(stats_total, 1) * 100, 1)

    # Female % (from gender column, not founder count)
    female_from_gender = gender.get('Female', 0)
    female_pct = round(female_from_gender / max(stats_total, 1) * 100, 1)

    # Main sector (highest by count)
    main_sector = max(sectors, key=sectors.get) if sectors else 'All'

    avg_rev_str = extras['avg_revenue_str']

    return {
        'type': 'segmentation',
        'name': portfolio_name,
        'filename': filename,
        'stats': {
            'total':        stats_total,
            'clean':        clean if clean > 0 else len(df),
            'duplicates':   dupes,
            'record_count': len(df),
        },
        'fte':             fte,
        'pte':             pte,
        'total_founders':  total_founders,
        'female_founders': female_founders,
        'sectors':         sectors,
        'subsectors':      subsectors,
        'gender':          gender,
        'location':        location,
        'archetypes':      archetypes,
        'districts':       districts,
        'age_bands':       age_bands,
        'education':       education,
        'biz_types':       biz_types,
        'employment_status': employment_status,
        'revenue': {
            'median': int(rev_nonzero.median())  if len(rev_nonzero) > 0 else 0,
            'mean':   int(rev_nonzero.mean())    if len(rev_nonzero) > 0 else 0,
            'count':  int(len(rev_nonzero)),
        },
        'funding': {
            'median': int(fund_nonzero.median()) if len(fund_nonzero) > 0 else 0,
            'mean':   int(fund_nonzero.mean())   if len(fund_nonzero) > 0 else 0,
            'count':  int(len(fund_nonzero)),
        },
        # enriched fields
        'pwd':              extras['pwd'],
        'refugees':         extras['refugees'],
        'ursb':             extras['ursb'],
        'avg_revenue_str':  avg_rev_str,
        'collectives_total': coll_total or extras['collectives_from_summary'],
        'groups':           groups,
        'youth_count':      youth_count,
        'youth_pct':        youth_pct,
        'rural_count':      rural_count,
        'rural_pct':        rural_pct,
        'female_pct':       female_pct,
        'main_sector':      main_sector,
    }


def infer_growth_plan_name(filename):
    """Derive a short human-readable name from a growth plans filename."""
    stem = Path(filename).stem
    # Check for known program keywords
    for keyword in ['PEDN', 'MKazi', 'Stanbic', 'Incubator']:
        if keyword.lower() in stem.lower():
            return f'{keyword} Growth Plans'
    return f'Growth Plans'


def parse_growth_plans_file(filename, xl):
    print(f'  Detected: growth_plans')
    all_dfs = []

    for sheet in xl.sheet_names:
        district = sheet.strip()
        df = xl.parse(sheet)

        # Auto-detect header (row 0 or row 1)
        msme_col = find_col(df, 'MSME Name', 'Business Name')
        if not msme_col:
            df = xl.parse(sheet, header=1)
            msme_col = find_col(df, 'MSME Name', 'Business Name')
        if not msme_col:
            continue

        df.columns = df.columns.str.strip()
        df = df.dropna(subset=[msme_col])
        df = df[df[msme_col].astype(str).str.strip().str.len() > 0]
        df['_District'] = district
        all_dfs.append(df)

    if not all_dfs:
        return None

    combined = pd.concat(all_dfs, ignore_index=True)
    combined.columns = combined.columns.str.strip()

    sector_col     = find_col(combined, 'Sector')
    gender_col     = find_col(combined, 'Gender')
    commodity_col  = find_col(combined, 'Commodity')
    vc_col         = find_col(combined, 'Value Chain Role')
    biz_struct_col = find_col(combined, 'Business Structure')
    age_col        = find_col(combined, 'Age')

    sectors      = value_counts_dict(combined[sector_col])                    if sector_col     else {}
    gender       = value_counts_dict(combined[gender_col])                    if gender_col     else {}
    districts    = value_counts_dict(combined['_District'])
    commodities  = value_counts_dict(combined[commodity_col], top_n=15)       if commodity_col  else {}
    value_chain  = value_counts_dict(combined[vc_col])                        if vc_col         else {}
    biz_structs  = value_counts_dict(combined[biz_struct_col])                if biz_struct_col else {}

    # Women / Youth led (may be 1/0 or Yes/No)
    women_led, youth_led = 0, 0
    wl_col = find_col(combined, 'Women led', 'Women Led')
    if wl_col:
        wl = combined[wl_col].astype(str).str.strip().str.lower()
        women_led = int(wl.isin(['1', 'yes', 'true', '1.0']).sum())

    yl_col = find_col(combined, 'Youth led', 'Youth Led')
    if yl_col:
        yl = combined[yl_col].astype(str).str.strip().str.lower()
        youth_led = int(yl.isin(['1', 'yes', 'true', '1.0']).sum())

    # Age distribution: numeric ages → buckets
    age_bands = {}
    if age_col:
        ages = pd.to_numeric(combined[age_col], errors='coerce').dropna()
        if len(ages) > 0:
            bins   = [0, 17, 25, 35, 45, 55, 120]
            labels = ['Under 18', '18–25', '26–35', '36–45', '46–55', '56+']
            age_cats = pd.cut(ages, bins=bins, labels=labels)
            age_bands = {str(k): int(v) for k, v in age_cats.value_counts().sort_index().items() if v > 0}

    return {
        'type': 'growth_plans',
        'name': infer_growth_plan_name(filename),
        'filename': filename,
        'stats': {
            'total':        len(combined),
            'clean':        len(combined),
            'duplicates':   0,
            'record_count': len(combined),
        },
        'sectors':     sectors,
        'gender':      gender,
        'districts':   districts,
        'commodities': commodities,
        'value_chain': value_chain,
        'biz_structures': biz_structs,
        'women_led':   women_led,
        'youth_led':   youth_led,
        'age_bands':   age_bands,
    }


# ══════════════════════════════════════════════════════════════════════════════
# NEW DATA-SOURCE PARSERS
# ══════════════════════════════════════════════════════════════════════════════

def parse_eoi_file(filename, xl):
    print('  Detected: eoi')
    # ── Main sheet ──────────────────────────────────────────────────────────
    main_sheet = xl.sheet_names[0]
    df = xl.parse(main_sheet, dtype=str)
    df = df.dropna(how='all')

    eso_col      = find_col_like(df, 'implementing_partner') or find_col_like(df, 'implementing', 'partner')
    sector_col   = find_col_like(df, 'sector')
    district_col = find_col_like(df, 'district')
    ursb_col     = find_col_like(df, 'ursb')
    fnd_col      = find_col_like(df, 'how many founders')
    fem_col      = find_col_like(df, 'founders are female') or find_col_like(df, 'female', 'founder')
    rev_col      = find_col_like(df, 'revenue', 'ugx')
    fund_col     = find_col_like(df, 'funding', 'ugx') or find_col_like(df, 'funding needed')
    tin_col      = find_col_like(df, 'tax identification')
    nssf_col     = find_col_like(df, 'nssf')

    eso_name = ''
    if eso_col:
        vals = df[eso_col].dropna().astype(str).str.strip()
        vals = vals[(vals.str.len() > 0) & (vals != 'nan')]
        if len(vals):
            eso_name = vals.mode().iloc[0]
    if not eso_name:
        eso_name = Path(filename).stem.split('_')[0][:25]

    total     = len(df)
    sectors   = value_counts_dict(df[sector_col],   top_n=15) if sector_col   else {}
    districts = value_counts_dict(df[district_col], top_n=15) if district_col else {}

    ursb_count = 0
    if ursb_col:
        ursb_count = int((df[ursb_col].astype(str).str.strip().str.lower() == 'yes').sum())

    total_founders  = safe_sum(pd.to_numeric(df[fnd_col],  errors='coerce')) if fnd_col  else 0
    female_founders = safe_sum(pd.to_numeric(df[fem_col], errors='coerce')) if fem_col else 0

    revenue_bands = value_counts_dict(df[rev_col])  if rev_col  else {}
    funding_bands = value_counts_dict(df[fund_col]) if fund_col else {}

    # ── Archetypes from revenue ──────────────────────────────────────────────
    def _arch_label(v):
        try:
            v = float(str(v).replace(',', '').strip())
        except Exception:
            return 'Invisibles'
        if pd.isna(v) or v == 0:
            return 'Invisibles'
        annual = v / 2
        if annual < 2_000_000:   return 'Gig Workers'
        if annual < 15_000_000:  return 'Bootstrappers'
        if annual < 50_000_000:  return 'Bootstrappers SME'
        return 'Gazelles'

    archetypes = {}
    if rev_col:
        arch_s = df[rev_col].apply(_arch_label)
        archetypes = arch_s.value_counts().to_dict()
    else:
        archetypes = {'Invisibles': total}

    # ── Registration status (TIN / NSSF) ─────────────────────────────────────
    tin_status  = value_counts_dict(df[tin_col],  top_n=5) if tin_col  else {}
    nssf_status = value_counts_dict(df[nssf_col], top_n=5) if nssf_col else {}

    # ── Founders sub-sheet ──────────────────────────────────────────────────
    founders_sheet  = next((s for s in xl.sheet_names if 'founder' in s.lower()), None)
    founders_gender = {}
    founders_pwd    = 0
    founders_refugees = 0
    age_bands  = {}
    id_status  = {}
    if founders_sheet:
        try:
            fdf = xl.parse(founders_sheet, dtype=str)
            fdf = fdf.dropna(how='all')
            gender_col_f = find_col_like(fdf, 'gender')
            if gender_col_f:
                founders_gender = value_counts_dict(fdf[gender_col_f])
            pwd_col_f = find_col_like(fdf, 'person with disabilities') or find_col_like(fdf, 'disability')
            if pwd_col_f:
                founders_pwd = int((fdf[pwd_col_f].astype(str).str.strip().str.lower() == 'yes').sum())
            citizen_col = find_col_like(fdf, 'citizenship') or find_col_like(fdf, 'nationality')
            if citizen_col:
                founders_refugees = int(
                    fdf[citizen_col].astype(str).str.lower().str.contains('refugee').sum()
                )
            dob_col = find_col_like(fdf, 'date of birth') or find_col_like(fdf, 'birth')
            if dob_col:
                ages = (pd.Timestamp.now() - pd.to_datetime(fdf[dob_col], errors='coerce')).dt.days / 365.25
                ages = ages.dropna()
                if len(ages) > 0:
                    bins   = [0, 17, 25, 35, 45, 55, 120]
                    labels = ['Under 18', '18–25', '26–35', '36–45', '46–55', '56+']
                    age_cats = pd.cut(ages, bins=bins, labels=labels)
                    age_bands = {
                        str(k): int(v)
                        for k, v in age_cats.value_counts().sort_index().items() if v > 0
                    }
            # National ID upload status for founders
            nid_upload_col = find_col_like(fdf, 'national id', 'upload')
            # exclude _url columns
            if nid_upload_col and '_url' in nid_upload_col.lower():
                nid_upload_col = None
            if nid_upload_col:
                filled = fdf[nid_upload_col].dropna().astype(str).str.strip()
                has_id = (filled != '') & (filled.str.lower() != 'nan')
                id_status = {
                    'Has National ID': int(has_id.sum()),
                    'Missing ID':      int((~has_id).sum()),
                }
        except Exception:
            pass

    # ── Employee NIN (FTE + PTE sheets) ─────────────────────────────────────
    id_status  = id_status  if 'id_status'  in dir() else {}
    nin_with   = 0
    nin_without = 0
    for emp_sheet in xl.sheet_names:
        if ('full' in emp_sheet.lower() or 'part' in emp_sheet.lower()) and \
           ('employ' in emp_sheet.lower() or 'fte' in emp_sheet.lower() or 'pte' in emp_sheet.lower()):
            try:
                edf = xl.parse(emp_sheet, dtype=str)
                nin_col_e = find_col_like(edf, 'national identification number') or find_col_like(edf, 'nin')
                if nin_col_e:
                    e_filled = edf[nin_col_e].dropna().astype(str).str.strip()
                    e_filled = e_filled[(e_filled != '') & (e_filled.str.lower() != 'nan')]
                    nin_with    += len(e_filled)
                    nin_without += len(edf) - len(e_filled)
            except Exception:
                pass
    nin_status = {'Has NIN': nin_with, 'No NIN': nin_without} if (nin_with + nin_without) > 0 else {}

    female_pct_f = round(
        founders_gender.get('Female', 0) / max(sum(founders_gender.values()), 1) * 100, 1
    )
    return {
        'type':     'eoi',
        'name':     f'{eso_name} EOI',
        'eso':      eso_name,
        'filename': filename,
        'stats': {
            'total':        total,
            'ursb':         ursb_count,
            'pwd':          founders_pwd,
            'refugees':     founders_refugees,
            'record_count': total,
        },
        'ursb_pct':          round(ursb_count / max(total, 1) * 100, 1),
        'sectors':           sectors,
        'districts':         districts,
        'total_founders':    int(total_founders),
        'female_founders':   int(female_founders),
        'revenue_bands':     revenue_bands,
        'funding_bands':     funding_bands,
        'archetypes':        archetypes,
        'tin_status':        tin_status,
        'nssf_status':       nssf_status,
        'id_status':         id_status,
        'nin_status':        nin_status,
        'founders': {
            'gender':     founders_gender,
            'female_pct': female_pct_f,
            'with_pwd':   founders_pwd,
            'refugees':   founders_refugees,
        },
        'age_bands': age_bands,
    }


def parse_yiw_file(filename, xl):
    print('  Detected: yiw')
    sheet = xl.sheet_names[0]
    df    = xl.parse(sheet, dtype=str)
    df    = df.dropna(how='all')

    eso_col      = find_col_like(df, 'implementing_partner') or find_col_like(df, 'implementing', 'partner')
    sector_col   = find_col_like(df, 'sector')
    district_col = find_col_like(df, 'district')
    earned_col   = find_col_like(df, 'earned', 'income', 'result') or find_col_like(df, 'earned an income')
    improved_col = find_col_like(df, 'working conditions', 'improved') or find_col_like(df, 'work improved')
    income_col   = (find_col_like(df, 'how much', 'earned') or
                    find_col_like(df, 'current earnings') or
                    find_col_like(df, 'current income'))
    # Foundation-completion columns may appear under multiple names across versions
    found_cols = [c for c in df.columns
                  if 'foundation' in c.lower() and
                     ('complete' in c.lower() or 'course' in c.lower())]

    total     = len(df)
    sectors   = value_counts_dict(df[sector_col],   top_n=15) if sector_col   else {}
    districts = value_counts_dict(df[district_col], top_n=15) if district_col else {}
    income_levels = value_counts_dict(df[income_col], top_n=10) if income_col else {}

    by_eso = {}
    if eso_col:
        for eso, grp in df.groupby(eso_col):
            eso = str(eso).strip()
            if eso and eso != 'nan':
                by_eso[eso] = {'total': len(grp)}

    def yes_pct(col):
        if not col or col not in df.columns:
            return 0.0
        s = df[col].astype(str).str.strip().str.lower()
        return round(s.isin(['yes', 'yes, i have', 'yes, i have earned']).sum() / max(total, 1) * 100, 1)

    earned_pct   = yes_pct(earned_col)
    improved_pct = yes_pct(improved_col)
    found_pct    = 0.0
    for fc in found_cols:
        s   = df[fc].astype(str).str.strip().str.lower()
        pct = round(s.isin(['yes', 'yes, completed', 'completed']).sum() / max(total, 1) * 100, 1)
        if pct > found_pct:
            found_pct = pct

    return {
        'type':     'yiw',
        'name':     'Youth in Work',
        'filename': filename,
        'stats': {
            'total':        total,
            'record_count': total,
        },
        'earned_income_pct':   earned_pct,
        'work_improved_pct':   improved_pct,
        'foundation_done_pct': found_pct,
        'by_eso':       by_eso,
        'sectors':      sectors,
        'districts':    districts,
        'income_levels': income_levels,
    }


def parse_buz_needs_file(filename, xl):
    print('  Detected: buz_needs')
    sheet = xl.sheet_names[0]
    df    = xl.parse(sheet, dtype=str)
    df    = df.dropna(how='all')

    eso_col      = find_col_like(df, 'implementing_partner') or find_col_like(df, 'implementing', 'partner')
    sector_col   = find_col_like(df, 'b.sector') or find_col_like(df, 'sector')
    district_col = find_col_like(df, 'district')
    reg_col      = (find_col_like(df, 'business registered') or
                    find_col_like(df, 'is your business registered'))
    pwd_col      = (find_col_like(df, 'person with a disability') or
                    find_col_like(df, 'disability'))
    refugee_col  = find_col_like(df, 'refugee') or find_col_like(df, 'which country')
    device_col   = find_col_like(df, 'need a device') or find_col_like(df, 'need device')
    income_col   = find_col_like(df, 'average income') or find_col_like(df, 'income per month')
    digital_cols = [c for c in df.columns if c.startswith('20.') or 'digital skills' in c.lower()]

    total     = len(df)
    sectors   = value_counts_dict(df[sector_col],   top_n=15) if sector_col   else {}
    districts = value_counts_dict(df[district_col], top_n=15) if district_col else {}
    income_levels = value_counts_dict(df[income_col], top_n=10) if income_col else {}

    by_eso = {}
    if eso_col:
        for eso, grp in df.groupby(eso_col):
            eso = str(eso).strip()
            if eso and eso != 'nan':
                by_eso[eso] = {'total': len(grp)}

    def yes_pct(col):
        if not col or col not in df.columns:
            return 0.0
        s = df[col].astype(str).str.strip().str.lower()
        return round(s.isin(['yes', 'yes, i am']).sum() / max(total, 1) * 100, 1)

    def yes_count(col):
        if not col or col not in df.columns:
            return 0
        s = df[col].astype(str).str.strip().str.lower()
        return int(s.isin(['yes', 'yes, i am']).sum())

    registered_pct  = yes_pct(reg_col)
    pwd_count       = yes_count(pwd_col)
    device_need_pct = yes_pct(device_col)

    refugee_count = 0
    if refugee_col:
        r = df[refugee_col].astype(str).str.strip()
        refugee_count = int(((r.str.len() > 0) & (r != 'nan')).sum())

    digital_skills = {}
    for dc in digital_cols[:15]:
        label = dc.split('/')[-1].strip() if '/' in dc else dc.strip()
        vals  = df[dc].astype(str).str.strip().str.lower()
        yes_n = int(vals.isin(['yes', '1', 'true', 'checked', 'selected']).sum())
        if yes_n > 0:
            digital_skills[label] = yes_n

    return {
        'type':     'buz_needs',
        'name':     'Business Needs',
        'filename': filename,
        'stats': {
            'total':        total,
            'pwd':          pwd_count,
            'refugees':     refugee_count,
            'record_count': total,
        },
        'registered_pct':  registered_pct,
        'device_need_pct': device_need_pct,
        'pwd_pct':         round(pwd_count / max(total, 1) * 100, 1),
        'by_eso':          by_eso,
        'sectors':         sectors,
        'districts':       districts,
        'income_levels':   income_levels,
        'digital_skills':  digital_skills,
    }


def parse_devices_file(filename, xl):
    print('  Detected: devices')
    sheet = xl.sheet_names[0]
    df    = xl.parse(sheet, dtype=str)
    df    = df.dropna(how='all')

    cols_lower = {c.lower(): c for c in df.columns}

    # Disability
    if 'has_disability' in cols_lower:
        disability_col = cols_lower['has_disability']
    else:
        disability_col = (find_col_like(df, 'form of disability') or
                          find_col_like(df, 'disability'))

    disability_type_col = (cols_lower.get('disability_type') or
                           find_col_like(df, 'type of disability') or
                           find_col_like(df, 'disability', 'type'))

    # Business registered
    if 'business_registered' in cols_lower:
        biz_reg_col = cols_lower['business_registered']
    else:
        biz_reg_col = (find_col_like(df, 'business registered') or
                       find_col_like(df, 'business', 'registered') or
                       find_col_like(df, 'own a business'))

    # Registration body
    reg_body_col = (cols_lower.get('registration_body') or
                    find_col_like(df, 'registered with') or
                    find_col_like(df, 'registration', 'body') or
                    find_col_like(df, 'authority'))

    # ESO hub
    eso_hub_col = cols_lower.get('eso_hub') or find_col_like(df, 'eso', 'hub')

    # ID type
    id_type_col = (cols_lower.get('id_type') or
                   find_col_like(df, 'type of identification') or
                   find_col_like(df, 'id', 'type'))

    # Device type (combined text column preferred over binary sub-columns)
    device_type_col = cols_lower.get('device_type') or find_col_like(df, 'device', 'type')

    # Price / installment
    price_col    = (cols_lower.get('price_range') or
                    find_col_like(df, 'total price') or
                    find_col_like(df, 'price'))
    payment_col  = (cols_lower.get('preferred_installment') or
                    find_col_like(df, 'preferred', 'installment') or
                    find_col_like(df, 'payment', 'installment'))
    duration_col = (cols_lower.get('payment_duration') or
                    find_col_like(df, 'payment', 'period') or
                    find_col_like(df, 'installment', 'period'))

    # SIM registered in own name
    sim_reg_col = (cols_lower.get('number_registered') or
                   find_col_like(df, 'number registered') or
                   find_col_like(df, 'sim', 'registered') or
                   find_col_like(df, 'registered', 'name'))

    # District / location
    dist_col = (cols_lower.get('district') or
                find_col_like(df, 'district') or
                cols_lower.get('village') or
                find_col_like(df, 'village'))

    total = len(df)

    def yes_count(col):
        if not col or col not in df.columns:
            return 0
        s = df[col].astype(str).str.strip().str.lower()
        return int(s.isin(['yes', '1', 'true', 'yes, i do']).sum())

    with_disability = yes_count(disability_col)
    business_reg    = yes_count(biz_reg_col)
    districts       = value_counts_dict(df[dist_col], top_n=20) if dist_col else {}

    # Device types — use binary sub-columns when available (more accurate)
    device_types = {}
    sub_cols = {
        'Smartphone': cols_lower.get('device_type/smartphone'),
        'Tablet':     cols_lower.get('device_type/tablet'),
        'Laptop':     cols_lower.get('device_type/laptop'),
        'POS':        cols_lower.get('device_type/pos'),
        'Software':   cols_lower.get('device_type/software'),
    }
    if any(sub_cols.values()):
        for label, col in sub_cols.items():
            if col and col in df.columns:
                n = int((df[col].astype(str).str.strip() == '1').sum())
                if n: device_types[label] = n
    elif device_type_col:
        # Combined text column (WITU): split on space, count each token
        for row in df[device_type_col].dropna().astype(str).str.strip():
            for part in row.split():
                part = part.strip()
                if part:
                    device_types[part] = device_types.get(part, 0) + 1
        device_types = dict(sorted(device_types.items(), key=lambda x: -x[1])[:8])

    # device_by_eso cross-tab: device type × ESO/Hub (binary sub-columns only)
    device_by_eso = {}
    if any(sub_cols.values()) and eso_hub_col and eso_hub_col in df.columns:
        for label, col in sub_cols.items():
            if col and col in df.columns:
                mask = df[col].astype(str).str.strip() == '1'
                eso_counts = {k: int(v) for k, v in
                              df[mask][eso_hub_col].value_counts().items() if str(k) != 'nan'}
                if eso_counts:
                    device_by_eso[label] = eso_counts

    # SIM registered counts
    sim_registered = value_counts_dict(df[sim_reg_col], top_n=5) if sim_reg_col else {}

    # ── Weekly / monthly temporal analytics ──────────────────────────────────
    weekly_activity = {}
    eso_weekly      = {}   # {eso: {total, this_week, last_week, this_month}}
    device_weekly   = {}   # {device: {total, this_week, this_month}}
    price_stats     = {}

    sub_time_col = next((c for c in df.columns if c.strip().lower() == '_submission_time'), None)
    if sub_time_col:
        dt          = pd.to_datetime(df[sub_time_col], errors='coerce')
        today       = pd.Timestamp.now().normalize()
        week_start  = today - pd.Timedelta(days=today.weekday())
        lweek_start = week_start - pd.Timedelta(weeks=1)
        month_start = today.replace(day=1)

        mask_wk  = dt >= week_start
        mask_lwk = (dt >= lweek_start) & (dt < week_start)
        mask_mo  = dt >= month_start

        weekly_activity = {
            'apps_this_week':  int(mask_wk.sum()),
            'apps_last_week':  int(mask_lwk.sum()),
            'apps_this_month': int(mask_mo.sum()),
        }

        # Per-ESO temporal breakdown
        if eso_hub_col and eso_hub_col in df.columns:
            for eso in df[eso_hub_col].dropna().unique():
                m = df[eso_hub_col] == eso
                eso_weekly[str(eso)] = {
                    'total':      int(m.sum()),
                    'this_week':  int((m & mask_wk).sum()),
                    'last_week':  int((m & mask_lwk).sum()),
                    'this_month': int((m & mask_mo).sum()),
                }

        # Per-device-type temporal breakdown (binary sub-cols)
        if any(sub_cols.values()):
            for label, col in sub_cols.items():
                if col and col in df.columns:
                    md = df[col].astype(str).str.strip() == '1'
                    if md.sum() > 0:
                        device_weekly[label] = {
                            'total':      int(md.sum()),
                            'this_week':  int((md & mask_wk).sum()),
                            'this_month': int((md & mask_mo).sum()),
                        }

    # Price stats (avg + median, exclude 0/null)
    if price_col and price_col in df.columns:
        prices = pd.to_numeric(
            df[price_col].astype(str).str.replace(',', '', regex=False),
            errors='coerce').dropna()
        prices = prices[prices > 0]
        if len(prices) > 0:
            price_stats = {
                'avg':    int(prices.mean()),
                'median': int(prices.median()),
            }

    # Price bands (group raw UGX values)
    price_bands = {}
    if price_col and price_col in df.columns:
        def _price_band(v):
            try:
                v = float(str(v).replace(',', '').strip())
            except Exception:
                return None
            if v <= 0:       return None
            if v < 200_000:  return 'Under 200K'
            if v < 500_000:  return '200K–500K'
            if v < 1_000_000:return '500K–1M'
            if v < 2_000_000:return '1M–2M'
            return '2M+'
        for v in df[price_col].dropna():
            b = _price_band(v)
            if b:
                price_bands[b] = price_bands.get(b, 0) + 1
        order = ['Under 200K', '200K–500K', '500K–1M', '1M–2M', '2M+']
        price_bands = {k: price_bands[k] for k in order if k in price_bands}

    # Payment duration — normalise verbose labels
    payment_duration = {}
    if duration_col and duration_col in df.columns:
        def _norm_dur(v):
            v = str(v).strip().lower()
            if 'quarter' in v or ('3' in v and 'month' in v): return 'Quarterly'
            if 'semi' in v or ('6' in v and 'month' in v):    return 'Semi-Annual'
            if 'bi' in v and 'week' in v: return 'Bi-Weekly'
            if 'week' in v:   return 'Weekly'
            if 'month' in v:  return 'Monthly'
            if 'year' in v or 'annual' in v: return 'Yearly'
            return v.title()[:20]
        for v in df[duration_col].dropna().astype(str):
            b = _norm_dur(v)
            if b:
                payment_duration[b] = payment_duration.get(b, 0) + 1
    elif payment_col and payment_col in df.columns:
        # Outbox uses raw number amounts (instalment size) — not useful for duration
        pass

    # ID types, reg body, ESO hub, disability types
    eso_hubs        = value_counts_dict(df[eso_hub_col],        top_n=10) if eso_hub_col        else {}
    id_types        = value_counts_dict(df[id_type_col],        top_n=8)  if id_type_col        else {}
    reg_body        = value_counts_dict(df[reg_body_col],       top_n=8)  if reg_body_col       else {}
    disability_types= value_counts_dict(df[disability_type_col],top_n=8)  if disability_type_col else {}

    return {
        'type':     'devices',
        'name':     'Device Financing',
        'filename': filename,
        'stats': {
            'total':           total,
            'with_disability': with_disability,
            'business_reg':    business_reg,
            'record_count':    total,
        },
        'disability_pct':    round(with_disability / max(total, 1) * 100, 1),
        'business_reg_pct':  round(business_reg   / max(total, 1) * 100, 1),
        'districts':         districts,
        'device_types':      device_types,
        'price_bands':       price_bands,
        'payment_duration':  payment_duration,
        'eso_hubs':          eso_hubs,
        'id_types':          id_types,
        'reg_body':          reg_body,
        'disability_types':  disability_types,
        'device_by_eso':    device_by_eso,
        'sim_registered':   sim_registered,
        'weekly_activity':  weekly_activity,
        'eso_weekly':       eso_weekly,
        'device_weekly':    device_weekly,
        'price_stats':      price_stats,
    }


def parse_platforms_data():
    """Read all digital platform files from plaforms/ and produce onboarding stats."""
    plaforms_dir = BASE_DIR / 'plaforms'
    if not plaforms_dir.exists():
        print('  plaforms/ directory not found — skipping')
        return None

    print(f'\nProcessing: plaforms/ (Digital Platforms)')
    print('  Detected: platforms')
    try:
        xente_total       = 0
        xente_female      = 0
        xente_male        = 0
        xente_pwd         = 0
        xente_locs        = {}
        stanbic_xente_total = 0
        chapchap_total    = 0
        chapchap_female   = 0
        chapchap_male     = 0
        chapchap_pedn_n   = 0
        flexipay_total    = 0
        flexipay_complete = 0
        ezyagric_total    = 0
        stanbic_xente_monthly   = {}
        chapchap_pedn_districts = {}
        flexipay_fully_reg      = 0
        flexipay_pending        = 0
        ezyagric_items          = 0
        ezyagric_cost           = 0
        ezy_districts           = {}

        # ── XENTE (Xente Tech MSME file — primary Xente source) ──────────────
        xente_f = plaforms_dir / 'Xente MSMEs_Oct- Dec 2025 (1).xlsx'
        if xente_f.exists():
            xl = pd.ExcelFile(xente_f)
            df_raw = xl.parse(xl.sheet_names[0], dtype=str, header=None)
            start = next((i for i, r in df_raw.iterrows()
                          if str(r.iloc[0]).strip().isdigit()), None)
            if start is not None:
                df = df_raw.iloc[start:].dropna(subset=[df_raw.columns[0]])
                xente_total  += len(df)
                sex_vc        = df.iloc[:, 10].value_counts()
                xente_female += int(sex_vc.get('F', 0))
                xente_male   += int(sex_vc.get('M', 0))
                pwd_vc        = df.iloc[:, 13].value_counts()
                xente_pwd    += int(pwd_vc.get('Yes', 0))
                for loc, cnt in df.iloc[:, 4].value_counts().head(10).items():
                    loc = str(loc).strip()
                    if loc and loc != 'nan':
                        xente_locs[loc] = xente_locs.get(loc, 0) + int(cnt)

        # ── XENTE (Stanbic ESO — Oscar + Zipora CSVs) ────────────────────────
        stanbic_xente_dir = plaforms_dir / 'Stanbic' / 'Xente'
        if stanbic_xente_dir.exists():
            for csv_f in sorted(stanbic_xente_dir.glob('*.csv')):
                df = pd.read_csv(csv_f, dtype=str)
                stanbic_xente_total += len(df)
                if 'Onboarded Date' in df.columns:
                    df['_dt'] = pd.to_datetime(df['Onboarded Date'], errors='coerce')
                    for period, cnt in df['_dt'].dt.to_period('M').value_counts().items():
                        k = str(period)
                        stanbic_xente_monthly[k] = stanbic_xente_monthly.get(k, 0) + int(cnt)

        # ── CHAPCHAP (Stanbic CSVs — deduplicated by email) ──────────────────
        seen_cc_emails = set()
        chap_stanbic_dir = plaforms_dir / 'Stanbic' / 'Chap Chap'
        if chap_stanbic_dir.exists():
            for csv_f in sorted(chap_stanbic_dir.glob('*.csv')):
                df = pd.read_csv(csv_f, dtype=str)
                email_col = next((c for c in df.columns
                                  if 'email' in c.lower()), None)
                if email_col:
                    new = df[~df[email_col].isin(seen_cc_emails)]
                    seen_cc_emails.update(df[email_col].dropna())
                    chapchap_total += len(new)
                else:
                    chapchap_total += len(df)

        # ── CHAPCHAP (PEDN Evidence file) ─────────────────────────────────────
        cc_pedn = (plaforms_dir / 'Platform_Chap-chap and Xent'
                   / 'Chap_chap' / 'Evidence - PEDN.xlsx')
        if cc_pedn.exists():
            xl = pd.ExcelFile(cc_pedn)
            df_raw = xl.parse(xl.sheet_names[0], dtype=str, header=None).dropna(how='all')
            # Structure: col 0 = blank, col 1 = _id, col 5 = Gender, data from row 2
            if len(df_raw) > 2:
                df = df_raw.iloc[2:].copy()
                df.columns = range(df.shape[1])
                df = df.dropna(subset=[1])   # col 1 holds _id
                chapchap_pedn_n  = len(df)
                chapchap_total  += chapchap_pedn_n
                if df.shape[1] > 5:
                    vc = df.iloc[:, 5].value_counts()
                    chapchap_female += int(vc.get('F', 0))
                    chapchap_male   += int(vc.get('M', 0))
                if df.shape[1] > 7:
                    dvc = df.iloc[:, 7].dropna().astype(str).str.strip()
                    dvc = dvc[dvc.str.lower() != 'nan']
                    for dist, cnt in dvc.value_counts().items():
                        chapchap_pedn_districts[dist] = chapchap_pedn_districts.get(dist, 0) + int(cnt)

        # ── FLEXIPAY (Stanbic) ────────────────────────────────────────────────
        flexipay_dir = plaforms_dir / 'Stanbic' / 'Flexipay'
        if flexipay_dir.exists():
            for xlsx_f in flexipay_dir.glob('*.xlsx'):
                xl = pd.ExcelFile(xlsx_f)
                df_raw = xl.parse(xl.sheet_names[0], dtype=str, header=None).dropna(how='all')
                if len(df_raw) > 1:
                    df = df_raw.iloc[1:].copy()
                    df.columns = range(df.shape[1])
                    df = df[df.iloc[:, 0].notna() &
                            (df.iloc[:, 0].astype(str).str.strip() != 'nan')]
                    flexipay_total += len(df)
                    if df.shape[1] > 2:
                        vc = df.iloc[:, 2].value_counts()
                        flexipay_complete += int(vc.get('Completed', 0))
                    if df.shape[1] > 3:
                        reg_vc = df.iloc[:, 3].astype(str).str.strip().value_counts()
                        flexipay_fully_reg += int(reg_vc.get('Fully Registered', 0))
                        flexipay_pending   += int(df.iloc[:, 3].astype(str).str.contains('Pending', na=False).sum())

        # ── EZYAGRIC (PEDN Training — "Onboarded on the App" sheet) ──────────
        ezy_f = (plaforms_dir / 'Platform_Chap-chap and Xent'
                 / 'EzyAgric' / '10X TRAINING DATA_PEDN.xlsx')
        if ezy_f.exists():
            xl = pd.ExcelFile(ezy_f)
            target_sh = next(
                (s for s in xl.sheet_names if 'onboard' in s.lower() or 'app' in s.lower()),
                xl.sheet_names[0]
            )
            df_raw = xl.parse(target_sh, dtype=str, header=None).dropna(how='all')
            if len(df_raw) > 2:
                df = df_raw.iloc[2:].copy()
                df.columns = range(df.shape[1])
                df = df[df.iloc[:, 0].astype(str).str.match(r'^[A-Z]{2}-\d+')]
                ezyagric_total = len(df)
                if df.shape[1] > 7:
                    ezyagric_items = int(pd.to_numeric(df.iloc[:, 6], errors='coerce').fillna(0).sum())
                    ezyagric_cost  = int(pd.to_numeric(df.iloc[:, 7], errors='coerce').fillna(0).sum())
                if df.shape[1] > 3:
                    dvc = df.iloc[:, 3].dropna().astype(str).str.strip()
                    dvc = dvc[dvc.str.lower() != 'nan']
                    for dist, cnt in dvc.value_counts().items():
                        ezy_districts[dist] = int(cnt)

        # ── Totals ─────────────────────────────────────────────────────────────
        xente_combined    = xente_total + stanbic_xente_total
        total_onboardings = xente_combined + chapchap_total + flexipay_total + ezyagric_total
        total_female      = xente_female + chapchap_female
        total_male        = xente_male   + chapchap_male

        by_platform = {
            'Xente (PEDN/Xente Tech)': {
                'total':     xente_total,
                'female':    xente_female,
                'male':      xente_male,
                'pwd':       xente_pwd,
                'locations': dict(
                    sorted(xente_locs.items(), key=lambda x: x[1], reverse=True)[:8]
                ),
            },
            'Xente (Stanbic)': {
                'total':   stanbic_xente_total,
                'monthly': dict(sorted(stanbic_xente_monthly.items())),
            },
            'ChapChap': {
                'total':         chapchap_total,
                'female':        chapchap_female,
                'male':          chapchap_male,
                'pedn':          chapchap_pedn_n,
                'stanbic':       chapchap_total - chapchap_pedn_n,
                'pedn_districts': dict(sorted(chapchap_pedn_districts.items(), key=lambda x: x[1], reverse=True)),
            },
            'FlexiPay': {
                'total':              flexipay_total,
                'completed':          flexipay_complete,
                'completion_rate':    round(flexipay_complete / max(flexipay_total, 1) * 100, 1),
                'fully_registered':   flexipay_fully_reg,
                'pending_validation': flexipay_pending,
            },
            'EzyAgric': {
                'total':          ezyagric_total,
                'total_items':    ezyagric_items,
                'total_cost_ugx': ezyagric_cost,
                'avg_items':      round(ezyagric_items / max(ezyagric_total, 1), 2),
                'avg_cost_ugx':   round(ezyagric_cost  / max(ezyagric_total, 1)),
                'districts':      dict(sorted(ezy_districts.items(), key=lambda x: x[1], reverse=True)),
            },
        }

        print(f'  OK \u2014 "Digital Platforms"  ({total_onboardings:,} records)')
        return {
            'type':     'platforms',
            'name':     'Digital Platforms',
            'filename': 'plaforms/',
            'stats': {
                'total':        total_onboardings,
                'record_count': total_onboardings,
            },
            'by_platform':    by_platform,
            'xente_combined': xente_combined,
            'total_female':   total_female,
            'total_male':     total_male,
            'female_pct':     round(total_female / max(total_female + total_male, 1) * 100, 1),
        }

    except Exception as exc:
        print(f'  ERROR reading platform files: {exc}')
        traceback.print_exc()
        return None


def parse_foundation_data():
    """Read Foundation_Merged.xlsx and produce per-ESO and module stats."""
    fpath = BASE_DIR / 'Foundation' / 'Foundation_Merged.xlsx'
    if not fpath.exists():
        print('  Foundation_Merged.xlsx not found — skipping')
        return None

    print(f'\nProcessing: Foundation/Foundation_Merged.xlsx')
    print('  Detected: foundation')
    try:
        xl  = pd.ExcelFile(fpath)
        df  = xl.parse(xl.sheet_names[0])
        df.columns = df.columns.str.strip()

        pct_col  = find_col(df, '% Completed', '% completed')
        cert_col = find_col(df, 'Has_Certificate', 'Has Certificate',
                            'Has Certificate (Yes / No)')
        eso_col  = find_col(df, 'ESO_Name', 'ESO Name')
        enroll_col   = find_col(df, 'Enrollment_Date', 'Enrollment Date')
        complete_col = find_col(df, 'Completed_Date', 'Completed Date', 'Completed At')

        pct_num         = pd.to_numeric(df[pct_col], errors='coerce') if pct_col else pd.Series(dtype=float)
        completed_count = int((pct_num >= 100).sum()) if pct_col else 0
        avg_completion  = round(pct_num.mean(), 1) if pct_col and len(pct_num.dropna()) > 0 else 0.0

        certified_count = 0
        if cert_col:
            certified_count = int(df[cert_col].astype(str).str.strip().str.lower().isin(['yes']).sum())

        total = len(df)

        # Date helpers for weekly/monthly windowing
        enroll_dates  = pd.to_datetime(df[enroll_col],   errors='coerce') if enroll_col   else pd.Series(dtype='datetime64[ns]')
        complete_dates= pd.to_datetime(df[complete_col], errors='coerce') if complete_col else pd.Series(dtype='datetime64[ns]')
        today        = pd.Timestamp.now().normalize()
        week_start   = today - pd.Timedelta(days=today.weekday())   # Monday
        lweek_start  = week_start  - pd.Timedelta(days=7)
        lweek_end    = week_start  - pd.Timedelta(days=1)
        month_start  = today.replace(day=1)

        weekly_activity = {
            'enrolled_this_week':   int((enroll_dates   >= week_start).sum()),
            'enrolled_last_week':   int(((enroll_dates  >= lweek_start) & (enroll_dates  <= lweek_end)).sum()),
            'enrolled_this_month':  int((enroll_dates   >= month_start).sum()),
            'completed_this_week':  int((complete_dates >= week_start).sum()),
            'completed_last_week':  int(((complete_dates>= lweek_start) & (complete_dates<= lweek_end)).sum()),
            'completed_this_month': int((complete_dates >= month_start).sum()),
        }

        # Progress bands
        progress_bands = {}
        if pct_col:
            progress_bands = {
                'Completed (100%)': int((pct_num >= 100).sum()),
                '75 \u2013 99%':   int(((pct_num >= 75) & (pct_num < 100)).sum()),
                '50 \u2013 74%':   int(((pct_num >= 50) & (pct_num < 75)).sum()),
                '25 \u2013 49%':   int(((pct_num >= 25) & (pct_num < 50)).sum()),
                'Under 25%':       int((pct_num < 25).sum()),
            }

        by_eso = {}
        if eso_col:
            for eso, grp in df.groupby(eso_col):
                eso = str(eso).strip()
                if not eso or eso == 'nan':
                    continue
                grp_pct   = pd.to_numeric(grp[pct_col], errors='coerce') if pct_col else pd.Series(dtype=float)
                grp_cert  = (grp[cert_col].astype(str).str.lower().isin(['yes'])
                             if cert_col else pd.Series([False] * len(grp)))
                grp_comp_d= complete_dates.loc[grp.index]
                grp_enr_d = enroll_dates.loc[grp.index]
                n_comp    = int((grp_pct >= 100).sum()) if pct_col else 0
                by_eso[eso] = {
                    'total':           len(grp),
                    'completed':       n_comp,
                    'certified':       int(grp_cert.sum()),
                    'avg_pct':         round(grp_pct.mean(), 1) if len(grp_pct.dropna()) > 0 else 0.0,
                    'completion_rate': round(n_comp / max(len(grp), 1) * 100, 1),
                    'comp_this_week':  int((grp_comp_d >= week_start).sum()),
                    'comp_last_week':  int(((grp_comp_d >= lweek_start) & (grp_comp_d <= lweek_end)).sum()),
                    'comp_this_month': int((grp_comp_d >= month_start).sum()),
                    'enr_this_week':   int((grp_enr_d  >= week_start).sum()),
                    'enr_this_month':  int((grp_enr_d  >= month_start).sum()),
                }

        # Module completion (values are numeric — 100.0 = completed)
        mod_cols = [c for c in df.columns
                    if c.startswith(('Welcome', 'Module', 'Next steps', 'module'))]
        modules  = {}
        for mc in mod_cols:
            mc_num      = pd.to_numeric(df[mc], errors='coerce')
            completed_n = int((mc_num >= 100).sum())
            if completed_n > 0:
                # Shorten label: strip common prefix
                label = mc.strip()
                label = label.replace('Module ', 'M').replace(' - ', ': ')
                label = label[:50]
                modules[label] = {
                    'completed': completed_n,
                    'pct':       round(completed_n / max(total, 1) * 100, 1),
                }

        print(f'  OK \u2014 "Foundation Course"  ({total:,} records)')
        return {
            'type':     'foundation',
            'name':     'Foundation Course',
            'filename': 'Foundation_Merged.xlsx',
            'stats': {
                'total':        total,
                'completed':    completed_count,
                'certified':    certified_count,
                'record_count': total,
            },
            'completed_pct':  round(completed_count / max(total, 1) * 100, 1),
            'certified_pct':  round(certified_count / max(total, 1) * 100, 1),
            'avg_completion': avg_completion,
            'by_eso':         by_eso,
            'modules':        modules,
            'progress_bands': progress_bands,
            'weekly_activity':weekly_activity,
        }
    except Exception as exc:
        print(f'  ERROR reading Foundation_Merged.xlsx: {exc}')
        traceback.print_exc()
        return None


def main():
    print()
    print('Portfolio Data Extractor')
    print('=' * 44)

    portfolios = []

    # Collect .xlsx files from all configured portfolio directories.
    # Skip Excel temp/lock files (starting with '~$') and deduplicate by resolved path.
    seen = set()
    xlsx_files = []
    for folder in PORTFOLIO_DIRS:
        if not folder.exists():
            continue
        for f in sorted(folder.glob('*.xlsx')):
            if f.name.startswith('~$'):
                continue
            resolved = f.resolve()
            if resolved not in seen:
                seen.add(resolved)
                xlsx_files.append(f)

    if not xlsx_files:
        dirs_str = ', '.join(str(d) for d in PORTFOLIO_DIRS)
        print(f'No .xlsx files found in: {dirs_str}')
        return

    for filepath in xlsx_files:
        filename = filepath.name
        rel_path = filepath.relative_to(BASE_DIR)
        print(f'\nProcessing: {rel_path}')
        try:
            xl    = pd.ExcelFile(filepath)
            ftype = detect_file_type(xl)
            if ftype == 'segmentation':
                data = parse_segmentation_file(filename, xl)
            elif ftype == 'eoi':
                data = parse_eoi_file(filename, xl)
            elif ftype == 'yiw':
                data = parse_yiw_file(filename, xl)
            elif ftype == 'buz_needs':
                data = parse_buz_needs_file(filename, xl)
            elif ftype == 'devices':
                data = parse_devices_file(filename, xl)
            else:
                data = parse_growth_plans_file(filename, xl)
            if data:
                portfolios.append(data)
                total = data['stats']['total']
                print(f'  OK — "{data["name"]}"  ({total:,} records)')
        except Exception as exc:
            print(f'  ERROR: {exc}')
            traceback.print_exc()

    # Foundation Course data (derived file — handled separately)
    foundation = parse_foundation_data()
    if foundation:
        portfolios.append(foundation)

    # Digital Platforms data (multi-file — handled separately)
    platforms = parse_platforms_data()
    if platforms:
        portfolios.append(platforms)

    if not portfolios:
        print('\nNo portfolios extracted. Check file formats.')
        return

    output = {
        'generated':  pd.Timestamp.now().strftime('%d %b %Y, %H:%M'),
        'portfolios': portfolios,
    }

    js_content = 'window.PORTFOLIO_DATA = ' + json.dumps(output, indent=2, default=str) + ';\n'
    out_path = BASE_DIR / 'data.js'
    out_path.write_text(js_content, encoding='utf-8')

    total_records = sum(p['stats']['total'] for p in portfolios)
    print()
    print('=' * 44)
    print(f'data.js written  —  {len(portfolios)} portfolios  |  {total_records:,} total records')
    print('Open dashboard.html in your browser to explore.')
    print()


if __name__ == '__main__':
    main()
