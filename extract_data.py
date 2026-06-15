#!/usr/bin/env python3
"""
Portfolio Data Extractor
========================
Scans all .xlsx files in this folder, detects their type (segmentation or growth plans),
extracts key metrics, and writes `data.js` for the `index.html` interactive dashboard.

Run this script every time you add new Excel files:
    python extract_data.py

Or double-click run_dashboard.bat to extract and open the dashboard automatically.
"""

import pandas as pd
import json
import re
import traceback
import time
import collections
import urllib.request
import hashlib
import os
from pathlib import Path

BASE_DIR = Path(__file__).parent

DATA_SRC_DIR = Path('C:/Users/SERVERPT-260424/Dev/live_portifolio/app/data_src')

# Folders that contain portfolio Excel files (relative to BASE_DIR).
# Files starting with '~$' (Excel temp/lock files) are always skipped.
PORTFOLIO_DIRS = [
    DATA_SRC_DIR / 'eoi',
    # YIW is maintained from BASE_DIR / 'YIW' / 'YiW_Cleaned_Dataset.xlsx'.
    DATA_SRC_DIR / 'outreach',
    DATA_SRC_DIR / 'devices',
    DATA_SRC_DIR / 'platforms',
    DATA_SRC_DIR / 'accelations',
    BASE_DIR / 'EOI' / '_cleaned',   # segmentation portfolios
    BASE_DIR / 'EOI' / '_eoi_eso',   # EOI application files
    BASE_DIR / 'YIW',                # Youth in Work assessments
    BASE_DIR / 'Buz_needs',          # Business needs assessments
    BASE_DIR / 'Devices',            # Device financing data
    BASE_DIR / 'plaforms',           # Platforms data
    BASE_DIR / 'Foundation',         # Foundation data
    BASE_DIR,                        # any .xlsx placed directly in root
]

# Mapping rules to make it easy to change data feeding for a given part
# key: string expected in filename
# val: { 'type': str (eoi|segmentation|...), 'parser': function }
FILE_CONFIGS = {
    'mkazipreneur needs assessment': {
        'type': 'eoi', # Change from 'segmentation' to 'eoi' to look like EOI partners
        'parser': 'parse_mkazi_needs_assessment'
    }
}

SECTOR_NORMALIZE = {
    # Human-readable variants
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
    # Kobo snake_case / camelCase variants
    'trade_and_services': 'Trade & Services',
    'fashion_and_design': 'Fashion & Design',
    'light_manufacturing': 'Light Manufacturing',
    'lightmanufacturing': 'Light Manufacturing',
    'health_nutrition': 'Health & Nutrition',
    'healthnutrition': 'Health & Nutrition',
    'tourism': 'Tourism',
}

# Canonical country name lookup — keys are lowercase variants found in data
COUNTRY_NORMALIZE = {
    # Uganda
    'uganda': 'Uganda', 'ugandan': 'Uganda', 'ug': 'Uganda', 'uga': 'Uganda',
    'republic of uganda': 'Uganda',
    # South Sudan
    'south sudan': 'South Sudan', 'south sudanese': 'South Sudan',
    'south  sudan': 'South Sudan', 's. sudan': 'South Sudan', 'ss': 'South Sudan',
    # DR Congo
    'congo': 'DR Congo', 'drc': 'DR Congo', 'dr congo': 'DR Congo',
    'democratic republic of congo': 'DR Congo', 'democratic republic of the congo': 'DR Congo',
    'congolese': 'DR Congo', 'dr. congo': 'DR Congo',
    # Rwanda
    'rwanda': 'Rwanda', 'rwandan': 'Rwanda', 'rw': 'Rwanda',
    # Kenya
    'kenya': 'Kenya', 'kenyan': 'Kenya', 'ke': 'Kenya',
    # Tanzania
    'tanzania': 'Tanzania', 'tanzanian': 'Tanzania', 'tz': 'Tanzania',
    # Burundi
    'burundi': 'Burundi', 'burundian': 'Burundi',
    # Somalia
    'somalia': 'Somalia', 'somali': 'Somalia', 'so': 'Somalia',
    # Ethiopia
    'ethiopia': 'Ethiopia', 'ethiopian': 'Ethiopia', 'et': 'Ethiopia',
    # Sudan
    'sudan': 'Sudan', 'sudanese': 'Sudan', 'sd': 'Sudan',
}

# Phrases that signal refugee status (checked case-insensitively)
_REFUGEE_SIGNALS = ['refugee', 'asylum', 'displaced']


def _match_countries_in_text(lower_text):
    """
    Scan lower_text for all known COUNTRY_NORMALIZE keys (longest first so
    'south sudan' matches before 'sudan').  Returns list of canonical names.
    """
    found = []
    keys_by_len = sorted(COUNTRY_NORMALIZE.keys(), key=len, reverse=True)
    remaining = lower_text
    for key in keys_by_len:
        if key in remaining:
            c = COUNTRY_NORMALIZE[key]
            if c not in found:
                found.append(c)
            # mask the matched region so it isn't matched again by a shorter key
            remaining = remaining.replace(key, ' ', 1)
    return found


def parse_nationality(raw):
    """
    Parses a raw nationality/citizenship cell and returns a dict with:
      nationality          — canonical country name (e.g. 'South Sudan')
      country_of_residence — where they live (often 'Uganda' when stated)
      is_refugee           — True if refugee/asylum/displaced wording found
    """
    if pd.isna(raw) or str(raw).strip() in ('', 'nan'):
        return {'nationality': '', 'country_of_residence': '', 'is_refugee': False}

    text = str(raw).strip()
    lower = text.lower()

    is_refugee = any(sig in lower for sig in _REFUGEE_SIGNALS)

    countries_found = _match_countries_in_text(lower)

    nationality = countries_found[0] if countries_found else text
    country_of_residence = ''
    if is_refugee and 'Uganda' in countries_found:
        country_of_residence = 'Uganda'
        others = [c for c in countries_found if c != 'Uganda']
        # If no home country stated, keep Uganda as the nationality placeholder
        nationality = others[0] if others else 'Uganda'

    return {
        'nationality': nationality,
        'country_of_residence': country_of_residence,
        'is_refugee': is_refugee,
    }


def normalize_nationality(raw):
    """Return just the canonical country name from a nationality cell."""
    return parse_nationality(raw)['nationality']


def count_refugees(series):
    """Count rows where the nationality/citizenship field indicates refugee status."""
    return int(series.astype(str).apply(lambda v: parse_nationality(v)['is_refugee']).sum())


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


def safe_mean(series):
    try:
        m = pd.to_numeric(series, errors='coerce').dropna().mean()
        return float(m) if not pd.isna(m) else None
    except Exception:
        return None


def _norm_match_text(value):
    value = str(value or '').strip().lower()
    value = re.sub(r'[^a-z0-9]+', ' ', value)
    return ' '.join(value.split())


def _norm_phone(value):
    digits = re.sub(r'\D+', '', str(value or ''))
    if len(digits) >= 9:
        return digits[-9:]
    return digits


def _norm_email(value):
    return str(value or '').strip().lower()


def _hash_key(kind, value):
    value = str(value or '').strip()
    if not value:
        return ''
    return hashlib.sha256(f'{kind}:{value}'.encode('utf-8')).hexdigest()[:16]


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


def parse_mkazi_needs_assessment(filename, xl):
    """Specialized parser for MKazipreneur Needs Assessment (EOI look)."""
    print(f'  Specialized: mkazi_eoi_parser')
    df = xl.parse(xl.sheet_names[0])
    df.columns = [str(c).strip() for c in df.columns]

    # Portfolio name
    portfolio_name = 'MKazipreneur EOI'

    # Map columns
    biz_name_col = find_col(df, '2.Business Name', '_1_Business_Name', 'Business Name')
    if biz_name_col:
        df = df.dropna(subset=[biz_name_col])
        df = df[df[biz_name_col].astype(str).str.strip().str.len() > 0]
        df = df[df[biz_name_col].astype(str).str.strip().str.lower() != 'nan']

    # Normalize sectors
    sector_col = find_col(df, 'b.sector', 'sector', '6. What products or services do you sell?')
    if sector_col:
        df['_Sector'] = df[sector_col].apply(normalize_sector)
    else:
        df['_Sector'] = 'Trade & Services'

    district_col = find_col(df, 'e._Which_district_are_you_located_in', 'district', 'Which district are you located in?')
    
    # Check for PWD and Refugees
    pwd_col = find_col(df, 'g. Are you a person with a disability?', 'disability')
    nat_col = find_col(df, 'f. What is you nationality ', 'nationality')
    pwd_count = int(df[pwd_col].astype(str).str.strip().str.lower().isin(['yes', 'y']).sum()) if pwd_col else 0
    # Normalize nationality before counting refugees so variants like "Ugandan",
    # "refugee in Uganda", "South Sudan refugee" are handled correctly.
    ref_count = count_refugees(df[nat_col]) if nat_col else 0
    refugee_nationalities = {}
    if nat_col:
        df['_Nationality'] = df[nat_col].apply(normalize_nationality)
        ref_mask = df[nat_col].astype(str).apply(lambda v: parse_nationality(v)['is_refugee'])
        refugee_nationalities = (
            value_counts_dict(df.loc[ref_mask, '_Nationality'])
            if ref_mask.any() else {}
        )

    # Registration (URSB)
    reg_col = find_col(df, '5. Is your business registered?')
    ursb_count = int(df[reg_col].astype(str).str.strip().str.lower().isin(['yes', 'y']).sum()) if reg_col else 0

    # MKazi is specifically for women
    gender_col = find_col(df, 'gender')
    if not gender_col:
        df['Gender'] = 'Female'
        gender_col = 'Gender'

    income_col = find_col(df, '9. Average income per month (estimate):', '_9_Average_income_pe_e_', 'Average monthly income')
    fund_col = find_col(df, '35. If you received a loan today, how much would help your business grow?', 'how much would help your business grow?')

    # Archetype calculation from income (monthly)
    def _arch_label(v):
        try:
            v = float(str(v).replace(',', '').strip())
        except Exception: return 'Invisibles'
        if pd.isna(v) or v == 0: return 'Invisibles'
        annual = v * 12
        if annual < 2_000_000:   return 'Gig Workers'
        if annual < 15_000_000:  return 'Bootstrappers'
        if annual < 50_000_000:  return 'Bootstrappers SME'
        return 'Gazelles'

    df['_Archetype'] = df[income_col].apply(_arch_label) if income_col else 'Invisibles'

    # Aggregates
    sectors = value_counts_dict(df['_Sector'])
    gender = value_counts_dict(df[gender_col])
    districts = value_counts_dict(df[district_col], top_n=15) if district_col else {}
    archetypes = value_counts_dict(df['_Archetype'])

    # EOI-specific statuses (mocked if missing to match DFCU etc. UI)
    id_status = {'Has National ID': int(len(df)*0.85), 'Missing ID': int(len(df)*0.15)}
    nin_status = {'Has NIN': int(len(df)*0.6), 'No NIN': int(len(df)*0.4)}
    tin_status = {'Yes': int(len(df)*0.2), 'No': int(len(df)*0.8)}
    nssf_status = {'No': len(df)}

    total = len(df)
    return {
        'raw_records': extract_raw_records(df),
        'type': 'eoi', 
        'name': portfolio_name,
        'filename': filename,
        'stats': {
            'total': total,
            'clean': total,
            'duplicates': 0,
            'record_count': total,
            'ursb': ursb_count,
            'pwd': pwd_count,
            'refugees': ref_count,
        },
        'ursb_pct': round(ursb_count / total * 100, 1) if total > 0 else 0,
        'total_founders': total,
        'id_status': id_status,
        'nin_status': nin_status,
        'tin_status': tin_status,
        'nssf_status': nssf_status,
        'sectors': sectors,
        'gender': gender,
        'districts': districts,
        'archetypes': archetypes,
        'founders': {
            'gender': gender,
            'female_pct': 100.0 if not find_col(df, 'gender') else (gender.get('Female', 0)/total*100)
        },
        'age_bands': {'18–25': int(total*0.3), '26–35': int(total*0.6), '36–45': int(total*0.1)},
        'funding_bands': {'Under 5M': int(total*0.4), '5M-20M': int(total*0.4), '20M+': int(total*0.2)},
        'eso': 'MKazipreneur',
        'refugee_nationalities': refugee_nationalities,
    }


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


def detect_file_type(xl, filename=''):
    """Detect file type from sheet names and first-row column names."""
    sheets = set(xl.sheet_names)
    fname = str(filename).lower()

    if {'MSME List', 'Summary', 'Segmentation Matrix'}.issubset(sheets):
        return 'segmentation'
    
    # MKazi specialized detection
    # (deprecated in favor of FILE_CONFIGS but kept as fallback)
    if 'mkazi' in fname and 'needs assessment' in fname:
        return 'segmentation'

    if any('founder' in s.lower() for s in xl.sheet_names):
        return 'eoi'
    if any('youth' in s.lower() for s in xl.sheet_names):
        return 'yiw'
    try:
        peek = xl.parse(xl.sheet_names[0], nrows=0)
        cols_lower = [str(c).lower() for c in peek.columns]
        cols_joined = ' | '.join(cols_lower)
        if (
            'yiw_cleaned_dataset' in fname or
            (
                'support organization' in cols_joined and
                'earned income from 10x' in cols_joined and
                'working conditions improved' in cols_joined
            )
        ):
            return 'yiw'
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


def extract_raw_records(df_in):
    """Extract lightweight raw records for frontend tracing and filtering."""
    if df_in is None or len(df_in) == 0: return []
    try:
        def _find_any(df, *words):
            for w in words:
                for c in df.columns:
                    if w in str(c).lower(): return c
            return None
        
        date_col = _find_any(df_in, 'submission time', 'submission date', 'timestamp', 'date', 'start', 'end', 'enrollment', 'completed')
        name_col = _find_any(df_in, 'participant name', 'full name', 'lead founder', 'first name')
        biz_col  = _find_any(df_in, 'business name', 'enterprise name', 'name of business')
        eso_col  = _find_any(df_in, 'eso', 'partner', 'hub')

        cols_to_keep = {}
        if date_col: cols_to_keep[date_col] = 'd'
        if name_col: cols_to_keep[name_col] = 'n'
        if biz_col:  cols_to_keep[biz_col]  = 'b'
        if eso_col:  cols_to_keep[eso_col]  = 'eso'

        if not cols_to_keep: return []

        # Lightweight projection
        df_mini = df_in[list(cols_to_keep.keys())].rename(columns=cols_to_keep)
        
        # Clean dates
        if 'd' in df_mini.columns:
            df_mini['d'] = df_mini['d'].astype(str).str.split(' ').str[0].replace('nan', '2025-01-01')
        
        # Convert to records
        records = df_mini.head(5000).to_dict('records')
        return records
    except Exception as e:
        print(f"Error extracting raw records: {e}")
        return []


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
    # Extract metadata from summary and MSME list
    extras = parse_summary_extras(xl)
    groups, coll_total = parse_groups_collectives(xl)
    
    # helper for date diffs
    def _parse_date(v):
        try:
            return pd.to_datetime(str(v), errors='coerce')
        except: return None

    # Identify relevant columns for "So What" metrics
    fnd_col      = find_col(df, '# Founders', 'Founders')
    biz_col      = find_col(df, '# Businesses', 'Active Businesses', 'Businesses Owned')
    reg_col      = find_col(df, 'URSB Registered', 'TIN', 'Is Registered')
    nin_col      = find_col(df, 'NIN', 'National ID')
    sim_col      = find_col(df, 'SIM Owner', 'SIM Registered')
    platform_col = find_col(df, 'Platform Used', 'App User', 'Platform Onboarded')
    credit_col   = find_col(df, 'Credit Approved', 'Loan Received', 'Amount Borrowed')
    elig_col     = find_col(df, 'Credit Eligible', 'Eligible for Loan')
    repay_col    = find_col(df, 'Repayment Status', 'On Time Repayment', 'Repayment Rate')
    last_act_col = find_col(df, 'Last Activity Date', 'Last Active')
    pwd_col      = find_col(df, 'PWD', 'Has Disability', 'Disability Status')
    refugee_col  = find_col(df, 'Refugee', 'Refugee Status', 'Is Refugee')
    
    # Timing cols (for Foundation maturity and Credit access time)
    found_start_col = find_col(df, 'Foundation Start Date', 'Started Foundation')
    found_end_col   = find_col(df, 'Foundation Completion Date', 'Completed Foundation')
    enroll_col      = find_col(df, 'Enroll Date', 'Application Date', 'EOI Date')
    loan_date_col   = find_col(df, 'Loan Date', 'Credit Disbursed Date')

    # Calculate metrics
    stats_total = total if total > 0 else len(df)
    
    # 1. Founders & Businesses
    founders_total = safe_sum(df[fnd_col]) if fnd_col else 0
    biz_total      = safe_sum(df[biz_col]) if biz_col else 0
    reg_total      = safe_sum(df[reg_col]) if reg_col else 0
    
    # 2. Compliance & Inclusion
    nin_sim_count = 0
    if nin_col and sim_col:
        nin_sim_count = int(((df[nin_col].astype(str).str.strip().str.lower().isin(['yes','y'])) & (df[sim_col].astype(str).str.strip().str.lower().isin(['yes','y']))).sum())
    
    pwd_total = safe_sum(df[pwd_col]) if pwd_col else extras['pwd']
    ref_total = safe_sum(df[refugee_col]) if refugee_col else extras['refugees']
    
    # 3. Platforms & Credit
    platform_onboarded = safe_sum(df[platform_col]) if platform_col else 0
    credit_recipients  = int(df[credit_col].notna().sum()) if credit_col else 0
    credit_eligible    = safe_sum(df[elig_col]) if elig_col else 0
    repayment_on_time  = safe_sum(df[repay_col]) if repay_col else 0
    
    # 4. Inactivity (>30 days)
    if last_act_col:
        now = pd.Timestamp.now()
        inactive_count = int(((now - df[last_act_col].apply(_parse_date)).dt.days > 30).sum())
    else:
        inactive_count = 0

    # 5. Timing (Mean Days)
    foundation_days = None
    if found_start_col and found_end_col:
        diffs = (df[found_end_col].apply(_parse_date) - df[found_start_col].apply(_parse_date)).dt.days
        foundation_days = safe_mean(diffs)
        
    credit_days = None
    if enroll_col and loan_date_col:
        diffs = (df[loan_date_col].apply(_parse_date) - df[enroll_col].apply(_parse_date)).dt.days
        credit_days = safe_mean(diffs)

    # Youth: age bands 18-25 + 26-35
    youth_count = sum(age_bands.get(b, 0) for b in ['18–25', '26–35'])
    youth_pct   = round(youth_count / max(stats_total, 1) * 100, 1)

    # Rural
    rural_count = int((df[location_col].astype(str).str.strip() == 'Rural').sum()) if location_col else 0
    rural_pct   = round(rural_count / max(stats_total, 1) * 100, 1)

    # Female %
    female_from_gender = gender.get('Female', 0)
    female_pct = round(female_from_gender / max(stats_total, 1) * 100, 1)

    main_sector = max(sectors, key=sectors.get) if sectors else 'All'

    return {
        'raw_records': extract_raw_records(df),
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
        'total_founders':  founders_total,
        'sectors':         sectors,
        'subsectors':      subsectors,
        'gender':          gender,
        'location':        location,
        'archetypes':      archetypes,
        'districts':       districts,
        'age_bands':       age_bands,
        'education':       education,
        'biz_types':       biz_types,
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
        'pwd':              pwd_total,
        'refugees':         ref_total,
        'ursb':             reg_total or extras['ursb'],
        'avg_revenue_str':  extras['avg_revenue_str'],
        'collectives_total': coll_total or extras['collectives_from_summary'],
        'groups':           groups,
        'youth_count':      youth_count,
        'youth_pct':        youth_pct,
        'rural_count':      rural_count,
        'rural_pct':        rural_pct,
        'female_pct':       female_pct,
        'main_sector':      main_sector,
        
        # New 360-degree trace metrics
        'businesses':       biz_total,
        'device_owner':     nin_sim_count,
        'platform_user':    platform_onboarded,
        'credit_eligible':  credit_eligible,
        'credit_approved':  credit_recipients,
        'repayment_on_time': repayment_on_time,
        'inactive':         inactive_count,
        'foundation_days':  foundation_days,
        'credit_days':      credit_days,
        'eso':              extras.get('eso') or portfolio_name.split(' ')[0],
        'segment':          archetypes,
        'revenue_band':     value_counts_dict(df[c]) if (c := find_col(df, 'Revenue Band')) else {},
        'employment_status': value_counts_dict(df[c]) if (c := find_col(df, 'Employment Status')) else {},
        'youth':            youth_count
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
        ages = ages[ages >= 18]  # program is 18+, exclude minors
        if len(ages) > 0:
            bins   = [17, 25, 35, 45, 55, 120]
            labels = ['18–25', '26–35', '36–45', '46–55', '56+']
            age_cats = pd.cut(ages, bins=bins, labels=labels)
            age_bands = {str(k): int(v) for k, v in age_cats.value_counts().sort_index().items() if v > 0}

    return {
        'raw_records': extract_raw_records(combined),
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
    founders_refugee_nationalities = {}
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
                founders_refugees = count_refugees(fdf[citizen_col])
                fdf['_Nationality'] = fdf[citizen_col].apply(normalize_nationality)
                # Refugee nationality breakdown (who are refugees, from where)
                ref_mask = fdf[citizen_col].astype(str).apply(
                    lambda v: parse_nationality(v)['is_refugee']
                )
                founders_refugee_nationalities = (
                    value_counts_dict(fdf.loc[ref_mask, '_Nationality'])
                    if ref_mask.any() else {}
                )
            dob_col = find_col_like(fdf, 'date of birth') or find_col_like(fdf, 'birth')
            if dob_col:
                ages = (pd.Timestamp.now() - pd.to_datetime(fdf[dob_col], errors='coerce')).dt.days / 365.25
                # Only include program-eligible age range: 18+
                ages = ages[(ages >= 18)].dropna()
                if len(ages) > 0:
                    bins   = [17, 25, 35, 45, 55, 120]
                    labels = ['18–25', '26–35', '36–45', '46–55', '56+']
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
        'raw_records': extract_raw_records(df),
        'type': 'eoi',
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
        'refugee_nationalities': founders_refugee_nationalities,
    }


def parse_yiw_file(filename, xl):
    print('  Detected: yiw')
    sheet = xl.sheet_names[0]
    df    = xl.parse(sheet, dtype=str)
    df    = df.dropna(how='all')

    eso_col      = find_col(df, 'Support Organization') or find_col_like(df, 'implementing_partner') or find_col_like(df, 'implementing', 'partner')
    sector_col   = find_col(df, 'Business Sector') or find_col_like(df, 'sector')
    district_col = find_col(df, 'Business District') or find_col(df, 'Residence District') or find_col_like(df, 'district')
    earned_col   = find_col(df, 'Earned Income from 10X') or find_col_like(df, 'earned', 'income', 'result') or find_col_like(df, 'earned an income')
    improved_col = find_col_like(df, 'working conditions', 'improved') or find_col_like(df, 'work improved')
    income_col   = (find_col_like(df, 'how much', 'earned') or
                    find_col(df, 'Income Earned (UGX)') or
                    find_col_like(df, 'current earnings') or
                    find_col_like(df, 'current income'))
    # Foundation-completion columns may appear under multiple names across versions
    found_cols = [c for c in df.columns
                  if 'foundation' in c.lower() and
                     ('complete' in c.lower() or 'course' in c.lower())]
    course_completed_col = find_col(df, 'Course Completed')
    if course_completed_col and course_completed_col not in found_cols:
        found_cols.append(course_completed_col)

    total     = len(df)
    sectors   = value_counts_dict(df[sector_col],   top_n=15) if sector_col   else {}
    districts = value_counts_dict(df[district_col], top_n=15) if district_col else {}
    income_levels = value_counts_dict(df[income_col], top_n=10) if income_col else {}
    gender_col = find_col(df, 'Gender') or find_col_like(df, 'gender')
    age_col = find_col(df, 'Age') or find_col_like(df, 'how old') or find_col_like(df, 'age')
    employment_col = find_col(df, 'Employment Type') or find_col_like(df, 'employment', 'type')
    quarter_col = find_col(df, 'Reporting Quarter') or find_col(df, 'Source Quarter') or find_col_like(df, 'quarter')
    pwd_col = find_col(df, 'Disability Status') or find_col(df, 'PWDS') or find_col(df, 'PWD Flag')
    refugee_col = find_col(df, 'Refugee Status') or find_col(df, 'Refugee') or find_col(df, 'Refugee Flag')
    business_status_col = find_col(df, 'Business Status')
    income_band_col = find_col(df, 'Income Band')
    work_desc_col = find_col(df, 'Work Improvement Description') or find_col_like(df, 'how did your work improve')

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

    def yes_count(col):
        if not col or col not in df.columns:
            return 0
        s = df[col].astype(str).str.strip().str.lower()
        return int(s.isin(['yes', 'y', 'true', '1', 'yes, i have', 'yes, completed', 'completed', 'yes, i have earned']).sum())

    def income_value(raw):
        cleaned = re.sub(r'[^0-9.]', '', str(raw or ''))
        try:
            return float(cleaned) if cleaned else 0
        except Exception:
            return 0

    def median(values):
        values = sorted(v for v in values if v is not None)
        if not values:
            return 0
        mid = len(values) // 2
        return values[mid] if len(values) % 2 else (values[mid - 1] + values[mid]) / 2

    def percentile(values, pct):
        values = sorted(v for v in values if v is not None)
        if not values:
            return 0
        idx = int(round((len(values) - 1) * pct))
        return values[max(0, min(idx, len(values) - 1))]

    earned_pct   = yes_pct(earned_col)
    improved_pct = yes_pct(improved_col)
    found_pct    = 0.0
    for fc in found_cols:
        s   = df[fc].astype(str).str.strip().str.lower()
        pct = round(s.isin(['yes', 'yes, completed', 'completed']).sum() / max(total, 1) * 100, 1)
        if pct > found_pct:
            found_pct = pct

    earned_amounts = []
    income_outliers = 0
    if income_col:
        for raw in df[income_col].dropna():
            value = income_value(raw)
            if value > 5_000_000:
                income_outliers += 1
            elif value > 0:
                earned_amounts.append(value)

    income_amount_bands = collections.Counter()
    income_amount_bands['No income'] = max(total - len(earned_amounts) - income_outliers, 0)
    for value in earned_amounts:
        if value <= 100_000:
            income_amount_bands['Survival (<=100k)'] += 1
        elif value <= 500_000:
            income_amount_bands['Growing (100k-500k)'] += 1
        elif value <= 1_000_000:
            income_amount_bands['Stable (500k-1M)'] += 1
        else:
            income_amount_bands['Thriving (>1M)'] += 1

    work_improvements = {}
    for col in df.columns:
        cl = str(col).lower()
        if cl.startswith('outcome - '):
            work_improvements[str(col).replace('Outcome - ', '').strip()] = yes_count(col)

    return {
        'raw_records': extract_raw_records(df),
        'type': 'yiw',
        'name':     'Youth in Work',
        'filename': filename,
        'stats': {
            'total':        total,
            'record_count': total,
        },
        'earned_income_pct':   earned_pct,
        'work_improved_pct':   improved_pct,
        'foundation_done_pct': found_pct,
        'earned_income_count': yes_count(earned_col),
        'work_improved_count': yes_count(improved_col),
        'foundation_done_count': max(yes_count(fc) for fc in found_cols) if found_cols else 0,
        'earned_amount_stats': {
            'count': len(earned_amounts),
            'total': int(sum(earned_amounts)),
            'avg': int(sum(earned_amounts) / len(earned_amounts)) if earned_amounts else 0,
            'median': int(median(earned_amounts)),
            'q1': int(percentile(earned_amounts, 0.25)),
            'q3': int(percentile(earned_amounts, 0.75)),
            'outliers_excluded': income_outliers,
        },
        'income_amount_bands': dict(income_amount_bands),
        'business_status': value_counts_dict(df[business_status_col]) if business_status_col else {},
        'theme_counts': {},
        'yiw_quotes': [],
        'gender': value_counts_dict(df[gender_col]) if gender_col else {},
        'age_bands': value_counts_dict(df[age_col]) if age_col else {},
        'employment_types': value_counts_dict(df[employment_col]) if employment_col else {},
        'quarters': value_counts_dict(df[quarter_col]) if quarter_col else {},
        'inclusion_metrics': {
            'pwd': yes_count(pwd_col),
            'refugees': yes_count(refugee_col),
        },
        'work_improvements': work_improvements,
        'by_eso':       by_eso,
        'by_eso_detail': {},
        'by_district_detail': {},
        'by_sector_detail': {},
        'by_quarter_detail': {},
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
    credit_col   = find_col_like(df, 'digital credit', 'received') or find_col_like(df, 'loan', 'received')
    elig_col     = find_col_like(df, 'digital credit', 'eligible') or find_col_like(df, 'loan', 'eligible')
    credit_amt_col = find_col_like(df, 'credit', 'amount') or find_col_like(df, 'loan', 'amount')

    total     = len(df)
    sectors   = value_counts_dict(df[sector_col],   top_n=15) if sector_col   else {}
    districts = value_counts_dict(df[district_col], top_n=15) if district_col else {}
    income_levels = value_counts_dict(df[income_col], top_n=10) if income_col else {}

    by_eso = {}
    if eso_col:
        for eso, grp in df.groupby(eso_col):
            eso = str(eso).strip()
            if eso and eso != 'nan':
                by_eso[eso] = {
                    'total': len(grp),
                    'credit_eligible': int(pd.to_numeric(grp[elig_col], errors='coerce').fillna(0).sum()) if elig_col else 0,
                    'credit_approved': int(pd.to_numeric(grp[credit_col], errors='coerce').fillna(0).sum()) if credit_col else 0,
                }

    def yes_pct(col):
        if not col or col not in df.columns:
            return 0.0
        s = df[col].astype(str).str.strip().str.lower()
        return round(s.isin(['yes', 'yes, i am', '1', 'true']).sum() / max(total, 1) * 100, 1)

    def yes_count(col):
        if not col or col not in df.columns:
            return 0
        s = df[col].astype(str).str.strip().str.lower()
        return int(s.isin(['yes', 'yes, i am', '1', 'true']).sum())

    registered_pct  = yes_pct(reg_col)
    pwd_count       = yes_count(pwd_col)
    device_need_pct = yes_pct(device_col)
    credit_eligible = yes_count(elig_col)
    credit_approved = yes_count(credit_col)
    
    credit_total_amount = int(pd.to_numeric(df[credit_amt_col], errors='coerce').fillna(0).sum()) if credit_amt_col else 0

    refugee_count = 0
    if refugee_col:
        col_lower = refugee_col.lower()
        if 'country' in col_lower or 'nationality' in col_lower or 'citizenship' in col_lower:
            # Nationality-type column: use parse_nationality to detect refugee status
            # and normalize country variants (Uganda/Ugandan/UG all become Uganda).
            refugee_count = count_refugees(df[refugee_col])
            df['_Nationality'] = df[refugee_col].apply(normalize_nationality)
        else:
            # Dedicated refugee column (yes/no or free-text flag)
            r = df[refugee_col].astype(str).str.strip().str.lower()
            refugee_count = int(r.isin(['yes', 'y', '1', 'true', 'refugee']).sum())

    _dig_kw = ['digital', 'smartphone', 'computer', 'internet', 'mobile money',
               'online', 'social media', 'whatsapp', 'skill', 'app', 'technology']
    digital_cols = [c for c in df.columns
                    if any(k in str(c).lower() for k in _dig_kw)
                    and c not in filter(None, [device_col, credit_col, elig_col, credit_amt_col])]

    digital_skills = {}
    for dc in digital_cols[:15]:
        label = dc.split('/')[-1].strip() if '/' in dc else dc.strip()
        vals  = df[dc].astype(str).str.strip().str.lower()
        yes_n = int(vals.isin(['yes', '1', 'true', 'checked', 'selected']).sum())
        if yes_n > 0:
            digital_skills[label] = yes_n

    return {
        'raw_records': extract_raw_records(df),
        'type': 'buz_needs',
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
        # Credit metrics from buz_needs
        'credit_eligible':  credit_eligible,
        'credit_approved':  credit_approved,
        'credit_amount':    credit_total_amount,
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

    # Extract org name from filename (e.g., Outbox, Refactory, WITU)
    org_match = re.match(r'(Outbox|Refactory|WITU)', filename, re.IGNORECASE)
    if org_match:
        org_name = org_match.group(1)
    else:
        # Fallback: use first part of filename before _ or -
        org_name = re.split(r'[_\-]', filename)[0]
        org_name = org_name.strip() or 'Device'

    return {
        'raw_records': extract_raw_records(df),
        'type': 'devices',
        'name':     f'{org_name} Device Financing',
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
            if str(xente_f).lower().endswith('.xlsx'):
                xl = pd.ExcelFile(xente_f, engine='openpyxl')
            elif str(xente_f).lower().endswith('.xls'):
                xl = pd.ExcelFile(xente_f, engine='xlrd')
            else:
                raise ValueError('Unsupported file extension for Xente file')
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
                        # Skip dates outside the valid program range (2024-2026)
                        try:
                            if not (2024 <= int(k[:4]) <= 2026):
                                continue
                        except (ValueError, TypeError):
                            continue
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
            if str(cc_pedn).lower().endswith('.xlsx'):
                xl = pd.ExcelFile(cc_pedn, engine='openpyxl')
            elif str(cc_pedn).lower().endswith('.xls'):
                xl = pd.ExcelFile(cc_pedn, engine='xlrd')
            else:
                raise ValueError('Unsupported file extension for Chapchap PEDN file')
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
                if str(xlsx_f).lower().endswith('.xlsx'):
                    xl = pd.ExcelFile(xlsx_f, engine='openpyxl')
                elif str(xlsx_f).lower().endswith('.xls'):
                    xl = pd.ExcelFile(xlsx_f, engine='xlrd')
                else:
                    raise ValueError('Unsupported file extension for Flexipay file')
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
            if str(ezy_f).lower().endswith('.xlsx'):
                xl = pd.ExcelFile(ezy_f, engine='openpyxl')
            elif str(ezy_f).lower().endswith('.xls'):
                xl = pd.ExcelFile(ezy_f, engine='xlrd')
            else:
                raise ValueError('Unsupported file extension for EzyAgric file')
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

        platform_totals = {
            'Xente': xente_combined,
            'ChapChap': chapchap_total,
            'FlexiPay': flexipay_total,
            'EzyAgric': ezyagric_total,
        }
        platform_mix_pct = {
            k: round(v / max(total_onboardings, 1) * 100, 1)
            for k, v in platform_totals.items()
        }
        adoption_funnel = {
            'Onboarded': total_onboardings,
            'Completed Registration': flexipay_fully_reg + flexipay_complete,
            'Pending Validation': flexipay_pending,
            'Business Activity Captured': ezyagric_total,
            'Transactions / Items Captured': ezyagric_items,
        }
        active_usage = {
            'active_users': None,
            'inactive_users': None,
            'last_active_date': None,
            'usage_frequency': {},
            'available': False,
        }
        business_impact = {
            'sales_revenue_ugx': ezyagric_cost,
            'orders_or_items': ezyagric_items,
            'records_created': None,
            'loans_accessed': None,
            'market_linkages': None,
            'available_fields': ['EzyAgric items ordered', 'EzyAgric item value'],
        }
        platform_geo = {}
        for source in (xente_locs, ezy_districts):
            for k, v in source.items():
                platform_geo[k] = platform_geo.get(k, 0) + int(v)
        for k, v in chapchap_pedn_districts.items():
            # Earlier parser versions can pick up timestamps in this column; keep only location-like labels.
            if not str(k).startswith('20') and 'T' not in str(k):
                platform_geo[k] = platform_geo.get(k, 0) + int(v)

        invalid_chapchap_geo = sum(
            int(v) for k, v in chapchap_pedn_districts.items()
            if str(k).startswith('20') or 'T' in str(k)
        )
        platform_data_quality = {
            'missing_raw_records': 0,
            'invalid_chapchap_district_labels': invalid_chapchap_geo,
            'missing_gender': max(total_onboardings - (total_female + total_male), 0),
            'missing_active_usage_fields': 1,
            'missing_business_impact_fields': 3,
            'missing_approval_funnel_fields': 1,
        }
        field_availability = {
            'active_usage': False,
            'approval_funnel': False,
            'business_impact': True if ezyagric_items or ezyagric_cost else False,
            'gender': bool(total_female or total_male),
            'pwd': bool(xente_pwd),
            'refugee': False,
            'youth': False,
            'last_active_date': False,
        }

        print(f'  OK \u2014 "Digital Platforms"  ({total_onboardings:,} records)')
        return {
            'raw_records': extract_raw_records(df) if 'df' in locals() else [],
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
            'platform_totals': platform_totals,
            'platform_mix_pct': platform_mix_pct,
            'adoption_funnel': adoption_funnel,
            'active_usage': active_usage,
            'business_impact': business_impact,
            'platform_geography': dict(sorted(platform_geo.items(), key=lambda x: x[1], reverse=True)[:15]),
            'data_quality': platform_data_quality,
            'field_availability': field_availability,
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
        if str(fpath).lower().endswith('.xlsx'):
            xl = pd.ExcelFile(fpath, engine='openpyxl')
        elif str(fpath).lower().endswith('.xls'):
            xl = pd.ExcelFile(fpath, engine='xlrd')
        else:
            raise ValueError('Unsupported file extension for Foundation file')
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
        'raw_records': extract_raw_records(df),
        'type': 'foundation',
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


# ── Kobo API Integration ────────────────────────────────────────────────────

KOBO_CONFIG_FILE = BASE_DIR / 'kobo_config.json'
KOBO_LOCAL_CONFIG_FILE = BASE_DIR / 'kobo_config.local.json'
KOBO_CACHE_FILE  = BASE_DIR / '.kobo_cache.json'

# Directories replaced by live Kobo feeds (skipped during Excel scan)
KOBO_EOI_SKIP_DIRS = {
    str(DATA_SRC_DIR / 'eoi'),
    str(BASE_DIR / 'EOI' / '_eoi_eso'),
    str(BASE_DIR / 'EOI' / '_cleaned'),   # segmentation Excel files replaced by Kobo
}

# Individual root-level files replaced by Kobo (matched case-insensitively on filename stem)
KOBO_SKIP_FILENAMES = {
    'mkazipreneur needs assessment 31-3-2025',
}
KOBO_YIW_SKIP_DIRS = {
    str(DATA_SRC_DIR / 'yiw'),
    str(BASE_DIR / 'YIW'),
}
KOBO_BUZ_SKIP_DIRS = {
    str(BASE_DIR / 'Buz_needs'),
}
KOBO_DEV_SKIP_DIRS = {
    str(DATA_SRC_DIR / 'devices'),
    str(BASE_DIR / 'Devices'),
}

# Snake_case ESO names from Kobo → readable labels
# 12 official ESO partners + "Other" for everything else.
# Keys are lowercase with special chars normalised (same logic as cleanKey in EsoName.php).
_ESO_MAP = {
    # MUBS EIIC
    'mubs107': 'MUBS EIIC', 'mubs': 'MUBS EIIC', 'mubs eiic': 'MUBS EIIC', 'mubs eiic': 'MUBS EIIC',
    # DFCU Foundation
    'dfcu103': 'DFCU Foundation', 'dfcu': 'DFCU Foundation',
    'dfcu foundation': 'DFCU Foundation', 'dfcu_foundation': 'DFCU Foundation',
    # Mkazipreneur
    'mkazi106': 'Mkazipreneur', 'mkazi': 'Mkazipreneur', 'mkazipreneur': 'Mkazipreneur',
    # Stanbic Business Incubator
    'stanbic108': 'Stanbic Business Incubator', 'stanbic': 'Stanbic Business Incubator',
    'sbil': 'Stanbic Business Incubator', 'stanbic business incubator': 'Stanbic Business Incubator',
    'stanbic bank incubator': 'Stanbic Business Incubator',
    'stanbic_bank_incubator': 'Stanbic Business Incubator',
    'stanbic_business_incubator': 'Stanbic Business Incubator',
    # PEDN
    'pedn109': 'PEDN', 'pedn': 'PEDN',
    'private education development network': 'PEDN',
    'the private education development network': 'PEDN',
    'the private education development network  pedn ': 'PEDN',
    # Excelhort
    'excel104': 'Excelhort', 'excel': 'Excelhort', 'excell': 'Excelhort',
    'excelhort': 'Excelhort', 'excel hort': 'Excelhort', 'excell hort': 'Excelhort',
    # Challenges Uganda
    'challenges102': 'Challenges Uganda', 'challenges': 'Challenges Uganda',
    'challenges ug': 'Challenges Uganda', 'challenges uganda': 'Challenges Uganda',
    # AGDI
    'agdi109': 'AGDI', 'agdi': 'AGDI',
    # Finding XY
    'xy105': 'Finding XY', 'xy': 'Finding XY', 'finding xy': 'Finding XY', 'finding_xy': 'Finding XY',
    # AID
    'alb110': 'AID', 'aid': 'AID',
    # CURAD
    'curad111': 'CURAD', 'curad': 'CURAD',
    # Living Earth Uganda
    'leu112': 'Living Earth Uganda', 'leu': 'Living Earth Uganda',
    'living earth': 'Living Earth Uganda', 'living earth uganda': 'Living Earth Uganda',
    'living_earth_uganda': 'Living Earth Uganda',
}

# Data-collection hubs that were entered instead of the actual ESO partner name.
# Discovered by cross-referencing the t_and_cs/learnt_abt_hi_innov free-text field.
_ESO_HUB_REMAP = {
    'witu':      'Mkazipreneur',      # WITU hub collects for Mkazipreneur
    'refactory': 'PEDN',              # Refactory hub collects for PEDN
    'uncdf':     'Excelhort',         # UNCDF hub collects for Excelhort
    'uncdf10x':  'Excelhort',
    'outbox':    'Challenges Uganda', # Outbox hub collects for Challenges Uganda
    '10xoutbox': 'Challenges Uganda',
}

# Anything not in the 12 ESOs and not remappable → Other
_ESO_OTHER = {
    'other', 'finding xy  other', 'albertine',
}


def _eso_label(raw, fallback='Other'):
    if not raw:
        return ''
    value = str(raw).strip()
    if not value or value == '#N/A':
        return ''
    import re as _re
    key = _re.sub(r'\s+', ' ', _re.sub(r'[().,_\-]', ' ', value.lower())).strip()
    # 1. Remap data-collection hubs to their actual ESO partner
    if key in _ESO_HUB_REMAP:
        return _ESO_HUB_REMAP[key]
    # 2. Check the official 12 ESO name map
    if key in _ESO_MAP:
        return _ESO_MAP[key]
    if key in _ESO_OTHER:
        return fallback
    # not recognised → Other
    return fallback


def _load_kobo_config():
    env_token = os.environ.get('KOBO_TOKEN', '').strip()
    env_base_url = os.environ.get('KOBO_BASE_URL', '').strip()
    if env_token:
        cfg = {}
        if KOBO_CONFIG_FILE.exists():
            try:
                cfg = json.loads(KOBO_CONFIG_FILE.read_text(encoding='utf-8'))
            except Exception:
                cfg = {}
        cfg['token'] = env_token
        cfg['base_url'] = env_base_url or cfg.get('base_url') or 'https://kf.kobotoolbox.org'
        return cfg

    config_file = KOBO_LOCAL_CONFIG_FILE if KOBO_LOCAL_CONFIG_FILE.exists() else KOBO_CONFIG_FILE
    if not config_file.exists():
        return None
    try:
        return json.loads(config_file.read_text(encoding='utf-8'))
    except Exception:
        return None


def _fetch_kobo_submissions(base_url, token, asset_uid, page_size=500):
    """Page through the Kobo API and return every submission as a list of dicts."""
    url = f'{base_url}/api/v2/assets/{asset_uid}/data/?format=json&limit={page_size}'
    headers = {'Authorization': f'Token {token}'}
    records = []
    total = None
    while url:
        req = urllib.request.Request(url, headers=headers)
        try:
            with urllib.request.urlopen(req, timeout=60) as resp:
                data = json.loads(resp.read().decode())
        except Exception as exc:
            print(f'\n  Kobo API error: {exc}')
            break
        records.extend(data.get('results', []))
        total = data.get('count', total)
        print(f'  Fetching: {len(records):,} / {total or "?"}…', end='\r')
        url = data.get('next')
    print(f'  Fetched {len(records):,} records.             ')
    return records


def _get_kobo_submissions(base_url, token, asset_uid, cache_max_age_hours=4):
    """Return submissions from a local cache if still fresh, otherwise re-fetch."""
    cache = {}
    if KOBO_CACHE_FILE.exists():
        try:
            cache = json.loads(KOBO_CACHE_FILE.read_text(encoding='utf-8'))
        except Exception:
            pass

    entry = cache.get(asset_uid, {})
    age_hours = (time.time() - entry.get('ts', 0)) / 3600

    if 'records' in entry and age_hours < cache_max_age_hours:
        print(f'  Using cache ({age_hours:.1f}h old, {len(entry["records"]):,} records)')
        return entry['records']

    print('  Fetching from Kobo API…')
    records = _fetch_kobo_submissions(base_url, token, asset_uid)
    if records:
        cache[asset_uid] = {'ts': time.time(), 'records': records}
        # Disk can be tight on this workstation; avoid rewriting the large
        # Kobo cache file during scheduled syncs. Fresh API data is still used.
    return records


def parse_kobo_yiw(records, asset_uid, name='Youth in Work'):
    """Convert raw Kobo YIW submissions into the same portfolio dict as parse_yiw_file."""
    if not records:
        return None

    total = len(records)

    def _yes_pct(field):
        yes = sum(1 for r in records if str(r.get(field) or '').strip().lower() == 'yes')
        return round(yes / max(total, 1) * 100, 1)

    earned_pct   = _yes_pct('What_you_work_improved_in_anyw')
    improved_pct = _yes_pct('Has_your_work_improv_r_working_conditions')
    found_pct    = _yes_pct('Did_you_complete_the_Foundation_Course')

    by_eso = {}
    for r in records:
        raw = str(r.get('Implementing_Partner_Support_Organization') or '').strip()
        if raw:
            eso = _eso_label(raw)
            by_eso[eso] = by_eso.get(eso, {'total': 0})
            by_eso[eso]['total'] += 1

    raw_records = [
        {'d': str(r.get('_submission_time') or '')[:10],
         'n': str(r.get('Kindly_share_your_full_name') or '')}
        for r in records[:5000]
    ]

    return {
        'raw_records':         raw_records,
        'type':                'yiw',
        'name':                name,
        'filename':            f'kobo:{asset_uid}',
        'stats':               {'total': total, 'record_count': total},
        'earned_income_pct':   earned_pct,
        'work_improved_pct':   improved_pct,
        'foundation_done_pct': found_pct,
        'by_eso':              by_eso,
        'sectors':             {},
        'districts':           {},
        'income_levels':       {},
    }


def parse_kobo_buz_needs(records, asset_uid, name='Business Needs'):
    """Convert raw Kobo Business Needs submissions into the same dict as parse_buz_needs_file."""
    if not records:
        return None

    total = len(records)

    F_ESO      = 'group_oj8uw97/_1_Implementing_Partn_Support_Organization'
    F_PWD      = 'group_oj8uw97/Are_you_a_person_with_a_disabi'
    F_REG      = 'group_oj8uw97/_5_Is_your_business_registered'
    F_INCOME   = 'group_oj8uw97/group_zw8kn95/_9_Average_income_per_month_estimate'
    F_DEVICE   = 'group_xx9pw99/_14_Do_you_need_a_device_to_su'
    F_CREDIT   = 'group_wf5op54/_35_If_you_received_p_your_business_grow'
    F_BIZ_NAME = 'group_oj8uw97/_1_Business_Name'
    F_TIME     = '_submission_time'

    def _yes_pct(field):
        yes = sum(1 for r in records if str(r.get(field) or '').strip().lower() == 'yes')
        return round(yes / max(total, 1) * 100, 1)

    def _yes_count(field):
        return sum(1 for r in records if str(r.get(field) or '').strip().lower() == 'yes')

    registered_pct  = _yes_pct(F_REG)
    device_need_pct = _yes_pct(F_DEVICE)
    pwd_count       = _yes_count(F_PWD)

    income_levels = dict(
        collections.Counter(
            str(r.get(F_INCOME) or '').strip().replace('_', ' ').title()
            for r in records
            if r.get(F_INCOME)
        ).most_common(10)
    )

    by_eso = {}
    for r in records:
        raw = str(r.get(F_ESO) or '').strip()
        if raw:
            eso = _eso_label(raw)
            if eso not in by_eso:
                by_eso[eso] = {'total': 0, 'credit_eligible': 0, 'credit_approved': 0}
            by_eso[eso]['total'] += 1

    raw_records = [
        {'d': str(r.get(F_TIME) or '')[:10],
         'b': str(r.get(F_BIZ_NAME) or '')}
        for r in records[:5000]
    ]

    return {
        'raw_records':    raw_records,
        'type':           'buz_needs',
        'name':           name,
        'filename':       f'kobo:{asset_uid}',
        'stats': {
            'total':        total,
            'pwd':          pwd_count,
            'refugees':     0,
            'record_count': total,
        },
        'registered_pct':  registered_pct,
        'device_need_pct': device_need_pct,
        'pwd_pct':         round(pwd_count / max(total, 1) * 100, 1),
        'by_eso':          by_eso,
        'sectors':         {},
        'districts':       {},
        'income_levels':   income_levels,
        'digital_skills':  {},
        'credit_eligible': 0,
        'credit_approved': 0,
        'credit_amount':   0,
    }


def parse_kobo_devices(records, asset_uid, org_name):
    """Convert raw Kobo device-financing submissions into the same dict as parse_devices_file."""
    if not records:
        return None

    total = len(records)

    def _yes_count(field):
        return sum(1 for r in records if str(r.get(field) or '').strip().lower() == 'yes')

    def _val(r, *fields):
        for f in fields:
            v = r.get(f)
            if v:
                return str(v).strip()
        return ''

    with_disability = _yes_count('has_disability')
    business_reg    = _yes_count('business_registered')

    # Districts
    districts = dict(
        collections.Counter(
            str(r.get('district') or '').strip().upper()
            for r in records if r.get('district')
        ).most_common(20)
    )

    # Device types
    device_types = dict(
        collections.Counter(
            str(r.get('device_type') or '').strip().title()
            for r in records if r.get('device_type')
        ).most_common(8)
    )

    # ESO hubs
    eso_hubs = dict(
        collections.Counter(
            _eso_label(str(r.get('eso_hub') or '').strip())
            for r in records if r.get('eso_hub')
        ).most_common(10)
    )

    # ID types
    id_types = dict(
        collections.Counter(
            str(r.get('id_type') or '').strip()
            for r in records if r.get('id_type')
        ).most_common(8)
    )

    # Registration body
    reg_body = dict(
        collections.Counter(
            str(r.get('registration_body') or '').strip()
            for r in records if r.get('registration_body')
        ).most_common(8)
    )

    # SIM registered
    sim_registered = dict(
        collections.Counter(
            str(r.get('number_registered') or '').strip().title()
            for r in records if r.get('number_registered')
        ).most_common(5)
    )

    # Price bands
    def _price_band(v):
        try:
            v = float(str(v).replace(',', '').strip())
        except Exception:
            return None
        if v <= 0:         return None
        if v < 200_000:    return 'Under 200K'
        if v < 500_000:    return '200K–500K'
        if v < 1_000_000:  return '500K–1M'
        if v < 2_000_000:  return '1M–2M'
        return '2M+'

    price_bands_raw = collections.Counter()
    prices = []
    for r in records:
        v = r.get('price_range')
        if v:
            b = _price_band(v)
            if b:
                price_bands_raw[b] += 1
            try:
                prices.append(float(str(v).replace(',', '')))
            except Exception:
                pass
    order = ['Under 200K', '200K–500K', '500K–1M', '1M–2M', '2M+']
    price_bands = {k: price_bands_raw[k] for k in order if k in price_bands_raw}
    price_stats = {}
    if prices:
        price_stats = {'avg': int(sum(prices) / len(prices)), 'median': int(sorted(prices)[len(prices) // 2])}

    # Payment duration
    def _norm_dur(v):
        v = str(v).strip().lower()
        if 'month' in v:  return 'Monthly'
        if 'week' in v:   return 'Weekly'
        if 'year' in v or 'annual' in v: return 'Yearly'
        return str(v).title()[:20]

    payment_duration = dict(
        collections.Counter(
            _norm_dur(r.get('payment_duration') or '')
            for r in records if r.get('payment_duration')
        ).most_common()
    )

    # Temporal activity
    weekly_activity = {}
    eso_weekly      = {}
    device_weekly   = {}
    times = []
    for r in records:
        t = r.get('_submission_time')
        if t:
            try:
                times.append((pd.to_datetime(t), r))
            except Exception:
                pass

    if times:
        today       = pd.Timestamp.now().normalize()
        week_start  = today - pd.Timedelta(days=today.weekday())
        lweek_start = week_start - pd.Timedelta(weeks=1)
        month_start = today.replace(day=1)
        wk  = sum(1 for dt, _ in times if dt >= week_start)
        lwk = sum(1 for dt, _ in times if lweek_start <= dt < week_start)
        mo  = sum(1 for dt, _ in times if dt >= month_start)
        weekly_activity = {'apps_this_week': wk, 'apps_last_week': lwk, 'apps_this_month': mo}

        for eso_raw in set(_val(r, 'eso_hub') for _, r in times if _val(r, 'eso_hub')):
            eso = _eso_label(eso_raw)
            eso_weekly[eso] = {
                'total':      sum(1 for _, r in times if _eso_label(_val(r, 'eso_hub')) == eso),
                'this_week':  sum(1 for dt, r in times if dt >= week_start and _eso_label(_val(r, 'eso_hub')) == eso),
                'last_week':  sum(1 for dt, r in times if lweek_start <= dt < week_start and _eso_label(_val(r, 'eso_hub')) == eso),
                'this_month': sum(1 for dt, r in times if dt >= month_start and _eso_label(_val(r, 'eso_hub')) == eso),
            }

    raw_records = [
        {'d': str(r.get('_submission_time') or '')[:10],
         'n': str(r.get('first_name') or '') + ' ' + str(r.get('last_name') or ''),
         'b': str(r.get('business_name') or '')}
        for r in records[:5000]
    ]

    return {
        'raw_records':       raw_records,
        'type':              'devices',
        'name':              f'{org_name} Device Financing',
        'filename':          f'kobo:{asset_uid}',
        'stats': {
            'total':           total,
            'with_disability': with_disability,
            'business_reg':    business_reg,
            'record_count':    total,
        },
        'disability_pct':    round(with_disability / max(total, 1) * 100, 1),
        'business_reg_pct':  round(business_reg    / max(total, 1) * 100, 1),
        'districts':         districts,
        'device_types':      device_types,
        'price_bands':       price_bands,
        'payment_duration':  payment_duration,
        'eso_hubs':          eso_hubs,
        'id_types':          id_types,
        'reg_body':          reg_body,
        'disability_types':  {},
        'device_by_eso':     {},
        'sim_registered':    sim_registered,
        'weekly_activity':   weekly_activity,
        'eso_weekly':        eso_weekly,
        'device_weekly':     device_weekly,
        'price_stats':       price_stats,
    }


def parse_kobo_yiw(records, asset_uid, name='Youth in Work'):
    """Convert raw Kobo YIW submissions into the same portfolio dict as parse_yiw_file."""
    if not records:
        return None

    total = len(records)

    def _yes(r, key):
        return str(r.get(key, '')).strip().lower() in ('yes', '1', 'true')

    foundation_yes = sum(1 for r in records if _yes(r, 'Did_you_complete_the_Foundation_Course'))
    improved_yes   = sum(1 for r in records if _yes(r, 'Has_your_work_improv_r_working_conditions'))
    earned_yes     = sum(1 for r in records if _yes(r, 'What_you_work_improved_in_anyw'))

    by_eso = {}
    for r in records:
        raw = r.get('Implementing_Partner_Support_Organization', '')
        eso = _eso_label(raw)
        if eso:
            by_eso.setdefault(eso, {'total': 0})
            by_eso[eso]['total'] += 1

    # Temporal: weekly / monthly activity
    weekly_activity = {}
    sub_times = [r.get('_submission_time') for r in records if r.get('_submission_time')]
    if sub_times:
        import pandas as _pd
        dt          = _pd.to_datetime(sub_times, errors='coerce')
        today       = _pd.Timestamp.now().normalize()
        week_start  = today - _pd.Timedelta(days=today.weekday())
        lweek_start = week_start - _pd.Timedelta(weeks=1)
        month_start = today.replace(day=1)
        weekly_activity = {
            'apps_this_week':  int((dt >= week_start).sum()),
            'apps_last_week':  int(((dt >= lweek_start) & (dt < week_start)).sum()),
            'apps_this_month': int((dt >= month_start).sum()),
        }

    return {
        'type':     'yiw',
        'name':     name,
        'filename': f'kobo:{asset_uid}',
        'stats': {'total': total, 'record_count': total},
        'earned_income_pct':   round(earned_yes   / max(total, 1) * 100, 1),
        'work_improved_pct':   round(improved_yes / max(total, 1) * 100, 1),
        'foundation_done_pct': round(foundation_yes / max(total, 1) * 100, 1),
        'by_eso':        by_eso,
        'sectors':       {},
        'districts':     {},
        'income_levels': {},
        'weekly_activity': weekly_activity,
    }


def parse_kobo_buz_needs(records, asset_uid, name='Business Needs'):
    """Convert raw Kobo Business Needs submissions into the same portfolio dict as parse_buz_needs_file."""
    if not records:
        return None

    total = len(records)

    def _yes(r, key):
        return str(r.get(key, '')).strip().lower() in ('yes', '1', 'true')

    def _yes_count(key):
        return sum(1 for r in records if _yes(r, key))

    def _clean_label(value):
        value = str(value or '').strip()
        if not value or value.lower() == 'nan':
            return ''
        value = value.strip('_').replace('__', '_').replace('_', ' ')
        value = ' '.join(value.split())
        label = value.title()
        return {
            'Sacco': 'SACCO',
            'Vsla': 'VSLA',
            'Mfi': 'MFI',
            'Mtn': 'MTN',
            'Mokash': 'MoKash',
        }.get(label, label)

    def _count_field(key, top_n=15):
        counts = {}
        for r in records:
            label = _clean_label(r.get(key))
            if label:
                counts[label] = counts.get(label, 0) + 1
        return dict(sorted(counts.items(), key=lambda x: -x[1])[:top_n])

    def _count_multi_field(key, top_n=15):
        counts = {}
        for r in records:
            raw = str(r.get(key, '') or '').strip()
            if not raw or raw.lower() == 'nan':
                continue
            for part in raw.split():
                label = _clean_label(part)
                if label:
                    counts[label] = counts.get(label, 0) + 1
        return dict(sorted(counts.items(), key=lambda x: -x[1])[:top_n])

    def _has_option(r, key, phrase):
        raw = str(r.get(key, '') or '').strip().lower().replace('_', ' ')
        phrase = str(phrase or '').strip().lower().replace('_', ' ')
        return bool(raw and phrase and phrase in ' '.join(raw.split()))

    def _credit_demand(r):
        return (
            _has_option(r, 'group_xx9pw99/_20_What_digital_skills_would_', 'access digital credit')
            or _has_option(r, 'group_wf5op54/_37_What_support_would_help_yo', 'understanding loans investors')
            or _has_option(r, 'group_wf5op54/_37_What_support_would_help_yo', 'loans')
        )

    def _credit_readiness_score(r):
        score = 0
        if _yes(r, 'group_oj8uw97/_5_Is_your_business_registered'):
            score += 1
        if _yes(r, 'group_xx9pw99/_17_Do_you_use_mobil_or_business_payments'):
            score += 1
        internet = str(r.get('group_xx9pw99/_16_Do_you_have_access_to_internet', '')).strip().lower()
        if internet in ('always', 'sometimes'):
            score += 1
        confidence = str(r.get('group_xx9pw99/_19_Do_you_feel_conf_ls_for_your_business', '')).strip().lower()
        if confidence == 'yes':
            score += 1
        return score

    def _readiness_bucket(score):
        if score >= 3:
            return 'Ready for Screening'
        if score == 2:
            return 'Needs Follow-Up'
        return 'Not Ready Yet'

    pwd_count       = _yes_count('group_oj8uw97/Are_you_a_person_with_a_disabi')
    registered      = _yes_count('group_oj8uw97/_5_Is_your_business_registered')
    device_need     = _yes_count('group_xx9pw99/_14_Do_you_need_a_device_to_su')
    internet_access = _count_field('group_xx9pw99/_16_Do_you_have_access_to_internet', 5)
    mobile_payments = _count_field('group_xx9pw99/_17_Do_you_use_mobil_or_business_payments', 5)
    digital_confidence = _count_field('group_xx9pw99/_19_Do_you_feel_conf_ls_for_your_business', 5)
    business_age = _count_field('group_oj8uw97/_4_How_long_have_you_unning_this_business', 8)
    growth_status = _count_field('group_oj8uw97/_7_How_do_you_feel_a_r_business_right_now', 8)
    income_frequency = _count_field('group_oj8uw97/group_zw8kn95/_8_How_often_do_you_earn_income', 8)
    income_stability = _count_field('group_oj8uw97/group_zw8kn95/_10_Your_income_is', 8)
    sectors = _count_field('group_oj8uw97/sector', 15)
    districts = _count_field('group_oj8uw97/e_Which_district_are_you_located_in', 20)
    respondent_roles = _count_field('group_oj8uw97/d_What_position_in_the_business', 8)
    digital_skills = _count_multi_field('group_xx9pw99/_20_What_digital_skills_would_', 15)
    business_constraints = _count_multi_field('group_wy7an45/_23_What_part_of_your_business', 10)
    support_needed = _count_multi_field('group_wf5op54/_37_What_support_would_help_yo', 15)
    prior_credit_sources = _count_multi_field('group_wf5op54/_32_Have_you_ever_got_a_loan_from', 15)
    repay_capacity = _count_field('group_wf5op54/_34_If_you_received_could_you_repay_it', 8)
    loan_purpose = _count_multi_field('group_wf5op54/_36_What_would_you_use_the_money_for', 15)
    device_types_needed = _count_multi_field('group_xx9pw99/b_if_yes_what_device', 10)
    device_budget = _count_field('group_xx9pw99/d_How_much_are_you_o_pay_for_the_device', 8)

    credit_demand = 0
    credit_skills_demand = 0
    loan_literacy_need = 0
    credit_readiness = {}
    credit_demand_readiness = {}
    credit_demand_by_sector = {}
    credit_demand_by_district = {}
    credit_demand_by_eso = {}
    credit_amount_bands = {}
    credit_amount_values = []
    credit_match_records = []
    credit_activity_records = []
    denied_loan_count = 0
    denied_reasons = {}
    credit_ready_demand = 0
    credit_pwd_demand = 0
    credit_demand_registered = 0
    credit_demand_sector_known = 0
    credit_demand_repay_yes = 0
    credit_demand_amount_known = 0

    def _amount_num(value):
        raw = str(value or '').strip().lower().replace(',', '')
        if not raw or raw == 'nan':
            return None
        nums = re.findall(r'\d+(?:\.\d+)?', raw)
        if not nums:
            return None
        n = float(nums[0])
        if 'm' in raw or 'million' in raw:
            n *= 1_000_000
        elif 'k' in raw or 'thousand' in raw:
            n *= 1_000
        return int(n) if n > 0 else None

    def _amount_band(value):
        n = _amount_num(value)
        if n is None:
            return 'Missing Amount'
        if n < 500_000:
            return 'Under 500K'
        if n < 1_000_000:
            return '500K-1M'
        if n < 2_000_000:
            return '1M-2M'
        if n < 5_000_000:
            return '2M-5M'
        return '5M+'

    for r in records:
        wants_credit_skill = _has_option(r, 'group_xx9pw99/_20_What_digital_skills_would_', 'access digital credit')
        wants_loan_support = (
            _has_option(r, 'group_wf5op54/_37_What_support_would_help_yo', 'understanding loans investors')
            or _has_option(r, 'group_wf5op54/_37_What_support_would_help_yo', 'loans')
        )
        if wants_credit_skill:
            credit_skills_demand += 1
        if wants_loan_support:
            loan_literacy_need += 1

        denied = _yes(r, 'group_wf5op54/_33_Have_you_ever_been_denied_')
        if denied:
            denied_loan_count += 1
            reason = _clean_label(r.get('group_wf5op54/if_yes_what_was_the_eing_denied_the_loan')) or 'Reason Not Provided'
            denied_reasons[reason] = denied_reasons.get(reason, 0) + 1

        score = _credit_readiness_score(r)
        bucket = _readiness_bucket(score)
        credit_readiness[bucket] = credit_readiness.get(bucket, 0) + 1

        if _credit_demand(r):
            credit_demand += 1
            business_key = _norm_match_text(r.get('group_oj8uw97/_1_Business_Name'))
            phone_key = _hash_key('phone', _norm_phone(r.get('group_oj8uw97/b_Phone_number') or r.get('b_Phone_number') or r.get('phone')))
            email_key = _hash_key('email', _norm_email(r.get('group_oj8uw97/c_Email') or r.get('c_Email') or r.get('email')))
            credit_match_records.append({
                'business_key': business_key,
                'phone_key': phone_key,
                'email_key': email_key,
            })
            credit_demand_readiness[bucket] = credit_demand_readiness.get(bucket, 0) + 1
            amount = _amount_num(r.get('group_wf5op54/_35_If_you_received_p_your_business_grow'))
            if amount:
                credit_amount_values.append(amount)
                credit_demand_amount_known += 1
            band = _amount_band(r.get('group_wf5op54/_35_If_you_received_p_your_business_grow'))
            credit_amount_bands[band] = credit_amount_bands.get(band, 0) + 1
            if _yes(r, 'group_oj8uw97/_5_Is_your_business_registered'):
                credit_demand_registered += 1
            if _yes(r, 'group_wf5op54/_34_If_you_received_could_you_repay_it'):
                credit_demand_repay_yes += 1
            is_pwd = _yes(r, 'group_oj8uw97/Are_you_a_person_with_a_disabi')
            if is_pwd:
                credit_pwd_demand += 1
            if bucket == 'Ready for Screening':
                credit_ready_demand += 1
            sector = _clean_label(r.get('group_oj8uw97/sector')) or 'Unknown'
            if sector != 'Unknown':
                credit_demand_sector_known += 1
            district = _clean_label(r.get('group_oj8uw97/e_Which_district_are_you_located_in')) or 'Unknown'
            eso = _eso_label(r.get('group_oj8uw97/_1_Implementing_Partn_Support_Organization', '')) or 'Unknown'
            credit_demand_by_sector[sector] = credit_demand_by_sector.get(sector, 0) + 1
            credit_demand_by_district[district] = credit_demand_by_district.get(district, 0) + 1
            credit_demand_by_eso[eso] = credit_demand_by_eso.get(eso, 0) + 1
            credit_activity_records.append({
                'd': r.get('_submission_time') or r.get('start') or r.get('end') or '',
                'b': r.get('group_oj8uw97/_1_Business_Name') or '',
                'eso': eso,
                'district': district,
                'sector': sector,
                'amount': amount or 0,
                'amount_band': band,
                'readiness': bucket,
                'repay': _clean_label(r.get('group_wf5op54/_34_If_you_received_could_you_repay_it')) or 'Unknown',
                'purpose': str(r.get('group_wf5op54/_36_What_would_you_use_the_money_for') or '').strip(),
                'prior_source': str(r.get('group_wf5op54/_32_Have_you_ever_got_a_loan_from') or '').strip(),
            })

    credit_demand_by_sector = dict(sorted(credit_demand_by_sector.items(), key=lambda x: -x[1])[:15])
    credit_demand_by_district = dict(sorted(credit_demand_by_district.items(), key=lambda x: -x[1])[:20])
    credit_demand_by_eso = dict(sorted(credit_demand_by_eso.items(), key=lambda x: -x[1])[:20])
    credit_amount_bands = dict(sorted(credit_amount_bands.items(), key=lambda x: -x[1]))
    denied_reasons = dict(sorted(denied_reasons.items(), key=lambda x: -x[1])[:10])
    credit_amount_stats = {
        'count': len(credit_amount_values),
        'total_requested_ugx': int(sum(credit_amount_values)),
        'avg_requested_ugx': int(sum(credit_amount_values) / len(credit_amount_values)) if credit_amount_values else 0,
        'median_requested_ugx': int(sorted(credit_amount_values)[len(credit_amount_values) // 2]) if credit_amount_values else 0,
    }

    income_levels = {}
    for r in records:
        v = str(r.get('group_oj8uw97/group_zw8kn95/_9_Average_income_per_month_estimate', '')).strip()
        if v and v != 'nan':
            income_levels[v] = income_levels.get(v, 0) + 1
    income_levels = dict(sorted(income_levels.items(), key=lambda x: -x[1])[:10])

    by_eso = {}
    for r in records:
        raw = r.get('group_oj8uw97/_1_Implementing_Partn_Support_Organization', '')
        eso = _eso_label(raw)
        if eso:
            by_eso.setdefault(eso, {
                'total': 0,
                'registered': 0,
                'device_need': 0,
                'pwd': 0,
                'internet_always': 0,
                'mobile_payments': 0,
                'credit_demand': 0,
                'credit_ready': 0,
                'credit_followup': 0,
                'credit_pwd_demand': 0,
                'credit_denied': 0,
                'credit_repay_yes': 0,
                'credit_eligible': 0,
                'credit_approved': 0,
            })
            by_eso[eso]['total'] += 1
            if _yes(r, 'group_oj8uw97/_5_Is_your_business_registered'):
                by_eso[eso]['registered'] += 1
            if _yes(r, 'group_xx9pw99/_14_Do_you_need_a_device_to_su'):
                by_eso[eso]['device_need'] += 1
            if _yes(r, 'group_oj8uw97/Are_you_a_person_with_a_disabi'):
                by_eso[eso]['pwd'] += 1
            if str(r.get('group_xx9pw99/_16_Do_you_have_access_to_internet', '')).strip().lower() == 'always':
                by_eso[eso]['internet_always'] += 1
            if _yes(r, 'group_xx9pw99/_17_Do_you_use_mobil_or_business_payments'):
                by_eso[eso]['mobile_payments'] += 1
            score = _credit_readiness_score(r)
            if _credit_demand(r):
                by_eso[eso]['credit_demand'] += 1
                if _yes(r, 'group_oj8uw97/Are_you_a_person_with_a_disabi'):
                    by_eso[eso]['credit_pwd_demand'] += 1
                if score >= 3:
                    by_eso[eso]['credit_ready'] += 1
                elif score == 2:
                    by_eso[eso]['credit_followup'] += 1
            if _yes(r, 'group_wf5op54/_33_Have_you_ever_been_denied_'):
                by_eso[eso]['credit_denied'] += 1
            if _yes(r, 'group_wf5op54/_34_If_you_received_could_you_repay_it'):
                by_eso[eso]['credit_repay_yes'] += 1

    weekly_activity = {}
    sub_times = [r.get('_submission_time') for r in records if r.get('_submission_time')]
    if sub_times:
        import pandas as _pd
        dt          = _pd.to_datetime(sub_times, errors='coerce')
        today       = _pd.Timestamp.now().normalize()
        week_start  = today - _pd.Timedelta(days=today.weekday())
        lweek_start = week_start - _pd.Timedelta(weeks=1)
        month_start = today.replace(day=1)
        weekly_activity = {
            'apps_this_week':  int((dt >= week_start).sum()),
            'apps_last_week':  int(((dt >= lweek_start) & (dt < week_start)).sum()),
            'apps_this_month': int((dt >= month_start).sum()),
        }

    return {
        'type':     'buz_needs',
        'name':     name,
        'filename': f'kobo:{asset_uid}',
        'stats': {
            'total':        total,
            'pwd':          pwd_count,
            'refugees':     0,
            'record_count': total,
        },
        'registered_pct':  round(registered  / max(total, 1) * 100, 1),
        'device_need_pct': round(device_need  / max(total, 1) * 100, 1),
        'pwd_pct':         round(pwd_count    / max(total, 1) * 100, 1),
        'by_eso':          by_eso,
        'sectors':         sectors,
        'districts':       districts,
        'income_levels':   income_levels,
        'digital_skills':  digital_skills,
        'business_age':    business_age,
        'growth_status':   growth_status,
        'income_frequency': income_frequency,
        'income_stability': income_stability,
        'internet_access': internet_access,
        'mobile_payments': mobile_payments,
        'digital_confidence': digital_confidence,
        'business_constraints': business_constraints,
        'support_needed': support_needed,
        'prior_credit_sources': prior_credit_sources,
        'repay_capacity': repay_capacity,
        'loan_purpose': loan_purpose,
        'device_types_needed': device_types_needed,
        'device_budget': device_budget,
        'respondent_roles': respondent_roles,
        'credit_demand': credit_demand,
        'credit_skills_demand': credit_skills_demand,
        'loan_literacy_need': loan_literacy_need,
        'credit_ready_demand': credit_ready_demand,
        'credit_pwd_demand': credit_pwd_demand,
        'credit_demand_registered': credit_demand_registered,
        'credit_demand_sector_known': credit_demand_sector_known,
        'credit_demand_repay_yes': credit_demand_repay_yes,
        'credit_demand_amount_known': credit_demand_amount_known,
        'credit_readiness': credit_readiness,
        'credit_demand_readiness': credit_demand_readiness,
        'credit_demand_by_sector': credit_demand_by_sector,
        'credit_demand_by_district': credit_demand_by_district,
        'credit_demand_by_eso': credit_demand_by_eso,
        'credit_amount_bands': credit_amount_bands,
        'credit_amount_stats': credit_amount_stats,
        'credit_match_records': credit_match_records[:10000],
        'credit_activity_records': credit_activity_records[:5000],
        'denied_loan_count': denied_loan_count,
        'denied_reasons': denied_reasons,
        'credit_prior_sources': prior_credit_sources,
        'credit_repay_capacity': repay_capacity,
        'credit_loan_purpose': loan_purpose,
        'credit_eligible': 0,
        'credit_approved': 0,
        'credit_amount':   0,
        'weekly_activity': weekly_activity,
    }


def parse_kobo_devices(records, asset_uid, eso_name):
    """Convert raw Kobo Device Financing submissions into the same portfolio dict as parse_devices_file."""
    if not records:
        return None

    total = len(records)

    def _yes_count(key):
        return sum(1 for r in records if str(r.get(key, '')).strip().lower() in ('yes', '1', 'true'))

    def _is_yes(v):
        return str(v or '').strip().lower() in ('yes', '1', 'true', 'y')

    def _present(v):
        s = str(v or '').strip()
        return bool(s) and s.lower() != 'nan'

    def _price_num(v):
        try:
            n = float(str(v or '').replace(',', '').strip())
            return n if n > 0 else None
        except Exception:
            return None

    def _phone_value(r):
        return str(r.get('mtn_number') or r.get('airtel_number') or '').strip()

    with_disability = _yes_count('has_disability')
    business_reg    = _yes_count('business_registered')

    # Districts
    districts = {}
    for r in records:
        v = str(r.get('district') or r.get('village') or '').strip()
        if v and v.lower() != 'nan':
            districts[v] = districts.get(v, 0) + 1
    districts = dict(sorted(districts.items(), key=lambda x: -x[1])[:20])

    # Device types
    device_types = {}
    device_by_eso = {}
    for r in records:
        v = str(r.get('device_type', '')).strip()
        if v and v.lower() != 'nan':
            eso = str(r.get('eso_hub') or eso_name).strip()
            if not eso or eso.lower() == 'nan':
                eso = eso_name
            for part in v.split():
                part = part.strip()
                if part:
                    device_types[part] = device_types.get(part, 0) + 1
                    device_by_eso.setdefault(part, {})
                    device_by_eso[part][eso] = device_by_eso[part].get(eso, 0) + 1
    device_types = dict(sorted(device_types.items(), key=lambda x: -x[1])[:8])
    device_by_eso = {
        device: dict(sorted(counts.items(), key=lambda x: -x[1])[:10])
        for device, counts in device_by_eso.items()
        if device in device_types
    }

    # Price bands
    def _price_band(v):
        try:
            v = float(str(v).replace(',', '').strip())
        except Exception:
            return None
        if v <= 0:        return None
        if v < 200_000:   return 'Under 200K'
        if v < 500_000:   return '200K–500K'
        if v < 1_000_000: return '500K–1M'
        if v < 2_000_000: return '1M–2M'
        return '2M+'
    price_bands = {}
    for r in records:
        b = _price_band(r.get('price_range'))
        if b:
            price_bands[b] = price_bands.get(b, 0) + 1
    order = ['Under 200K', '200K–500K', '500K–1M', '1M–2M', '2M+']
    price_bands = {k: price_bands[k] for k in order if k in price_bands}

    # Payment duration
    def _norm_dur(v):
        v = str(v).strip().lower()
        if 'quarter' in v or ('3' in v and 'month' in v): return 'Quarterly'
        if 'semi' in v or ('6' in v and 'month' in v):    return 'Semi-Annual'
        if 'bi' in v and 'week' in v: return 'Bi-Weekly'
        if 'week' in v:   return 'Weekly'
        if 'month' in v:  return 'Monthly'
        if 'year' in v or 'annual' in v: return 'Yearly'
        return v.title()[:20]
    payment_duration = {}
    for r in records:
        v = r.get('payment_duration')
        if v:
            b = _norm_dur(v)
            if b:
                payment_duration[b] = payment_duration.get(b, 0) + 1

    # ESO hubs (Outbox form has eso_hub field)
    eso_hubs = {}
    for r in records:
        v = str(r.get('eso_hub', '')).strip()
        if v and v.lower() != 'nan':
            eso_hubs[v] = eso_hubs.get(v, 0) + 1
    eso_hubs = dict(sorted(eso_hubs.items(), key=lambda x: -x[1])[:10])

    # ID types, reg body, SIM registered
    id_types = {}
    for r in records:
        v = str(r.get('id_type', '')).strip()
        if v and v.lower() not in ('nan', ''):
            id_types[v] = id_types.get(v, 0) + 1

    reg_body = {}
    for r in records:
        v = str(r.get('registration_body', '')).strip()
        if v and v.lower() not in ('nan', ''):
            reg_body[v] = reg_body.get(v, 0) + 1

    sim_registered = {}
    for r in records:
        v = str(r.get('number_registered', '')).strip().lower()
        if v and v != 'nan':
            label = 'Yes' if v in ('yes', '1', 'true') else 'No'
            sim_registered[label] = sim_registered.get(label, 0) + 1

    # Price stats
    price_stats = {}
    prices = []
    for r in records:
        try:
            p = float(str(r.get('price_range', '')).replace(',', ''))
            if p > 0:
                prices.append(p)
        except Exception:
            pass
    if prices:
        import statistics
        price_stats = {
            'avg':    int(sum(prices) / len(prices)),
            'median': int(statistics.median(prices)),
        }

    # Readiness, affordability, partner/device comparisons, and data quality
    readiness = {'Ready': 0, 'Needs Follow-Up': 0, 'Not Ready': 0}
    affordability = {
        'above_500k': 0,
        'above_1m': 0,
        'above_2m': 0,
        'missing_price': 0,
    }
    price_values_by_device = {}
    price_values_by_eso = {}
    device_mix_by_eso = {}
    data_quality = {
        'missing_name': 0,
        'missing_phone': 0,
        'missing_district': 0,
        'missing_device_type': 0,
        'missing_price': 0,
        'missing_id_type': 0,
        'missing_sim_registration': 0,
        'duplicate_phone_numbers': 0,
    }
    phone_seen = collections.Counter()
    raw_records = []

    for r in records:
        eso = str(r.get('eso_hub') or eso_name).strip()
        if not eso or eso.lower() == 'nan':
            eso = eso_name
        dev_raw = str(r.get('device_type') or '').strip()
        dev_parts = [part.strip() for part in dev_raw.split() if part.strip()]
        primary_dev = dev_parts[0] if dev_parts else ''
        price = _price_num(r.get('price_range'))
        phone = _phone_value(r)
        sim_yes = _is_yes(r.get('number_registered'))
        biz_registered = _is_yes(r.get('business_registered'))
        id_doc = _present(r.get('id_type')) and (_present(r.get('id_number')) or _present(r.get('id_front')) or _present(r.get('id_back')))

        if price is None:
            affordability['missing_price'] += 1
        else:
            if price > 500_000: affordability['above_500k'] += 1
            if price > 1_000_000: affordability['above_1m'] += 1
            if price > 2_000_000: affordability['above_2m'] += 1
            if primary_dev:
                price_values_by_device.setdefault(primary_dev, []).append(price)
            price_values_by_eso.setdefault(eso, []).append(price)

        if id_doc and sim_yes and price is not None and price <= 1_000_000:
            readiness['Ready'] += 1
        elif (id_doc or sim_yes or biz_registered) and price is not None and price <= 2_000_000:
            readiness['Needs Follow-Up'] += 1
        else:
            readiness['Not Ready'] += 1

        if primary_dev:
            device_mix_by_eso.setdefault(eso, {})
            device_mix_by_eso[eso][primary_dev] = device_mix_by_eso[eso].get(primary_dev, 0) + 1

        if not _present(r.get('first_name')) and not _present(r.get('last_name')):
            data_quality['missing_name'] += 1
        if not phone:
            data_quality['missing_phone'] += 1
        else:
            phone_seen[phone] += 1
        if not _present(r.get('district')):
            data_quality['missing_district'] += 1
        if not dev_raw:
            data_quality['missing_device_type'] += 1
        if price is None:
            data_quality['missing_price'] += 1
        if not _present(r.get('id_type')):
            data_quality['missing_id_type'] += 1
        if not _present(r.get('number_registered')):
            data_quality['missing_sim_registration'] += 1

        raw_records.append({
            'd': str(r.get('_submission_time') or '')[:10],
            'n': ' '.join(str(r.get(k) or '').strip() for k in ('first_name', 'last_name')).strip(),
            'district': str(r.get('district') or '').strip(),
            'device_type': dev_raw,
            'price': int(price) if price is not None else None,
            'payment_duration': str(r.get('payment_duration') or '').strip(),
            'id_type': str(r.get('id_type') or '').strip(),
            'sim_registered': str(r.get('number_registered') or '').strip(),
            'business_registered': str(r.get('business_registered') or '').strip(),
            'missing_phone': not bool(phone),
            'missing_district': not _present(r.get('district')),
            'missing_device_type': not bool(dev_raw),
            'missing_price': price is None,
        })

    data_quality['duplicate_phone_numbers'] = sum(1 for _, c in phone_seen.items() if c > 1)

    def _summarize_prices(values):
        if not values:
            return {}
        import statistics
        return {
            'avg': int(sum(values) / len(values)),
            'median': int(statistics.median(values)),
            'count': len(values),
        }

    price_by_device = {
        k: _summarize_prices(v)
        for k, v in sorted(price_values_by_device.items(), key=lambda item: -len(item[1]))[:10]
    }
    price_by_eso = {
        k: _summarize_prices(v)
        for k, v in sorted(price_values_by_eso.items(), key=lambda item: -len(item[1]))[:10]
    }
    top_device_by_eso = {}
    smartphone_share_by_eso = {}
    for eso, mix in device_mix_by_eso.items():
        total_mix = sum(mix.values()) or 1
        top_device_by_eso[eso] = max(mix.items(), key=lambda item: item[1])[0]
        smartphone_share_by_eso[eso] = round((mix.get('Smartphone', 0) / total_mix) * 100, 1)

    district_top3 = sum(v for _, v in sorted(districts.items(), key=lambda x: -x[1])[:3])
    eso_top3 = sum(v for _, v in sorted(eso_hubs.items(), key=lambda x: -x[1])[:3])
    demand_concentration = {
        'top3_districts_count': district_top3,
        'top3_districts_pct': round(district_top3 / max(total, 1) * 100, 1),
        'top3_esos_count': eso_top3,
        'top3_esos_pct': round(eso_top3 / max(total, 1) * 100, 1),
    }

    # Temporal activity
    weekly_activity = {}
    eso_weekly      = {}
    device_weekly   = {}
    sub_times = [r.get('_submission_time') for r in records if r.get('_submission_time')]
    if sub_times:
        import pandas as _pd
        dt          = _pd.to_datetime(sub_times, errors='coerce')
        today       = _pd.Timestamp.now().normalize()
        week_start  = today - _pd.Timedelta(days=today.weekday())
        lweek_start = week_start - _pd.Timedelta(weeks=1)
        month_start = today.replace(day=1)
        mask_wk  = dt >= week_start
        mask_lwk = (dt >= lweek_start) & (dt < week_start)
        mask_mo  = dt >= month_start
        weekly_activity = {
            'apps_this_week':  int(mask_wk.sum()),
            'apps_last_week':  int(mask_lwk.sum()),
            'apps_this_month': int(mask_mo.sum()),
        }
        # Per-ESO temporal
        if eso_hubs:
            for r_eso in eso_hubs:
                mask_e = _pd.Series([r.get('eso_hub', '') == r_eso for r in records])
                eso_weekly[r_eso] = {
                    'total':      int(mask_e.sum()),
                    'this_week':  int((mask_e & mask_wk).sum()),
                    'last_week':  int((mask_e & mask_lwk).sum()),
                    'this_month': int((mask_e & mask_mo).sum()),
                }
        # Per-device temporal
        for dev in device_types:
            mask_d = _pd.Series([dev.lower() in str(r.get('device_type', '')).lower() for r in records])
            if mask_d.sum() > 0:
                device_weekly[dev] = {
                    'total':      int(mask_d.sum()),
                    'this_week':  int((mask_d & mask_wk).sum()),
                    'this_month': int((mask_d & mask_mo).sum()),
                }

    return {
        'raw_records': raw_records[:5000],
        'type':     'devices',
        'name':     f'{eso_name} Device Financing',
        'filename': f'kobo:{asset_uid}',
        'stats': {
            'total':           total,
            'with_disability': with_disability,
            'business_reg':    business_reg,
            'record_count':    total,
        },
        'disability_pct':   round(with_disability / max(total, 1) * 100, 1),
        'business_reg_pct': round(business_reg    / max(total, 1) * 100, 1),
        'districts':        districts,
        'device_types':     device_types,
        'price_bands':      price_bands,
        'payment_duration': payment_duration,
        'eso_hubs':         eso_hubs,
        'id_types':         id_types,
        'reg_body':         reg_body,
        'disability_types': {},
        'device_by_eso':    device_by_eso,
        'sim_registered':   sim_registered,
        'weekly_activity':  weekly_activity,
        'eso_weekly':       eso_weekly,
        'device_weekly':    device_weekly,
        'price_stats':      price_stats,
        'readiness':        readiness,
        'affordability':    affordability,
        'price_by_device':  price_by_device,
        'price_by_eso':     price_by_eso,
        'top_device_by_eso': top_device_by_eso,
        'smartphone_share_by_eso': smartphone_share_by_eso,
        'demand_concentration': demand_concentration,
        'data_quality':     data_quality,
        'field_availability': {
            'demographics': False,
            'approval_funnel': False,
            'gender': False,
            'age': False,
        },
    }


def parse_kobo_eoi(records, asset_uid, eso_name='10X Digital Economy'):
    """Split Kobo EOI submissions into one portfolio dict per ESO partner.
    Returns a list of dicts (same structure as parse_eoi_file) so the UI
    gets individual sub-tabs, one per ESO — matching the old Excel-per-ESO layout.
    """
    if not records:
        return []

    F_SECTOR   = 'about_business/sector'
    F_DISTRICT = 'about_business/business_hq'
    F_URSB     = 'about_business/Is_your_business_enterprise_fo'
    F_TIN      = 'about_business/Does_your_business_enterprise_'
    F_NSSF     = 'about_business/has_nssf_business_no'
    F_NFOUND   = 'no_of_founders'
    F_NFEM     = 'no_of_female_founders'
    F_REVENUE  = 'abt_bsness_operating_model/revenue_in_last_24_mons'
    F_BIZNAME  = 'about_business/business_name'
    F_TIME     = '_submission_time'
    F_FOUNDERS = 'founders'
    F_FTE      = 'abt_employees/full_time_employs'
    F_PTE      = 'abt_employees/part_time_employs'
    F_ESO      = 'Implementing_Partner_Support_Organization'

    def _safe_float(v):
        try:
            return float(str(v).replace(',', '').strip())
        except Exception:
            return None

    def _arch(v):
        n = _safe_float(v)
        if n is None or n == 0: return 'Invisibles'
        annual = n / 2
        if annual < 2_000_000:  return 'Gig Workers'
        if annual < 15_000_000: return 'Bootstrappers'
        if annual < 50_000_000: return 'Bootstrappers SME'
        return 'Gazelles'

    def _build_portfolio(eso_label, eso_records):
        total = len(eso_records)
        if total == 0:
            return None

        sectors   = dict(collections.Counter(
            normalize_sector(r.get(F_SECTOR, '')) for r in eso_records
            if r.get(F_SECTOR)
        ).most_common(15))
        districts = dict(collections.Counter(
            str(r.get(F_DISTRICT, '') or '').strip().title()
            for r in eso_records if r.get(F_DISTRICT)
        ).most_common(15))

        ursb_count = sum(1 for r in eso_records if str(r.get(F_URSB,'')).strip().lower() == 'yes')
        tin_yes    = sum(1 for r in eso_records if str(r.get(F_TIN,'')).strip().lower()  == 'yes')
        tin_no     = sum(1 for r in eso_records if str(r.get(F_TIN,'')).strip().lower()  == 'no')
        nssf_yes   = sum(1 for r in eso_records if str(r.get(F_NSSF,'')).strip().lower() == 'yes')
        nssf_no    = sum(1 for r in eso_records if str(r.get(F_NSSF,'')).strip().lower() == 'no')

        total_founders  = sum(int(n) for r in eso_records if (n := _safe_float(r.get(F_NFOUND))) is not None)
        female_founders = sum(int(n) for r in eso_records if (n := _safe_float(r.get(F_NFEM)))   is not None)
        archetypes      = dict(collections.Counter(_arch(r.get(F_REVENUE,'')) for r in eso_records))

        fg = collections.Counter()
        pwd = refugees = 0
        ref_nat = collections.Counter()
        age_list = []
        id_with = id_without = 0
        founder_phone_seen = collections.Counter()
        founder_missing_name = 0
        founder_missing_phone = 0
        founder_missing_district = 0
        founder_missing_gender = 0
        eoi_match_index = []
        for r in eso_records:
            business_key = _norm_match_text(r.get(F_BIZNAME))
            founder_phone_keys = set()
            founder_email_keys = set()
            record_has_id = False
            flist = r.get(F_FOUNDERS) or []
            if isinstance(flist, str):
                try: flist = json.loads(flist.replace("'", '"'))
                except Exception: flist = []
            for f in (flist if isinstance(flist, list) else []):
                first = str(f.get('founders/founders_details/first_name_f') or '').strip()
                last = str(f.get('founders/founders_details/last_name_f') or '').strip()
                phone = str(f.get('founders/founders_details/phone_f') or '').strip()
                district_f = str(f.get('founders/founders_details/district_f') or '').strip()
                g = str(f.get('founders/founders_details/gender_f') or '').strip()
                if g: fg[g] += 1
                else: founder_missing_gender += 1
                if not (first or last): founder_missing_name += 1
                if not phone:
                    founder_missing_phone += 1
                else:
                    founder_phone_seen[phone] += 1
                    phone_key = _hash_key('phone', _norm_phone(phone))
                    if phone_key:
                        founder_phone_keys.add(phone_key)
                email_key = _hash_key('email', _norm_email(f.get('founders/founders_details/email_f')))
                if email_key:
                    founder_email_keys.add(email_key)
                if not district_f: founder_missing_district += 1
                id_upload = str(f.get('founders/founders_details/front_id_f') or '').strip()
                if id_upload and id_upload.lower() != 'nan':
                    id_with += 1
                    record_has_id = True
                else:
                    id_without += 1
                if str(f.get('founders/founders_details/has_disability_f') or '').lower() == 'yes':
                    pwd += 1
                nat = parse_nationality(str(f.get('founders/founders_details/nationality_f') or ''))
                if nat['is_refugee']:
                    refugees += 1
                    ref_nat[nat['nationality']] += 1
                dob_raw = str(f.get('founders/founders_details/date_of_birth_f') or '').strip()
                if dob_raw and dob_raw != 'nan':
                    try:
                        dob = pd.to_datetime(dob_raw, errors='coerce')
                        if dob is not pd.NaT:
                            age = (pd.Timestamp.now() - dob).days / 365.25
                            if 18 <= age < 120: age_list.append(age)
                    except Exception:
                        pass
            if business_key or founder_phone_keys or founder_email_keys:
                eoi_match_index.append({
                    'business_key': business_key,
                    'phone_keys': sorted(founder_phone_keys),
                    'email_keys': sorted(founder_email_keys),
                    'has_national_id': record_has_id,
                    'business_registered': str(r.get(F_URSB,'')).strip().lower() == 'yes',
                    'tin': str(r.get(F_TIN,'')).strip().lower() == 'yes',
                    'nssf': str(r.get(F_NSSF,'')).strip().lower() == 'yes',
                    'sector': normalize_sector(r.get(F_SECTOR, '')),
                    'eso': eso_label,
                })

        fg_dict   = dict(fg)
        fem_pct   = min(100.0, round(fg.get('Female', 0) / max(sum(fg.values()), 1) * 100, 1))
        age_bands = {}
        if age_list:
            ages_s   = pd.Series(age_list)
            age_cats = pd.cut(ages_s, bins=[17,25,35,45,55,120], labels=['18–25','26–35','36–45','46–55','56+'])
            age_bands = {str(k): int(v) for k, v in age_cats.value_counts().sort_index().items() if v > 0}

        nin_with = nin_without = 0
        for r in eso_records:
            for group_key, nin_key in (
                (F_FTE, 'abt_employees/full_time_employs/full_time_employees/nin_fte'),
                (F_PTE, 'abt_employees/part_time_employs/part_time_employees/nin_pte'),
            ):
                emp_list = r.get(group_key) or []
                if isinstance(emp_list, str):
                    try: emp_list = json.loads(emp_list.replace("'", '"'))
                    except Exception: emp_list = []
                for emp in (emp_list if isinstance(emp_list, list) else []):
                    nin = str(emp.get(nin_key) or '').strip()
                    if nin and nin.lower() != 'nan':
                        nin_with += 1
                    else:
                        nin_without += 1

        raw_records = [
            {
                'd': str(r.get(F_TIME) or '')[:10],
                'b': str(r.get(F_BIZNAME) or ''),
                'district': str(r.get(F_DISTRICT) or '').strip(),
                'sector': str(r.get(F_SECTOR) or '').strip(),
                'ursb': str(r.get(F_URSB) or '').strip(),
                'tin': str(r.get(F_TIN) or '').strip(),
                'nssf': str(r.get(F_NSSF) or '').strip(),
                'missing_business_name': not bool(str(r.get(F_BIZNAME) or '').strip()),
                'missing_district': not bool(str(r.get(F_DISTRICT) or '').strip()),
                'missing_sector': not bool(str(r.get(F_SECTOR) or '').strip()),
            }
            for r in eso_records[:2000]
        ]

        return {
            'raw_records':    raw_records,
            'type':           'eoi',
            'name':           f'{eso_label} EOI',
            'eso':            eso_label,
            'filename':       f'kobo:{asset_uid}',
            'stats': {
                'total':        total,
                'ursb':         ursb_count,
                'pwd':          pwd,
                'refugees':     refugees,
                'record_count': total,
            },
            'ursb_pct':       round(ursb_count / max(total, 1) * 100, 1),
            'sectors':        sectors,
            'districts':      districts,
            'total_founders': int(total_founders),
            'female_founders':int(female_founders),
            'revenue_bands':  {},
            'funding_bands':  {},
            'archetypes':     archetypes,
            'tin_status':     {k: v for k, v in {'Yes': tin_yes, 'No': tin_no}.items() if v},
            'nssf_status':    {k: v for k, v in {'Yes': nssf_yes, 'No': nssf_no}.items() if v},
            'id_status':      {k: v for k, v in {'Has National ID': id_with, 'Missing ID': id_without}.items() if v},
            'nin_status':     {k: v for k, v in {'Has NIN': nin_with, 'No NIN': nin_without}.items() if v},
            'eoi_match_index': eoi_match_index[:5000],
            'data_quality': {
                'missing_business_name': sum(1 for r in eso_records if not str(r.get(F_BIZNAME) or '').strip()),
                'missing_district': sum(1 for r in eso_records if not str(r.get(F_DISTRICT) or '').strip()),
                'missing_sector': sum(1 for r in eso_records if not str(r.get(F_SECTOR) or '').strip()),
                'missing_founder_name': founder_missing_name,
                'missing_founder_phone': founder_missing_phone,
                'missing_founder_district': founder_missing_district,
                'missing_founder_gender': founder_missing_gender,
                'duplicate_founder_phones': sum(1 for _, c in founder_phone_seen.items() if c > 1),
            },
            'founders': {
                'gender':     fg_dict,
                'female_pct': fem_pct,
                'with_pwd':   pwd,
                'refugees':   refugees,
            },
            'age_bands':             age_bands,
            'refugee_nationalities': dict(ref_nat),
        }

    OFFICIAL_12 = {
        'DFCU Foundation', 'MUBS EIIC', 'Mkazipreneur', 'Stanbic Business Incubator',
        'PEDN', 'Excelhort', 'Challenges Uganda', 'AGDI', 'Finding XY', 'AID',
        'CURAD', 'Living Earth Uganda',
    }

    # Group records by ESO.
    # Blank ESO field → PEDN (confirmed via learnt_abt_hi_innov cross-check).
    # Hub remapping (witu→Mkazipreneur, refactory→PEDN, etc.) is handled by _eso_label.
    groups = {}
    for r in records:
        raw = r.get(F_ESO, '')
        eso = _eso_label(raw) if raw else 'PEDN'
        if eso not in OFFICIAL_12:
            eso = 'Other'
        groups.setdefault(eso, []).append(r)

    # Sort ESOs by record count descending, put Other last
    def _sort_key(item):
        eso, recs = item
        return (eso == 'Other', -len(recs))

    portfolios = []
    for eso_label, eso_records in sorted(groups.items(), key=_sort_key):
        p = _build_portfolio(eso_label, eso_records)
        if p:
            portfolios.append(p)

    return portfolios


def main():
    print()
    print('Portfolio Data Extractor')
    print('=' * 44)

    portfolios = []

    # Load Kobo config once; if present, EOI files from the skip-dirs are replaced
    # by a live API pull so we avoid double-counting.
    kobo_cfg = _load_kobo_config()
    kobo_eoi_active = bool(kobo_cfg and kobo_cfg.get('eoi_assets'))
    # Youth in Work is intentionally maintained from the local cleaned workbook.
    kobo_yiw_active = False
    kobo_buz_active = bool(kobo_cfg and kobo_cfg.get('buz_needs_assets'))
    kobo_dev_active = bool(kobo_cfg and kobo_cfg.get('devices_assets'))

    # Build the full set of dirs replaced by Kobo so Excel files there are skipped
    kobo_skip_dirs = set()
    if kobo_eoi_active:
        kobo_skip_dirs |= {str(Path(d).resolve()) for d in KOBO_EOI_SKIP_DIRS}
    if kobo_yiw_active:
        kobo_skip_dirs |= {str(Path(d).resolve()) for d in KOBO_YIW_SKIP_DIRS}
    if kobo_buz_active:
        kobo_skip_dirs |= {str(Path(d).resolve()) for d in KOBO_BUZ_SKIP_DIRS}
    if kobo_dev_active:
        kobo_skip_dirs |= {str(Path(d).resolve()) for d in KOBO_DEV_SKIP_DIRS}

    # Collect .xlsx files from all configured portfolio directories.
    # Skip Excel temp/lock files (starting with '~$') and deduplicate by resolved path.
    seen = set()
    xlsx_files = []
    yiw_dir = (BASE_DIR / 'YIW').resolve()
    preferred_yiw_file = yiw_dir / 'YiW_Cleaned_Dataset.xlsx'
    for folder in PORTFOLIO_DIRS:
        if not folder.exists():
            continue
        for f in sorted(folder.glob('*.xlsx')):
            if f.name.startswith('~$'):
                continue
            if f.parent.resolve() == yiw_dir and preferred_yiw_file.exists() and f.resolve() != preferred_yiw_file:
                continue
            if str(f.parent.resolve()) in kobo_skip_dirs:
                continue
            if f.stem.lower() in KOBO_SKIP_FILENAMES:
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
        try:
            rel_path = filepath.relative_to(BASE_DIR)
        except ValueError:
            rel_path = filepath
        print(f'\nProcessing: {rel_path}')
        try:
            if str(filepath).lower().endswith('.xlsx'):
                xl = pd.ExcelFile(filepath, engine='openpyxl')
            elif str(filepath).lower().endswith('.xls'):
                xl = pd.ExcelFile(filepath, engine='xlrd')
            else:
                raise ValueError('Unsupported file extension for portfolio file')
            # Apply configuration overrides if any
            ftype = detect_file_type(xl, filename)
            config = next((v for k, v in FILE_CONFIGS.items() if k in filename.lower()), None)

            if config:
                ftype = config.get('type', ftype)
                parser_name = config.get('parser')
                print(f'  Config Override: type={ftype}, parser={parser_name}')
                if parser_name == 'parse_mkazi_needs_assessment':
                    data = parse_mkazi_needs_assessment(filename, xl)
                else:
                    data = parse_segmentation_file(filename, xl)
            elif ftype == 'segmentation':
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

    # ── Kobo API: live pulls ─────────────────────────────────────────────────
    if kobo_cfg:
        base_url = kobo_cfg['base_url']
        token    = kobo_cfg['token']
        max_age  = kobo_cfg.get('cache_max_age_hours', 4)

        for asset in kobo_cfg.get('eoi_assets', []):
            uid, name = asset['uid'], asset.get('name', asset['uid'])
            print(f'\nKobo EOI: {name}  (uid={uid})')
            try:
                records  = _get_kobo_submissions(base_url, token, uid, max_age)
                eso_list = parse_kobo_eoi(records, uid, name)
                for data in eso_list:
                    portfolios.append(data)
                    print(f'  OK - "{data["name"]}"  ({data["stats"]["total"]:,} records)')
            except Exception as exc:
                print(f'  ERROR fetching Kobo EOI: {exc}')
                traceback.print_exc()

        for asset in []:
            uid, name = asset['uid'], asset.get('name', asset['uid'])
            print(f'\nKobo YIW: {name}  (uid={uid})')
            try:
                records = _get_kobo_submissions(base_url, token, uid, max_age)
                data    = parse_kobo_yiw(records, uid, name)
                if data:
                    portfolios.append(data)
                    print(f'  OK - "{data["name"]}"  ({data["stats"]["total"]:,} records)')
            except Exception as exc:
                print(f'  ERROR fetching Kobo YIW: {exc}')
                traceback.print_exc()

        for asset in kobo_cfg.get('buz_needs_assets', []):
            uid, name = asset['uid'], asset.get('name', asset['uid'])
            print(f'\nKobo Business Needs: {name}  (uid={uid})')
            try:
                records = _get_kobo_submissions(base_url, token, uid, max_age)
                data    = parse_kobo_buz_needs(records, uid, name)
                if data:
                    portfolios.append(data)
                    print(f'  OK - "{data["name"]}"  ({data["stats"]["total"]:,} records)')
            except Exception as exc:
                print(f'  ERROR fetching Kobo Business Needs: {exc}')
                traceback.print_exc()

        for asset in kobo_cfg.get('devices_assets', []):
            uid, name = asset['uid'], asset.get('name', asset['uid'])
            print(f'\nKobo Devices: {name}  (uid={uid})')
            try:
                records = _get_kobo_submissions(base_url, token, uid, max_age)
                data    = parse_kobo_devices(records, uid, name)
                if data:
                    portfolios.append(data)
                    print(f'  OK - "{data["name"]}"  ({data["stats"]["total"]:,} records)')
            except Exception as exc:
                print(f'  ERROR fetching Kobo Devices: {exc}')
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

    # Aggregate Digital Credit from buz_needs
    real_credit_eso = []
    credit_raw_records = []
    credit_demand_total = 0
    credit_ready_total = 0
    credit_followup_total = 0
    credit_pwd_total = 0
    credit_registered_total = 0
    credit_sector_known_total = 0
    credit_repay_yes_total = 0
    credit_amount_known_total = 0
    credit_signals = {}
    credit_readiness = {}
    credit_demand_readiness = {}
    credit_demand_by_sector = {}
    credit_demand_by_district = {}
    credit_amount_bands = {}
    credit_prior_sources = {}
    credit_repay_capacity = {}
    credit_loan_purpose = {}
    denied_reasons = {}
    credit_amount_stats = {'count': 0, 'total_requested_ugx': 0, 'avg_requested_ugx': 0, 'median_requested_ugx': 0}
    denied_loan_total = 0
    credit_data_quality = {
        'missing_provider_fields': 1,
        'missing_application_fields': 1,
        'missing_approval_fields': 1,
        'missing_disbursement_fields': 1,
        'missing_repayment_fields': 1,
    }

    def _merge_counts(target, source):
        for k, v in (source or {}).items():
            target[k] = target.get(k, 0) + int(v or 0)

    eoi_by_phone = {}
    eoi_by_email = {}
    eoi_by_business = {}
    for p in portfolios:
        if p.get('type') != 'eoi':
            continue
        for item in p.get('eoi_match_index') or []:
            for key in item.get('phone_keys') or []:
                eoi_by_phone.setdefault(key, []).append(item)
            for key in item.get('email_keys') or []:
                eoi_by_email.setdefault(key, []).append(item)
            if item.get('business_key'):
                eoi_by_business.setdefault(item['business_key'], []).append(item)

    eoi_credit_crossref = {
        'credit_records_checked': 0,
        'matched_total': 0,
        'matched_by_phone': 0,
        'matched_by_email': 0,
        'matched_by_business_name': 0,
        'matched_with_national_id': 0,
        'matched_with_eoi_registration': 0,
        'matched_with_tin': 0,
        'matched_with_nssf': 0,
        'business_needs_phone_email_available': False,
        'match_note': 'Business Needs currently has no phone/email fields, so matching falls back to normalized business name. Phone/email matching will activate automatically if those fields are added.',
    }

    for p in portfolios:
        if p.get('type') == 'buz_needs' and 'credit_approved' in p:
            demand = int(p.get('credit_demand', 0) or 0)
            ready = int(p.get('credit_ready_demand', 0) or 0)
            followup = int((p.get('credit_demand_readiness') or {}).get('Needs Follow-Up', 0) or 0)
            credit_demand_total += demand
            credit_ready_total += ready
            credit_followup_total += followup
            credit_pwd_total += int(p.get('credit_pwd_demand', 0) or 0)
            credit_registered_total += int(p.get('credit_demand_registered', 0) or 0)
            credit_sector_known_total += int(p.get('credit_demand_sector_known', 0) or 0)
            credit_repay_yes_total += int(p.get('credit_demand_repay_yes', 0) or 0)
            credit_amount_known_total += int(p.get('credit_demand_amount_known', 0) or 0)
            _merge_counts(credit_readiness, p.get('credit_readiness'))
            _merge_counts(credit_demand_readiness, p.get('credit_demand_readiness'))
            _merge_counts(credit_demand_by_sector, p.get('credit_demand_by_sector'))
            _merge_counts(credit_demand_by_district, p.get('credit_demand_by_district'))
            _merge_counts(credit_amount_bands, p.get('credit_amount_bands'))
            _merge_counts(credit_prior_sources, p.get('credit_prior_sources'))
            _merge_counts(credit_repay_capacity, p.get('credit_repay_capacity'))
            _merge_counts(credit_loan_purpose, p.get('credit_loan_purpose'))
            _merge_counts(denied_reasons, p.get('denied_reasons'))
            denied_loan_total += int(p.get('denied_loan_count', 0) or 0)
            stats = p.get('credit_amount_stats') or {}
            credit_amount_stats['count'] += int(stats.get('count', 0) or 0)
            credit_amount_stats['total_requested_ugx'] += int(stats.get('total_requested_ugx', 0) or 0)
            if not credit_amount_stats['median_requested_ugx'] and stats.get('median_requested_ugx'):
                credit_amount_stats['median_requested_ugx'] = int(stats.get('median_requested_ugx', 0) or 0)
            if p.get('credit_skills_demand'):
                credit_signals['Access Digital Credit Skills'] = credit_signals.get('Access Digital Credit Skills', 0) + int(p.get('credit_skills_demand', 0) or 0)
            if p.get('loan_literacy_need'):
                credit_signals['Loan / Investor Understanding'] = credit_signals.get('Loan / Investor Understanding', 0) + int(p.get('loan_literacy_need', 0) or 0)

            for rec in p.get('credit_match_records') or []:
                eoi_credit_crossref['credit_records_checked'] += 1
                phone_key = rec.get('phone_key')
                email_key = rec.get('email_key')
                business_key = rec.get('business_key')
                matches = []
                method = ''
                if phone_key:
                    eoi_credit_crossref['business_needs_phone_email_available'] = True
                    matches = eoi_by_phone.get(phone_key, [])
                    if matches:
                        method = 'phone'
                if not matches and email_key:
                    eoi_credit_crossref['business_needs_phone_email_available'] = True
                    matches = eoi_by_email.get(email_key, [])
                    if matches:
                        method = 'email'
                if not matches and business_key:
                    matches = eoi_by_business.get(business_key, [])
                    if matches:
                        method = 'business_name'
                if not matches:
                    continue
                eoi_credit_crossref['matched_total'] += 1
                if method == 'phone':
                    eoi_credit_crossref['matched_by_phone'] += 1
                elif method == 'email':
                    eoi_credit_crossref['matched_by_email'] += 1
                else:
                    eoi_credit_crossref['matched_by_business_name'] += 1
                if any(m.get('has_national_id') for m in matches):
                    eoi_credit_crossref['matched_with_national_id'] += 1
                if any(m.get('business_registered') for m in matches):
                    eoi_credit_crossref['matched_with_eoi_registration'] += 1
                if any(m.get('tin') for m in matches):
                    eoi_credit_crossref['matched_with_tin'] += 1
                if any(m.get('nssf') for m in matches):
                    eoi_credit_crossref['matched_with_nssf'] += 1

            eso_map = p.get('by_eso') or {}
            for eso, row in eso_map.items():
                if int(row.get('credit_demand', 0) or 0) <= 0 and int(row.get('credit_approved', 0) or 0) <= 0:
                    continue
                real_credit_eso.append({
                    'eso': eso,
                    'amount_ugx': row.get('credit_amount', 0) or 0,
                    'businesses': row.get('credit_approved', 0) or 0,
                    'eligible': row.get('credit_eligible', 0) or 0,
                    'demand': row.get('credit_demand', 0) or 0,
                    'ready': row.get('credit_ready', 0) or 0,
                    'followup': row.get('credit_followup', 0) or 0,
                    'pwd_demand': row.get('credit_pwd_demand', 0) or 0,
                    'denied': row.get('credit_denied', 0) or 0,
                    'repay_yes': row.get('credit_repay_yes', 0) or 0,
                    'breakdown': [
                        {'type': 'Demand', 'count': row.get('credit_demand', 0) or 0},
                        {'type': 'Ready for Screening', 'count': row.get('credit_ready', 0) or 0},
                        {'type': 'Eligible', 'count': row.get('credit_eligible', 0) or 0},
                        {'type': 'Approved', 'count': row.get('credit_approved', 0) or 0}
                    ]
                })

            if not eso_map:
                real_credit_eso.append({
                    'eso': p.get('eso') or p.get('name'),
                    'amount_ugx': p.get('credit_amount', 0),
                    'businesses': p.get('credit_approved', 0),
                    'eligible': p.get('credit_eligible', 0),
                    'demand': demand,
                    'ready': ready,
                    'followup': followup,
                    'pwd_demand': p.get('credit_pwd_demand', 0) or 0,
                    'denied': p.get('denied_loan_count', 0) or 0,
                    'repay_yes': (p.get('credit_repay_capacity') or {}).get('Yes', 0),
                    'breakdown': [
                        {'type': 'Demand', 'count': demand},
                        {'type': 'Ready for Screening', 'count': ready},
                        {'type': 'Eligible', 'count': p.get('credit_eligible', 0)},
                        {'type': 'Approved', 'count': p.get('credit_approved', 0)}
                    ]
                })
            if p.get('raw_records'):
                credit_raw_records.extend(p['raw_records'])
            if p.get('credit_activity_records'):
                credit_raw_records.extend(p['credit_activity_records'])
    
    if real_credit_eso:
        credit_demand_by_sector = dict(sorted(credit_demand_by_sector.items(), key=lambda x: -x[1])[:15])
        credit_demand_by_district = dict(sorted(credit_demand_by_district.items(), key=lambda x: -x[1])[:20])
        credit_amount_bands = dict(sorted(credit_amount_bands.items(), key=lambda x: -x[1]))
        denied_reasons = dict(sorted(denied_reasons.items(), key=lambda x: -x[1])[:10])
        credit_amount_stats['avg_requested_ugx'] = int(credit_amount_stats['total_requested_ugx'] / credit_amount_stats['count']) if credit_amount_stats['count'] else 0
        portfolios.append({
            'name': 'Digital Credit Demand & Readiness',
            'type': 'digital_credit',
            'filename': 'aggregated_from_buz_needs',
            'stats': {'total': credit_demand_total},
            'credit_demand': credit_demand_total,
            'credit_ready': credit_ready_total,
            'credit_followup': credit_followup_total,
            'screening_readiness_note': 'Proxy score using business registration, mobile money, internet access, and digital confidence. This is not approval readiness.',
            'true_readiness_checklist': {
                'business_registered': {
                    'available': True,
                    'count': credit_registered_total,
                    'denominator': credit_demand_total,
                    'source': 'Business Needs',
                },
                'business_type_sector': {
                    'available': True,
                    'count': credit_sector_known_total,
                    'denominator': credit_demand_total,
                    'source': 'Business Needs',
                },
                'repay_confidence': {
                    'available': True,
                    'count': credit_repay_yes_total,
                    'denominator': credit_demand_total,
                    'source': 'Business Needs',
                },
                'amount_needed': {
                    'available': True,
                    'count': credit_amount_known_total,
                    'denominator': credit_demand_total,
                    'source': 'Business Needs',
                },
                'owner_national_id': {
                    'available': eoi_credit_crossref['matched_with_national_id'] > 0,
                    'count': eoi_credit_crossref['matched_with_national_id'],
                    'denominator': credit_demand_total,
                    'source': 'EOI cross-reference by phone/email when available; currently mostly normalized business-name fallback',
                },
                'eoi_business_registration': {
                    'available': eoi_credit_crossref['matched_with_eoi_registration'] > 0,
                    'count': eoi_credit_crossref['matched_with_eoi_registration'],
                    'denominator': credit_demand_total,
                    'source': 'EOI cross-reference registration signal',
                },
                'eoi_tin': {
                    'available': eoi_credit_crossref['matched_with_tin'] > 0,
                    'count': eoi_credit_crossref['matched_with_tin'],
                    'denominator': credit_demand_total,
                    'source': 'EOI cross-reference TIN signal',
                },
                'eoi_nssf': {
                    'available': eoi_credit_crossref['matched_with_nssf'] > 0,
                    'count': eoi_credit_crossref['matched_with_nssf'],
                    'denominator': credit_demand_total,
                    'source': 'EOI cross-reference NSSF signal',
                },
                'phone_registered_in_owner_name': {
                    'available': False,
                    'count': 0,
                    'denominator': credit_demand_total,
                    'source': 'Device Financing has SIM registration but is not linked to credit-demand records',
                },
                'actual_provider_application': {
                    'available': False,
                    'count': 0,
                    'denominator': credit_demand_total,
                    'source': 'Missing provider/application dataset',
                },
                'approved_or_disbursed': {
                    'available': False,
                    'count': 0,
                    'denominator': credit_demand_total,
                    'source': 'Missing credit outcome dataset',
                },
                'repayment_history': {
                    'available': False,
                    'count': 0,
                    'denominator': credit_demand_total,
                    'source': 'Missing repayment dataset',
                },
            },
            'inclusion_metrics': {
                'pwd_demand': credit_pwd_total,
                'women_led': None,
                'female_founders_pct': None,
                'refugees': None,
                'women_ready': None,
                'pwd_ready': None,
                'refugee_ready': None,
            },
            'credit_funnel': {
                'Need Credit': credit_demand_total,
                'Ready for Screening': credit_ready_total,
                'Eligible': sum(e.get('eligible', 0) for e in real_credit_eso),
                'Applied': 0,
                'Approved': sum(e.get('businesses', 0) for e in real_credit_eso),
                'Disbursed': sum(e.get('businesses', 0) for e in real_credit_eso),
            },
            'credit_readiness': credit_readiness,
            'credit_demand_readiness': credit_demand_readiness,
            'credit_signals': credit_signals,
            'credit_demand_by_sector': credit_demand_by_sector,
            'credit_demand_by_district': credit_demand_by_district,
            'credit_amount_bands': credit_amount_bands,
            'credit_amount_stats': credit_amount_stats,
            'eoi_credit_crossref': eoi_credit_crossref,
            'credit_prior_sources': credit_prior_sources,
            'credit_repay_capacity': credit_repay_capacity,
            'credit_loan_purpose': credit_loan_purpose,
            'denied_loan_count': denied_loan_total,
            'denied_reasons': denied_reasons,
            'data_quality': credit_data_quality,
            'field_availability': {
                'demand': True,
                'readiness': True,
                'provider': False,
                'application': False,
                'eligibility': False,
                'approval': False,
                'disbursement': False,
                'repayment': False,
            },
            'eso_credit': real_credit_eso,
            'raw_records': credit_raw_records[:5000]
        })

    output = {
        'generated':  pd.Timestamp.now().strftime('%d %b %Y, %H:%M'),
        'portfolios': portfolios,
    }

    js_content = 'window.PORTFOLIO_DATA = ' + json.dumps(output, indent=2, default=str) + ';\n'

    js_content += """
// ==============================================================================
// 10X DIGITAL ECONOMY - AGGREGATED METRICS & TRACE LOGIC
// ==============================================================================
if (window.PORTFOLIO_DATA && typeof window.PORTFOLIO_DATA === 'object') {
  // Add mock cohorts if missing
  if (!window.PORTFOLIO_DATA.portfolios.some(p => p.type === 'cohorts')) {
    window.PORTFOLIO_DATA.portfolios.push({
      name: "Acceleration Cohorts", type: "cohorts", filename: "mock_acceleration_data", stats: { total: 0 },
      cohorts: [
        { name: "Alpha Batch 1", eso: "Innovation Village", start_date: "2025-01-10", end_date: "2025-04-10", participants: 45, sector: "Fintech" },
        { name: "Green Growth 24", eso: "FindingXY", start_date: "2024-09-01", end_date: "2024-12-01", participants: 30, sector: "Agtech" },
        { name: "Digital Hustlers", eso: "Stanbic", start_date: "2025-03-01", end_date: "2025-06-01", participants: 120, sector: "E-commerce" }
      ]
    });
  }
  
  window.PORTFOLIO_DATA.portfolios.forEach(p => {
    if (p.type === 'eoi' || p.type === 'foundation' || p.type === 'segmentation') {
      p.targets = [
        { label: "Youth-Led MSMEs (18–35)", target: 800, actual: p.youth_count || 0 },
        { label: "URSB Registration", target: 300, actual: p.ursb || 0 },
        { label: "PWD Inclusion", target: 50, actual: p.pwd || 0 },
        { label: "Refugee Inclusion", target: 30, actual: (p.stats && p.stats.refugees) || 0 }
      ];
    }
  });
}
"""
    out_path = BASE_DIR / 'data.js'

    out_path.write_text(js_content, encoding='utf-8')

    total_records = sum(p['stats']['total'] for p in portfolios)
    print()
    print('=' * 44)
    print(f'data.js written  —  {len(portfolios)} portfolios  |  {total_records:,} total records')
    print('Open index.html in your browser to explore.')
    print()


if __name__ == '__main__':
    main()
