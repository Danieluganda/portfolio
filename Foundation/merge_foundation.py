"""
merge_foundation.py
Merges all Foundation course CSV files into one row-per-user Excel file.

Join key (PK/FK): Email address (case-insensitive)

Sources:
  user_export_*.csv        – BASE: one row per user (profile, phone, spend)
  List of orders*.csv      – ESO name (derived from Coupon Code prefix, e.g. mkazi106 → MKazi)
  progress_*.csv           – student completion %  and per-module status
  List of enrollments*.csv – enrollment date, certificate, group name

Output:
  Foundation_Merged.xlsx
"""

import glob
import os
import re
import pandas as pd

BASE = os.path.dirname(os.path.abspath(__file__))


def find_csv(pattern):
    """Return path to the first CSV matching pattern, or None."""
    matches = glob.glob(os.path.join(BASE, pattern))
    return matches[0] if matches else None


def norm_email(series):
    return series.str.strip().str.lower()


def eso_from_coupon(code):
    """Extract ESO name from coupon code prefix (e.g. 'mkazi106' → 'MKazi')."""
    if not isinstance(code, str) or not code.strip():
        return None
    prefix = re.match(r'^([a-zA-Z]+)', code.strip())
    return prefix.group(1).title() if prefix else None


print('Loading Foundation CSV files...')

# ── 1. BASE: user_export ────────────────────────────────────────────────────

user_path = find_csv('user_export_*.csv')
if not user_path:
    raise FileNotFoundError('user_export_*.csv not found in Foundation folder')

base = pd.read_csv(user_path, dtype=str)
base['Email'] = norm_email(base['Email'])
base = base.drop_duplicates(subset='Email')
print(f'  user_export:  {len(base):,} users  (base)')

# ── 2. ORDERS → ESO name ───────────────────────────────────────────────────

orders_path = find_csv('List of orders*.csv')
if orders_path:
    orders = pd.read_csv(orders_path, dtype=str)
    orders['Email'] = norm_email(orders['Student Email'])
    # Each user may have multiple orders; take the most informative coupon code
    # (prefer non-empty ones, then take last by order date)
    orders_sorted = orders[orders['Coupon Code'].notna()].sort_values('Order Date')
    orders_eso = orders_sorted.groupby('Email').agg(
        ESO_Name=('Coupon Code', lambda x: eso_from_coupon(x.iloc[-1]) if len(x) else None),
        Coupon_Code=('Coupon Code', 'last'),
        Last_Order_Date=('Order Date', 'last'),
    ).reset_index()
    print(f'  orders:       {len(orders):,} rows → {len(orders_eso):,} unique users')
else:
    orders_eso = None
    print('  orders:       NOT FOUND — skipping')

# ── 3. PROGRESS → completion & module status ───────────────────────────────

progress_path = find_csv('progress_*.csv')
if progress_path:
    progress = pd.read_csv(progress_path, dtype=str)
    progress['Email'] = norm_email(progress['Email'])
    progress = progress.drop_duplicates(subset='Email')

    # Separate module columns from identity columns
    id_cols   = ['Email', 'Company', 'Started At', 'Completed At',
                 'Activated At', 'Expires At', 'Last Sign In', '% Viewed', '% Completed']
    mod_cols  = [c for c in progress.columns if c not in id_cols + ['First Name', 'Last Name']]
    prog_keep = [c for c in id_cols + mod_cols if c in progress.columns]

    progress = progress[prog_keep]
    print(f'  progress:     {len(progress):,} users  ({len(mod_cols)} module columns)')
else:
    progress = None
    print('  progress:     NOT FOUND — skipping')

# ── 4. ENROLLMENTS → certificate, enrollment date, group ──────────────────

enroll_path = find_csv('List of enrollments*.csv')
if enroll_path:
    enroll = pd.read_csv(enroll_path, dtype=str)
    enroll['Email'] = norm_email(enroll['Student Email'])
    enroll_agg = enroll.sort_values('Enrollment Date').groupby('Email').agg(
        Enrollment_Date=('Enrollment Date', 'last'),
        Completed_Date=('Completed Date', 'last'),
        Enrollment_Pct=('Percentage Completed', 'last'),
        Has_Certificate=('Has Certificate (Yes / No)', 'last'),
        ESO_Group=('User First Group Name', lambda x: ', '.join(x.dropna().unique()) or None),
    ).reset_index()
    print(f'  enrollments:  {len(enroll):,} rows → {len(enroll_agg):,} unique users')
else:
    enroll_agg = None
    print('  enrollments:  NOT FOUND — skipping')

# ── 5. Merge everything onto user_export base ──────────────────────────────

print('\nMerging onto user_export...')
merged = base.copy()

if orders_eso is not None:
    merged = merged.merge(orders_eso, on='Email', how='left')

if progress is not None:
    merged = merged.merge(progress, on='Email', how='left')

if enroll_agg is not None:
    merged = merged.merge(enroll_agg, on='Email', how='left')

print(f'  Result: {len(merged):,} rows × {len(merged.columns)} columns')

# ── 6. Column ordering ─────────────────────────────────────────────────────

priority = [
    'First Name', 'Last Name', 'Email', 'Phone number',
    'ESO_Name', 'ESO_Group', 'Coupon_Code',
    'Company', 'Date created', 'Sign in count', 'Last sign in',
    'Enrollments', 'Enrollments - list',
    'Started At', 'Completed At', 'Activated At', 'Expires At',
    '% Completed', '% Viewed',
    'Enrollment_Pct', 'Has_Certificate',
    'Enrollment_Date', 'Completed_Date',
    'Last_Order_Date', 'Amount spent', 'Referred by', 'External source',
]

mod_cols = [c for c in merged.columns
            if c.startswith(('Welcome', 'Module', 'Next steps'))]
ordered  = [c for c in priority if c in merged.columns]
ordered += mod_cols
ordered += [c for c in merged.columns if c not in ordered]
merged   = merged[ordered]

# ── 7. Write Excel output ─────────────────────────────────────────────────

out_path = os.path.join(BASE, 'Foundation_Merged.xlsx')
print(f'\nWriting: {out_path}')

with pd.ExcelWriter(out_path, engine='openpyxl') as writer:
    merged.to_excel(writer, sheet_name='Foundation Data', index=False)

    ws = writer.sheets['Foundation Data']
    for col_cells in ws.columns:
        max_len = max(
            (len(str(cell.value)) if cell.value else 0 for cell in col_cells),
            default=10
        )
        ws.column_dimensions[col_cells[0].column_letter].width = min(max_len + 2, 60)

print(f'Done — {len(merged):,} rows | {len(merged.columns)} columns')
print(f'Output: Foundation_Merged.xlsx')
