import os
import pandas as pd
from pathlib import Path

# Directory containing Devices Excel files
DEVICES_DIR = Path(__file__).parent / 'Devices'

print(f'Checking Devices files in: {DEVICES_DIR}\n')

for file in DEVICES_DIR.glob('*.xlsx'):
    print(f'File: {file.name}')
    try:
        xl = pd.ExcelFile(file, engine='openpyxl')
        print(f'  Sheets: {xl.sheet_names}')
        for sheet in xl.sheet_names:
            df = xl.parse(sheet)
            print(f'    Sheet "{sheet}": {df.shape[0]} rows, {df.shape[1]} columns')
            if df.shape[0] > 0:
                print(f'      First row: {df.iloc[0].to_dict()}')
            else:
                print('      (Sheet is empty)')
    except Exception as e:
        print(f'  ERROR reading file: {e}')
    print('-' * 40)
