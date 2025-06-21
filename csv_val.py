import pandas as pd
import os

csv_directory = r'C:/Users/rachi/OneDrive/Desktop/GRAPHENE/Sanofimce_2024'
tables = [
    'RAW_BASEPEOPLEMCE', 'RAW_ENGAGEMENT', 'RAW_NPS_ATTRIBUTES',
    'RAW_NPS_DEDICATEDPROMOTERS', 'RAW_NPS_MATRIX', 'RAW_PRESCRIPTIONINTENT',
    'RAW_PRESCRIPTIONINTENT_ATTRIBUTES', 'RAW_PRESCRIPTIONINTENT_MATRIX',
    'RAW_PRODUCTSATISFACTION', 'RAW_PRODUCTSATISFACTION_ATTRIBUTES',
    'RAW_PRODUCTSATISFACTION_MATRIX', 'RAW_TOUCHPOINTSENGAGEMENT',
    'RAW_TOUCHPOINTSREACH', 'RAW_TPCONTENT_ATTRIBUTES',
    'RAW_TPCONTENTEXECUTION', 'RAW_TPCONTENTEXECUTION_MATRIX',
    'RAW_TPEXECUTION_ATTRIBUTES'
]

for table in tables:
    csv_file = f"{table}.csv"
    file_path = os.path.join(csv_directory, csv_file)
    
    if os.path.exists(file_path):
        df = pd.read_csv(file_path, dtype=str, encoding='utf-8')
        print(f"\n📌 {table}.csv Columns: {list(df.columns)}")
    else:
        print(f"⚠️ {table}.csv not found")
