import os

csv_directory = r'C:/Users/rachi/OneDrive/Desktop/GRAPHENE/Sanofimce_2024'
files = os.listdir(csv_directory)

for table in [
    'RAW_BASEPEOPLEMCE.csv', 'RAW_ENGAGEMENT.csv', 'RAW_NPS_ATTRIBUTES.csv'
]:
    print(f"{table}: {'✅ Found' if table in files else '❌ Missing'}")
