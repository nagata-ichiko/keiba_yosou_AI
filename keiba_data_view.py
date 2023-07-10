import os
import pandas as pd
from modules import LocalPaths
# 各pickleファイルを読み込み
df_results = pd.read_pickle(LocalPaths.RAW_RESULTS_PATH)
df_return_tables = pd.read_pickle(LocalPaths.RAW_RETURN_TABLES_PATH)
df_horse_results = pd.read_pickle(LocalPaths.RAW_HORSE_RESULTS_PATH)
df_peds = pd.read_pickle(LocalPaths.RAW_PEDS_PATH)

# 各データフレームを表示
print("df_results:\n", df_results.head())
print("\ndf_horse_results:\n", df_horse_results.head())
print("\ndf_peds:\n", df_peds.head())

# CSVを保存するディレクトリ
CSV_DIR = os.path.join("data/csv", 'csv')
os.makedirs(CSV_DIR, exist_ok=True)  # ディレクトリが存在しなければ作成

# 各DataFrameをCSV形式で出力
df_results.to_csv(os.path.join(CSV_DIR, 'results.csv'), index=False)
df_return_tables.to_csv(os.path.join(CSV_DIR, 'return_tables.csv'), index=False)
df_horse_results.to_csv(os.path.join(CSV_DIR, 'horse_results.csv'), index=False)
df_peds.to_csv(os.path.join(CSV_DIR, 'peds.csv'), index=False)