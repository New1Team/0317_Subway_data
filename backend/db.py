import pandas as pd
from sqlalchemy import create_engine, text
from settings import settings
import os

def save(mariadb, file_path):
  if mariadb:
    # ANSI = cp949
    # UTF = utf-8
    df = pd.read_csv(file_path, encoding="utf-8", header=0, thousands=',', quotechar='"', skipinitialspace=True)
    df.columns = df.columns.str.strip()
    print(df.columns)
    df.to_sql('seoul_metro_temp', con=mariadb, if_exists='append', index=False)

def main():
  mariadb_engine = create_engine(settings.mariadb_host)
  with mariadb_engine.connect() as conn:
    conn.execute(text("TRUNCATE TABLE seoul_metro_temp"))
    conn.commit()

    folder_path = settings.file_dir
    files = [f for f in os.listdir(folder_path) if os.path.isfile(os.path.join(folder_path, f))]
    print("파일만:", files)
    all_files = os.listdir(folder_path)

    for file in all_files:
      file_path = os.path.join(folder_path, file)
      if os.path.isfile(os.path.join(folder_path, file_path)):
        # save(mariadb_engine, file_path)
        pass

if __name__ == "__main__":
  main()
