import os
import pandas as pd
import yfinance as yf
from supabase import create_client, Client
from datetime import datetime, timedelta
import time

# ====================== 配置 ======================
CSV_PATH = "russell1000.csv"          # 你的股票列表文件
SUPABASE_URL = os.environ["https://xmqosarkjhktzkdobocd.supabase.co"]
SUPABASE_KEY = os.environ["sb_publishable_KYY2U-LufvbCbJzX6p7krg_9FMKW-VD"]

supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)

def load_tickers():
    df = pd.read_csv(CSV_PATH)
    for col in ("Ticker", "Symbol", "ticker", "symbol"):
        if col in df.columns:
            return df[col].dropna().astype(str).str.replace('.', '-', regex=False).unique().tolist()
    raise ValueError("未找到股票代码列")

def fetch_and_upload():
    tickers = load_tickers()
    print(f"开始下载 {len(tickers)} 只股票...")

    BATCH_SIZE = 60
    all_data = []

    for i in range(0, len(tickers), BATCH_SIZE):
        batch = tickers[i:i+BATCH_SIZE]
        try:
            data = yf.download(batch, period="3mo", threads=True, progress=False)
            if data.empty:
                continue
            df = data.stack(level=1, future_stack=True).reset_index()
            df.columns = [str(c).lower() for c in df.columns]
            if "adj close" in df.columns:
                df["close"] = df["adj close"]
            
            df = df.rename(columns={"date": "date", "ticker": "ticker"})[
                ["date", "ticker", "open", "high", "low", "close", "volume"]
            ]
            df = df.dropna(subset=["close"])
            df["date"] = pd.to_datetime(df["date"]).dt.date
            all_data.append(df)
            print(f"✅ 批次 {i//BATCH_SIZE + 1} 完成")
        except Exception as e:
            print(f"⚠️ 批次失败: {e}")
        time.sleep(2)

    if not all_data:
        print("❌ 全部下载失败")
        return

    df_final = pd.concat(all_data, ignore_index=True)
    print(f"总计获取 {len(df_final)} 条记录，开始上传 Supabase...")

    # 分批 upsert（Supabase 推荐单次 < 5000 条）
    batch_size = 4000
    for i in range(0, len(df_final), batch_size):
        chunk = df_final.iloc[i:i+batch_size]
        data_dict = chunk.to_dict(orient="records")
        supabase.table("russell1000_prices").upsert(data_dict, on_conflict="date,ticker").execute()
        print(f"✅ 已上传 {i + len(chunk)} / {len(df_final)} 条")

    print("🎉 全部数据已同步到 Supabase！")

if __name__ == "__main__":
    fetch_and_upload()
