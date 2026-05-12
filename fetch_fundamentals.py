import yfinance as yf
import pandas as pd
from supabase import create_client, Client
import json
import os

SUPABASE_URL = "https://xmqosarkjhktzkdobocd.supabase.co"
SUPABASE_KEY = os.getenv("SUPABASE_KEY")

supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)

def fetch_fundamentals(ticker: str):
    stock = yf.Ticker(ticker)
    info = stock.info

    data = {
        "ticker": ticker,
        "company_name": info.get("longName"),
        "sector": info.get("sector"),
        "industry": info.get("industry"),
        "headquarters": f"{info.get('city', '')}, {info.get('state', '')}".strip(", "),
        "listing_date": info.get("ipoDate"),
        "exchange": info.get("exchange"),
        "description": info.get("longBusinessSummary"),

        "pe_ttm": info.get("trailingPE"),
        "beta": info.get("beta"),
        "eps_latest": info.get("trailingEps"),
        "net_assets_ps": info.get("bookValue"),
        "revenue_latest": info.get("totalRevenue"),
        "revenue_yoy_latest": info.get("revenueGrowth"),
        "net_profit_latest": info.get("netIncomeToCommon"),
        "net_profit_yoy_latest": None,
        "dividend": info.get("dividendRate"),
        "dividend_yield": info.get("dividendYield"),
        "cash_equiv": info.get("totalCash"),

        "net_margin_latest": info.get("profitMargins"),
        "current_ratio_latest": info.get("currentRatio"),
        "roe_latest": info.get("returnOnEquity"),
        "debt_to_assets_latest": info.get("debtToEquity"),
    }

    # 财务历史（最近3年）
    history = []
    try:
        for df in [stock.quarterly_financials, stock.financials]:
            for col in df.columns[:12]:
                if col is None:
                    continue
                history.append({
                    "report_date": str(col.date()),
                    "period_type": "Q" if "Quarter" in str(col) else "A",
                    "revenue": float(df.loc["Total Revenue", col]) if "Total Revenue" in df.index else None,
                    "net_profit": float(df.loc["Net Income", col]) if "Net Income" in df.index else None,
                    "eps": float(df.loc["Basic EPS", col]) if "Basic EPS" in df.index else None,
                    "roe": float(df.loc["Return On Equity", col]) if "Return On Equity" in df.index else None,
                })
    except:
        pass

    data["financial_history"] = json.dumps(history[-36:])
    data["report_date_latest"] = history[0]["report_date"] if history else None

    supabase.table("russell1000_fundamentals").upsert(data).execute()
    print(f"✅ {ticker} 更新完成")

if __name__ == "__main__":
    # 使用你项目中已有的 russell1000.csv
    df_tickers = pd.read_csv("russell1000.csv")
    tickers = df_tickers["ticker"].unique().tolist()
    
    print(f"开始更新 {len(tickers)} 只 Russell 1000 股票的基本面数据...")
    for ticker in tickers:
        try:
            fetch_fundamentals(ticker)
        except Exception as e:
            print(f"❌ {ticker} 更新失败: {e}")
