#!/usr/bin/env python3
import yfinance as yf
import pandas as pd
from supabase import create_client, Client
from datetime import datetime
import json
import os

SUPABASE_URL = "https://xmqosarkjhktzkdobocd.supabase.co"
SUPABASE_KEY = os.getenv("SUPABASE_KEY")  # 在 GitHub Secrets 中设置

supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)

def fetch_fundamentals(ticker: str):
    stock = yf.Ticker(ticker)
    info = stock.info
    
    # 主要指标
    fundamentals = {
        "ticker": ticker,
        "company_name": info.get("longName"),
        "sector": info.get("sector"),
        "industry": info.get("industry"),
        "headquarters": info.get("city") + ", " + info.get("state", "") if info.get("city") else None,
        "establishment_date": None,  # yfinance 暂无，可后续手动补充
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
        "net_profit_yoy_latest": None,  # yfinance 暂无同比，可后续补充
        "dividend": info.get("dividendRate"),
        "dividend_yield": info.get("dividendYield"),
        "cash_equiv": info.get("totalCash"),
    }

    # 财务历史（最近3年报告期）
    financial_history = []
    try:
        quarterly = stock.quarterly_financials
        annual = stock.financials
        for df in [quarterly, annual]:
            for col in df.columns[:12]:  # 最近3年左右
                if col is None:
                    continue
                financial_history.append({
                    "report_date": str(col.date()),
                    "period_type": "Q" if "Quarter" in str(col) else "A",
                    "revenue": float(df.loc["Total Revenue", col]) if "Total Revenue" in df.index else None,
                    "net_profit": float(df.loc["Net Income", col]) if "Net Income" in df.index else None,
                    "eps": float(df.loc["Basic EPS", col]) if "Basic EPS" in df.index else None,
                    "roe": float(df.loc["Return On Equity", col]) if "Return On Equity" in df.index else None,
                    "revenue_yoy": None,  # 可后续通过计算补充
                })
    except:
        pass

    fundamentals["financial_history"] = json.dumps(financial_history[-36:])  # 保留最近36个月
    fundamentals["report_date_latest"] = financial_history[0]["report_date"] if financial_history else None

    return fundamentals

def main():
    # 这里可以从 russell1000_info 表读取 ticker 列表，或者直接用现有股价表的 ticker
    # 为简化，先用一个示例，后续可改为批量
    tickers = ["SPOT"]  # 后续改为从数据库读取所有 Russell 1000 ticker
    
    for ticker in tickers:
        try:
            data = fetch_fundamentals(ticker)
            supabase.table("russell1000_fundamentals").upsert(data).execute()
            print(f"✅ Updated {ticker}")
        except Exception as e:
            print(f"❌ Failed {ticker}: {e}")

if __name__ == "__main__":
    main()
