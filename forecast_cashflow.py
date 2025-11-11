import pandas as pd
from sqlalchemy import create_engine
from pathlib import Path
from statsmodels.tsa.statespace.sarimax import SARIMAX
import numpy as np

ENGINE = create_engine("mysql+pymysql://portfolio_user:StrongPass123!@localhost:3306/portfolio", future=True)

STARTING_BALANCE = 25000.00   # tweak in scenarios
HORIZON_DAYS = 84             # 12 weeks

def make_scenarios(df_daily):
    """
    Apply simple scenario multipliers:
    - inflows +x%, outflows -y% (outflows are negative amounts)
    """
    base = df_daily.copy()
    inflow_up = base.assign(NetCash=np.where(base["NetCash"]>0, base["NetCash"]*1.10, base["NetCash"]))   # +10% receipts
    conservative = base.assign(NetCash=np.where(base["NetCash"]>0, base["NetCash"]*0.9,
                                                base["NetCash"]*1.05))  # receipts -10%, costs +5%
    return {"Base": base, "Optimistic": inflow_up, "Conservative": conservative}

def forecast_series(net_cash_series, horizon=HORIZON_DAYS):
    # simple SARIMAX (ARIMA) on daily net cash
    model = SARIMAX(net_cash_series, order=(1,0,1), seasonal_order=(0,0,0,0), enforce_stationarity=False, enforce_invertibility=False)
    res = model.fit(disp=False)
    fc = res.get_forecast(steps=horizon)
    mean = fc.predicted_mean
    conf = fc.conf_int(alpha=0.2)  # 80% band
    return mean, conf

def to_balance(df, start_balance):
    out = df.copy()
    out["Balance"] = start_balance + out["NetCash"].cumsum()
    return out

def main():
    base = Path(__file__).resolve().parent

    # load daily actuals
    daily = pd.read_sql("SELECT Day, NetCash FROM v_cf_series", ENGINE, parse_dates=["Day"]).set_index("Day").asfreq("D").fillna(0)

    # scenarios on history (affects the ARIMA fit)
    scenarios = make_scenarios(daily)

    all_paths = []
    for name, hist in scenarios.items():
        mean, conf = forecast_series(hist["NetCash"])
        fc_index = pd.date_range(hist.index[-1] + pd.Timedelta(days=1), periods=HORIZON_DAYS, freq="D")
        fc = pd.DataFrame({"NetCash": mean.values}, index=fc_index)

        hist_bal = to_balance(hist, STARTING_BALANCE)
        fc_bal   = to_balance(fc, hist_bal["Balance"].iloc[-1])

        hist_bal["Scenario"] = name
        fc_bal["Scenario"]   = name
        hist_bal["Phase"] = "Actual"
        fc_bal["Phase"]  = "Forecast"

        all_paths.append(hist_bal.reset_index())
        all_paths.append(fc_bal.reset_index())

    out = pd.concat(all_paths, ignore_index=True)
    out.rename(columns={"index":"Day"}, inplace=True)
    out.to_csv(base / "cashflow_paths.csv", index=False)

    # drivers table for visuals (top inflow/outflow last 30 days)
    drivers = pd.read_sql("""
        SELECT TxnDate, Description, Amount
        FROM cf_bank_txn
        WHERE TxnDate >= DATE_SUB(CURDATE(), INTERVAL 30 DAY)
        ORDER BY ABS(Amount) DESC
        LIMIT 50
    """, ENGINE)
    drivers.to_csv(base / "cash_drivers_30d.csv", index=False)

    print("✅ Exported cashflow_paths.csv and cash_drivers_30d.csv")

if __name__ == "__main__":
    main()
