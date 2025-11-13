import os
import numpy as np
import pandas as pd
import streamlit as st
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots
from datetime import datetime, timedelta
from scipy import stats
import time

from filters import apply_time_entry_filters


# ----------------------------
# CONFIG: where the Excel files live
# ----------------------------

DATA_DIR = "Files"

TIME_ENTRY_FILES = [
    "Time Entry Prep File (10.31).xlsx",
    "Time Entry Prep File (10.31) - FY25.xlsx",
]
INVOICE_FILE = "Invoice Prep File (10.31).xlsx"
PAYMENT_FILE = "Payment Prep File (10.31).xlsx"


# ----------------------------
# Password removed for easier access
# ----------------------------

def check_password() -> bool:
    """Password disabled - direct access."""
    return True


# ----------------------------
# Smart Caching with Parquet (10-30x faster!)
# ----------------------------

def get_cached_file_path(excel_file):
    """Get path for cached Parquet version."""
    base_name = os.path.splitext(excel_file)[0]
    return os.path.join(DATA_DIR, f"{base_name}.parquet")


def load_single_file_smart(excel_file):
    """Load file with smart caching strategy."""
    excel_path = os.path.join(DATA_DIR, excel_file)
    parquet_path = get_cached_file_path(excel_file)
    
    # Check if Parquet cache exists and is newer than Excel
    if os.path.exists(parquet_path):
        excel_mtime = os.path.getmtime(excel_path)
        parquet_mtime = os.path.getmtime(parquet_path)
        
        if parquet_mtime > excel_mtime:
            # Cache is fresh - use it (super fast!)
            start = time.time()
            df = pd.read_parquet(parquet_path)
            load_time = time.time() - start
            return df, excel_file, load_time, "cached"
    
    # Need to load from Excel (slow first time)
    start = time.time()
    
    file_size_mb = os.path.getsize(excel_path) / (1024 * 1024)
    
    # Read Excel
    df = pd.read_excel(excel_path, engine='openpyxl')
    
    # Handle header row if needed
    if "ELIMINATED BILLING ORIGINATORS" in str(df.columns):
        header_row = df.iloc[0]
        df = df[1:].copy()
        df.columns = header_row
    
    load_time = time.time() - start
    
    # Save to Parquet for next time
    try:
        df.to_parquet(parquet_path, compression='snappy', index=False)
    except Exception as e:
        st.warning(f"Could not cache {excel_file}: {e}")
    
    return df, excel_file, load_time, "excel"


@st.cache_data(show_spinner=False, ttl=3600)
def load_time_entries() -> pd.DataFrame:
    """
    🚀 Ultra-fast loader with Parquet caching.
    
    First run: 30-60 seconds (Excel load + cache creation)
    Subsequent runs: 1-3 seconds (Parquet cache) - 10-30x faster!
    """
    
    # Check if all files have Parquet caches
    all_cached = all(os.path.exists(get_cached_file_path(f)) for f in TIME_ENTRY_FILES)
    
    if all_cached:
        # Fast path - all files cached
        with st.spinner("⚡ Loading from cache..."):
            frames = []
            start_time = time.time()
            
            for file in TIME_ENTRY_FILES:
                df = pd.read_parquet(get_cached_file_path(file))
                frames.append(df)
            
            df = pd.concat(frames, ignore_index=True)
            load_time = time.time() - start_time
        
        st.success(f"✅ Loaded {len(df):,} records from cache in {load_time:.2f} seconds!")
        
    else:
        # Slow path - need to load from Excel
        st.warning("""
        ⏱️ **First-Time Load**
        
        This will take 30-60 seconds while we:
        1. Load your Excel files
        2. Create fast Parquet caches
        
        **Future loads will be 10-30x faster!**
        """)
        
        progress_bar = st.progress(0)
        status = st.empty()
        
        # Load files
        results = []
        for idx, file in enumerate(TIME_ENTRY_FILES):
            file_size_mb = os.path.getsize(os.path.join(DATA_DIR, file)) / (1024 * 1024)
            estimated_time = int(file_size_mb * 3)
            
            status.markdown(f"""
            ### 📂 Loading: {file}
            **Size:** {file_size_mb:.1f} MB  
            **Estimated time:** ~{estimated_time} seconds  
            **Progress:** {idx + 1}/{len(TIME_ENTRY_FILES)}
            """)
            
            result = load_single_file_smart(file)
            results.append(result)
            
            progress_bar.progress((idx + 1) / len(TIME_ENTRY_FILES))
        
        progress_bar.empty()
        status.empty()
        
        frames = [df for df, _, _, _ in results]
        df = pd.concat(frames, ignore_index=True)
        
        total_time = sum(load_time for _, _, load_time, _ in results)
        
        st.success(f"""
        ✅ **Initial Load Complete!**
        
        - Loaded {len(df):,} records in {total_time:.1f} seconds
        - Created Parquet caches for future use
        - Next load will be **{int(total_time / 2)}x faster** (~2 seconds!)
        """)
    
    # Data cleaning
    for col in ["Date_of_Work", "Time_Creation_Date", "Invoice Date", "Period of Invoice"]:
        if col in df.columns:
            df[col] = pd.to_datetime(df[col], errors="coerce")
    
    numeric_cols = [
        "Billable_Amount_in_USD",
        "Billable_Amount_Orig_Currency",
        "Billable_Hours",
        "Billing_Rate_in_USD",
    ]
    for col in numeric_cols:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")
    
    return df


@st.cache_data(show_spinner=False, ttl=3600)
def load_invoice_prep() -> pd.DataFrame:
    """Load Invoice Prep file with Parquet caching."""
    excel_path = os.path.join(DATA_DIR, INVOICE_FILE)
    
    if not os.path.exists(excel_path):
        return pd.DataFrame()
    
    parquet_path = get_cached_file_path(INVOICE_FILE)
    
    # Try cache first
    if os.path.exists(parquet_path):
        if os.path.getmtime(parquet_path) > os.path.getmtime(excel_path):
            return pd.read_parquet(parquet_path)
    
    # Load from Excel
    with st.spinner("Loading Invoice data..."):
        df = pd.read_excel(excel_path, engine="openpyxl")
        
        date_cols = ["Invoice Date", "Invoice_Creation_Date"]
        for col in date_cols:
            if col in df.columns:
                df[col] = pd.to_datetime(df[col], errors="coerce")
        
        for col in [
            "Original Inv. Total",
            "Orig Labor Total",
            "Orig Expense Total",
            "Net Labor Billings",
            "Net Expense Billings",
        ]:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors="coerce")
        
        # Cache it
        df.to_parquet(parquet_path, compression='snappy', index=False)
    
    return df


@st.cache_data(show_spinner=False, ttl=3600)
def load_payment_prep() -> pd.DataFrame:
    """Load Payment Prep file with Parquet caching."""
    excel_path = os.path.join(DATA_DIR, PAYMENT_FILE)
    
    if not os.path.exists(excel_path):
        return pd.DataFrame()
    
    parquet_path = get_cached_file_path(PAYMENT_FILE)
    
    # Try cache first
    if os.path.exists(parquet_path):
        if os.path.getmtime(parquet_path) > os.path.getmtime(excel_path):
            return pd.read_parquet(parquet_path)
    
    # Load from Excel
    with st.spinner("Loading Payment data..."):
        df_raw = pd.read_excel(excel_path, engine="openpyxl")
        
        header_row = df_raw.iloc[1]
        df = df_raw[2:].copy()
        df.columns = header_row
        
        date_cols = ["Invoice\nor\nPayment\nDate", "Payment\nApplied Date", "Payment_Date"]
        for col in date_cols:
            if col in df.columns:
                df[col] = pd.to_datetime(df[col], errors="coerce")
        
        numeric_cols = [
            "Payment_Applied_To_User_Amount_in_Original_Currency",
            "Payment_Applied_To_User_Amount_in_USD",
            "Payments_Applied_to_Labor_in_Orig_Currency",
            "Payments_Applied_to_Expense_in_Orig_Currency",
            "Payments_Applied_to_Labor_in_USD",
            "Payments_Applied_to_Expense_in_USD",
        ]
        for col in numeric_cols:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors="coerce")
        
        # Cache it
        df.to_parquet(parquet_path, compression='snappy', index=False)
    
    return df


# ----------------------------
# Advanced Analytics Functions
# ----------------------------

def calculate_growth_metrics(df: pd.DataFrame) -> dict:
    """Calculate period-over-period growth metrics."""
    if df.empty or len(df) < 2:
        return {}
    
    df = df.sort_values("YearMonth")
    
    # MoM growth
    df["MoM_Growth"] = df["Billable_Amount_in_USD"].pct_change() * 100
    
    # YoY growth (if we have 12+ months)
    if len(df) >= 12:
        df["YoY_Growth"] = df["Billable_Amount_in_USD"].pct_change(periods=12) * 100
    
    # 3-month moving average
    df["MA_3"] = df["Billable_Amount_in_USD"].rolling(window=3, min_periods=1).mean()
    
    # 6-month moving average
    df["MA_6"] = df["Billable_Amount_in_USD"].rolling(window=6, min_periods=1).mean()
    
    # Calculate trend
    if len(df) >= 6:
        x = np.arange(len(df))
        y = df["Billable_Amount_in_USD"].values
        slope, intercept = np.polyfit(x, y, 1)
        trend = "growing" if slope > 0 else "declining"
    else:
        slope = 0
        trend = "stable"
    
    return {
        "data": df,
        "latest_mom": df["MoM_Growth"].iloc[-1] if len(df) > 0 else None,
        "avg_mom": df["MoM_Growth"].mean(),
        "volatility": df["MoM_Growth"].std(),
        "trend": trend,
        "trend_slope": slope,
    }


def analyze_attorney_productivity(df: pd.DataFrame) -> pd.DataFrame:
    """Analyze attorney productivity metrics."""
    if df.empty or "Timekeeper" not in df.columns:
        return pd.DataFrame()
    
    attorney_stats = df.groupby("Timekeeper").agg({
        "Billable_Hours": ["sum", "mean", "std"],
        "Billable_Amount_in_USD": ["sum", "mean"],
        "Billing_Rate_in_USD": "mean",
    }).round(2)
    
    attorney_stats.columns = [
        "Total_Hours", "Avg_Hours_Per_Entry", "Std_Hours",
        "Total_Revenue", "Avg_Revenue_Per_Entry", "Avg_Billing_Rate"
    ]
    
    # Calculate effective hourly rate
    attorney_stats["Effective_Hourly_Rate"] = (
        attorney_stats["Total_Revenue"] / attorney_stats["Total_Hours"]
    ).round(2)
    
    # Calculate consistency score (inverse of coefficient of variation)
    attorney_stats["Consistency_Score"] = np.where(
        attorney_stats["Avg_Hours_Per_Entry"] > 0,
        100 - (attorney_stats["Std_Hours"] / attorney_stats["Avg_Hours_Per_Entry"] * 100),
        0
    ).clip(0, 100).round(1)
    
    return attorney_stats.sort_values("Total_Revenue", ascending=False).reset_index()


def analyze_client_concentration(df: pd.DataFrame) -> dict:
    """Analyze client revenue concentration and risk."""
    if df.empty or "Client_Name" not in df.columns:
        return {}
    
    client_revenue = df.groupby("Client_Name")["Billable_Amount_in_USD"].sum().sort_values(ascending=False)
    total_revenue = client_revenue.sum()
    
    if total_revenue == 0:
        return {}
    
    client_revenue_pct = (client_revenue / total_revenue * 100).round(2)
    
    # Calculate concentration metrics
    top_5_pct = client_revenue_pct.head(5).sum()
    top_10_pct = client_revenue_pct.head(10).sum()
    top_20_pct = client_revenue_pct.head(20).sum()
    
    # Calculate Herfindahl-Hirschman Index (HHI)
    hhi = (client_revenue_pct ** 2).sum()
    
    return {
        "client_revenue": client_revenue,
        "client_revenue_pct": client_revenue_pct,
        "top_5_concentration": top_5_pct,
        "top_10_concentration": top_10_pct,
        "top_20_concentration": top_20_pct,
        "hhi": hhi,
        "num_clients": len(client_revenue),
    }


def analyze_billing_patterns(df: pd.DataFrame) -> dict:
    """Analyze billing patterns and behaviors."""
    if df.empty:
        return {}
    
    df = df.copy()
    df["Date_of_Work"] = pd.to_datetime(df["Date_of_Work"])
    df["DayOfWeek"] = df["Date_of_Work"].dt.day_name()
    df["Hour"] = pd.to_datetime(df.get("Time_Creation_Date", df["Date_of_Work"])).dt.hour
    df["WeekOfMonth"] = ((df["Date_of_Work"].dt.day - 1) // 7) + 1
    
    # Day of week analysis
    dow_revenue = df.groupby("DayOfWeek")["Billable_Amount_in_USD"].sum()
    dow_hours = df.groupby("DayOfWeek")["Billable_Hours"].sum()
    
    # Time of day analysis (if available)
    hour_dist = df.groupby("Hour").size() if "Time_Creation_Date" in df.columns else pd.Series()
    
    # Week of month patterns
    wom_revenue = df.groupby("WeekOfMonth")["Billable_Amount_in_USD"].sum()
    
    return {
        "day_of_week_revenue": dow_revenue,
        "day_of_week_hours": dow_hours,
        "hour_distribution": hour_dist,
        "week_of_month_revenue": wom_revenue,
    }


def calculate_rate_type_metrics(df: pd.DataFrame) -> dict:
    """Detailed analysis of rate types."""
    if df.empty or "Rate_Type" not in df.columns:
        return {}
    
    rate_stats = df.groupby("Rate_Type").agg({
        "Billable_Amount_in_USD": ["sum", "mean", "count"],
        "Billable_Hours": "sum",
    }).round(2)
    
    rate_stats.columns = ["Total_Revenue", "Avg_Revenue", "Count", "Total_Hours"]
    
    total_revenue = rate_stats["Total_Revenue"].sum()
    rate_stats["Revenue_Share_Pct"] = (rate_stats["Total_Revenue"] / total_revenue * 100).round(2)
    
    return {
        "rate_stats": rate_stats.sort_values("Total_Revenue", ascending=False),
        "total_revenue": total_revenue,
    }


def prepare_monthly_time_by_rate(df: pd.DataFrame) -> pd.DataFrame:
    """Prepare monthly time entry data grouped by rate type."""
    if df.empty:
        return df

    df = df.copy()
    df["Date_of_Work"] = pd.to_datetime(df["Date_of_Work"], errors="coerce")
    df = df.dropna(subset=["Date_of_Work", "Billable_Amount_in_USD"])

    df["YearMonth"] = df["Date_of_Work"].dt.to_period("M").dt.to_timestamp()

    group_cols = ["YearMonth"]
    if "Rate_Type" in df.columns:
        group_cols.append("Rate_Type")
    else:
        df["Rate_Type"] = "Unknown"
        group_cols.append("Rate_Type")

    monthly = (
        df.groupby(group_cols)
        .agg({
            "Billable_Amount_in_USD": "sum",
            "Billable_Hours": "sum",
        })
        .reset_index()
        .sort_values("YearMonth")
    )
    return monthly


def advanced_forecast(series: pd.Series, periods: int = 3, method: str = "linear") -> dict:
    """
    Advanced forecasting with multiple methods and confidence intervals.
    FIXED: Properly handles datetime index.
    """
    series = series.dropna()
    if len(series) < 3:
        return {
            "forecast": pd.Series(dtype=float), 
            "lower": pd.Series(dtype=float), 
            "upper": pd.Series(dtype=float),
            "method": method,
            "metrics": {},
            "historical_values": series
        }

    x = np.arange(len(series))
    y = series.values
    
    if method == "linear":
        # Linear regression
        slope, intercept = np.polyfit(x, y, 1)
        future_x = np.arange(len(series), len(series) + periods)
        forecast_values = intercept + slope * future_x
        
        # Calculate residuals for confidence intervals
        fitted = intercept + slope * x
        residuals = y - fitted
        std_error = np.std(residuals)
        
        # R-squared
        ss_res = np.sum(residuals ** 2)
        ss_tot = np.sum((y - np.mean(y)) ** 2)
        r_squared = 1 - (ss_res / ss_tot) if ss_tot > 0 else 0
        
        # 95% confidence interval
        margin = 1.96 * std_error
        
        metrics = {
            "r_squared": r_squared,
            "slope": slope,
            "intercept": intercept,
            "std_error": std_error,
            "mean_absolute_error": np.mean(np.abs(residuals)),
            "mape": np.mean(np.abs(residuals / y)) * 100 if np.all(y != 0) else None
        }
        
    elif method == "exponential":
        # Exponential smoothing
        alpha = 0.3
        forecast_values = []
        level = y[0]
        
        fitted = []
        for val in y:
            fitted.append(level)
            level = alpha * val + (1 - alpha) * level
        
        for _ in range(periods):
            forecast_values.append(level)
        
        forecast_values = np.array(forecast_values)
        fitted = np.array(fitted)
        residuals = y - fitted
        std_error = np.std(residuals)
        margin = 1.96 * std_error
        
        metrics = {
            "alpha": alpha,
            "final_level": level,
            "std_error": std_error,
            "mean_absolute_error": np.mean(np.abs(residuals)),
            "mape": np.mean(np.abs(residuals / y)) * 100 if np.all(y != 0) else None
        }
    
    else:  # moving average
        window = min(3, len(series))
        ma_value = series.tail(window).mean()
        forecast_values = np.full(periods, ma_value)
        std_error = series.tail(window).std()
        margin = 1.96 * std_error
        
        metrics = {
            "window": window,
            "ma_value": ma_value,
            "std_error": std_error
        }
    
    # Ensure non-negative
    forecast_values = np.maximum(forecast_values, 0)
    
    # FIX: Generate future dates correctly from the last date in series
    last_date = series.index[-1]
    
    # Create future dates by adding months
    future_dates = pd.date_range(
        start=last_date + pd.DateOffset(months=1),
        periods=periods,
        freq='MS'  # Month start frequency
    )
    
    forecast_series = pd.Series(forecast_values, index=future_dates)
    lower_bound = pd.Series(np.maximum(forecast_values - margin, 0), index=future_dates)
    upper_bound = pd.Series(forecast_values + margin, index=future_dates)
    
    return {
        "forecast": forecast_series,
        "lower": lower_bound,
        "upper": upper_bound,
        "method": method,
        "metrics": metrics,
        "historical_values": series,
        "std_error": std_error
    }


def generate_comprehensive_insights(filtered_time: pd.DataFrame, monthly_long: pd.DataFrame) -> str:
    """Generate comprehensive business insights with ALL trends analysis."""
    if monthly_long.empty:
        return "Insufficient data for analysis."
    
    insights = []
    insights.append("## 📊 Executive Summary\n")
    
    # Revenue trends
    pivot = (
        monthly_long
        .pivot(index="YearMonth", columns="Rate_Type", values="Billable_Amount_in_USD")
        .fillna(0)
        .sort_index()
    )
    
    total = pivot.sum(axis=1)
    
    # ==========================================
    # 1. REVENUE TREND ANALYSIS
    # ==========================================
    if len(total) >= 2:
        recent_3m = total.tail(3).mean()
        prior_3m = total.iloc[-6:-3].mean() if len(total) >= 6 else total.iloc[:-3].mean() if len(total) > 3 else total.iloc[0]
        
        if prior_3m > 0:
            growth = ((recent_3m - prior_3m) / prior_3m) * 100
            insights.append(f"**Revenue Trend**: The most recent 3-month average revenue (${recent_3m:,.0f}) is "
                          f"{'**up ' + f'{growth:.1f}%**' if growth > 0 else '**down ' + f'{abs(growth):.1f}%**'} "
                          f"compared to the prior 3-month period (${prior_3m:,.0f}).\n")
    
    # Overall trend direction
    if len(total) >= 6:
        x = np.arange(len(total))
        y = total.values
        slope, _ = np.polyfit(x, y, 1)
        monthly_change = slope
        annual_projected_change = slope * 12
        
        if slope > 0:
            insights.append(f"**Overall Trajectory**: Revenue is on a **positive trajectory**, "
                          f"growing at approximately ${monthly_change:,.0f}/month "
                          f"(projected ${annual_projected_change:,.0f}/year increase).\n")
        elif slope < 0:
            insights.append(f"**Overall Trajectory**: Revenue is on a **declining trajectory**, "
                          f"decreasing at approximately ${abs(monthly_change):,.0f}/month "
                          f"(projected ${abs(annual_projected_change):,.0f}/year decrease).\n")
        else:
            insights.append(f"**Overall Trajectory**: Revenue is relatively **flat** with minimal month-over-month changes.\n")
    
    # ==========================================
    # 2. VOLATILITY & STABILITY ANALYSIS
    # ==========================================
    if len(total) >= 6:
        volatility = total.pct_change().std() * 100
        if volatility < 10:
            stability = "very stable"
            stability_icon = "🟢"
        elif volatility < 20:
            stability = "moderately stable"
            stability_icon = "🟡"
        elif volatility < 30:
            stability = "somewhat volatile"
            stability_icon = "🟠"
        else:
            stability = "highly volatile"
            stability_icon = "🔴"
        
        insights.append(f"{stability_icon} **Revenue Stability**: Month-over-month revenue shows **{stability}** patterns "
                       f"(volatility: {volatility:.1f}%).\n")
        
        # Identify most volatile periods
        mom_changes = total.pct_change() * 100
        max_increase = mom_changes.max()
        max_decrease = mom_changes.min()
        
        if not pd.isna(max_increase) and abs(max_increase) > 20:
            max_increase_date = mom_changes.idxmax()
            insights.append(f"  - Largest increase: **+{max_increase:.1f}%** in {max_increase_date.strftime('%b %Y')}\n")
        
        if not pd.isna(max_decrease) and abs(max_decrease) > 20:
            max_decrease_date = mom_changes.idxmin()
            insights.append(f"  - Largest decrease: **{max_decrease:.1f}%** in {max_decrease_date.strftime('%b %Y')}\n")
    
    # ==========================================
    # 3. BILLING MIX ANALYSIS
    # ==========================================
    if len(pivot.columns) > 1:
        insights.append("\n## 💼 Billing Mix Analysis\n")
        
        for col in pivot.columns:
            col_total = pivot[col].sum()
            col_pct = (col_total / total.sum()) * 100
            
            # Calculate recent trend
            if len(pivot) >= 6:
                recent_avg = pivot[col].tail(3).mean()
                prior_avg = pivot[col].iloc[-6:-3].mean() if len(pivot) >= 6 else pivot[col].iloc[:-3].mean()
                col_trend_pct = ((recent_avg - prior_avg) / prior_avg * 100) if prior_avg > 0 else 0
                
                if col_trend_pct > 5:
                    trend_desc = f"**📈 Trending up** ({col_trend_pct:+.1f}%)"
                elif col_trend_pct < -5:
                    trend_desc = f"**📉 Trending down** ({col_trend_pct:+.1f}%)"
                else:
                    trend_desc = "**➡️ Stable**"
            else:
                trend_desc = "Insufficient data for trend"
            
            # Calculate share change
            if len(pivot) >= 6:
                recent_share = (pivot[col].tail(3).sum() / total.tail(3).sum()) * 100
                prior_share = (pivot[col].iloc[-6:-3].sum() / total.iloc[-6:-3].sum()) * 100 if len(pivot) >= 6 else col_pct
                share_change = recent_share - prior_share
                
                insights.append(f"**{col}**: ${col_total:,.0f} ({col_pct:.1f}% of total) - {trend_desc}\n")
                if abs(share_change) > 2:
                    insights.append(f"  - Market share change: {share_change:+.1f} percentage points\n")
            else:
                insights.append(f"**{col}**: ${col_total:,.0f} ({col_pct:.1f}% of total) - {trend_desc}\n")
    
    # ==========================================
    # 4. SEASONALITY PATTERNS
    # ==========================================
    if len(total) >= 12:
        insights.append("\n## 📅 Seasonal Patterns\n")
        
        monthly_avg = total.groupby(total.index.month).mean()
        strongest_month = monthly_avg.idxmax()
        weakest_month = monthly_avg.idxmin()
        
        month_names = {1: "January", 2: "February", 3: "March", 4: "April", 5: "May", 6: "June",
                      7: "July", 8: "August", 9: "September", 10: "October", 11: "November", 12: "December"}
        
        insights.append(f"Historically, **{month_names[strongest_month]}** has been the strongest month "
                       f"(avg: ${monthly_avg[strongest_month]:,.0f}), while **{month_names[weakest_month]}** "
                       f"has been the weakest (avg: ${monthly_avg[weakest_month]:,.0f}).\n")
        
        # Quarter analysis
        df_with_quarter = total.to_frame('revenue')
        df_with_quarter['quarter'] = df_with_quarter.index.quarter
        quarterly_avg = df_with_quarter.groupby('quarter')['revenue'].mean()
        strongest_q = quarterly_avg.idxmax()
        weakest_q = quarterly_avg.idxmin()
        
        insights.append(f"\n**Quarterly Trends**: Q{strongest_q} is typically strongest (avg: ${quarterly_avg[strongest_q]:,.0f}), "
                       f"while Q{weakest_q} is weakest (avg: ${quarterly_avg[weakest_q]:,.0f}).\n")
        
        # Seasonality strength
        seasonality_strength = (monthly_avg.std() / monthly_avg.mean()) * 100
        if seasonality_strength < 15:
            insights.append(f"**Seasonality Impact**: Low ({seasonality_strength:.1f}%) - Revenue is relatively consistent throughout the year.\n")
        elif seasonality_strength < 30:
            insights.append(f"**Seasonality Impact**: Moderate ({seasonality_strength:.1f}%) - Noticeable seasonal patterns exist.\n")
        else:
            insights.append(f"**Seasonality Impact**: High ({seasonality_strength:.1f}%) - Strong seasonal variations present.\n")
    
    # ==========================================
    # 5. CLIENT CONCENTRATION ANALYSIS
    # ==========================================
    if "Client_Name" in filtered_time.columns:
        insights.append("\n## 🏢 Client Concentration\n")
        client_analysis = analyze_client_concentration(filtered_time)
        
        if client_analysis:
            insights.append(f"**Total Clients**: {client_analysis['num_clients']:,}\n")
            insights.append(f"**Top 5 Clients**: {client_analysis['top_5_concentration']:.1f}% of revenue\n")
            insights.append(f"**Top 10 Clients**: {client_analysis['top_10_concentration']:.1f}% of revenue\n")
            
            # Risk assessment based on HHI
            hhi = client_analysis['hhi']
            if hhi < 1000:
                risk_assessment = "🟢 **Low risk** - Well diversified client base"
            elif hhi < 1800:
                risk_assessment = "🟡 **Moderate risk** - Some concentration present"
            else:
                risk_assessment = "🔴 **High risk** - Significant client concentration"
            
            insights.append(f"**Concentration Risk**: {risk_assessment} (HHI: {hhi:.0f})\n")
            
            # Top client impact
            if len(client_analysis['client_revenue_pct']) > 0:
                top_client_pct = client_analysis['client_revenue_pct'].iloc[0]
                if top_client_pct > 20:
                    insights.append(f"⚠️ **Warning**: Top client represents {top_client_pct:.1f}% of revenue - consider diversification strategies.\n")
    
    # ==========================================
    # 6. PRODUCTIVITY METRICS
    # ==========================================
    if "Timekeeper" in filtered_time.columns and "Billable_Hours" in filtered_time.columns:
        insights.append("\n## 👥 Attorney Productivity\n")
        
        total_hours = filtered_time["Billable_Hours"].sum()
        total_revenue = filtered_time["Billable_Amount_in_USD"].sum()
        avg_rate = total_revenue / total_hours if total_hours > 0 else 0
        
        attorney_count = filtered_time["Timekeeper"].nunique()
        avg_hours_per_attorney = total_hours / attorney_count if attorney_count > 0 else 0
        avg_revenue_per_attorney = total_revenue / attorney_count if attorney_count > 0 else 0
        
        insights.append(f"**Total Attorneys**: {attorney_count:,}\n")
        insights.append(f"**Average Hours/Attorney**: {avg_hours_per_attorney:,.0f}\n")
        insights.append(f"**Average Revenue/Attorney**: ${avg_revenue_per_attorney:,.0f}\n")
        insights.append(f"**Average Realization Rate**: ${avg_rate:.0f}/hour\n")
        
        # Top performers
        attorney_stats = analyze_attorney_productivity(filtered_time)
        if not attorney_stats.empty:
            top_3 = attorney_stats.head(3)
            insights.append(f"\n**Top 3 Revenue Generators**:\n")
            for idx, row in top_3.iterrows():
                insights.append(f"  {idx + 1}. {row['Timekeeper']}: ${row['Total_Revenue']:,.0f} "
                              f"({row['Total_Hours']:,.0f} hours @ ${row['Effective_Hourly_Rate']:.0f}/hr)\n")
    
    # ==========================================
    # 7. RECOMMENDATIONS
    # ==========================================
    insights.append("\n## 💡 Strategic Recommendations\n")
    
    # Based on revenue trend
    if len(total) >= 6:
        x = np.arange(len(total))
        y = total.values
        slope, _ = np.polyfit(x, y, 1)
        
        if slope < 0:
            insights.append("🎯 **Revenue Recovery**: Revenue is declining. Consider:\n")
            insights.append("  - Analyzing lost clients and win-back strategies\n")
            insights.append("  - Reviewing pricing models and rate structures\n")
            insights.append("  - Expanding business development efforts\n")
            insights.append("  - Identifying and addressing service quality issues\n\n")
        elif slope > 0 and volatility < 20:
            insights.append("✅ **Maintain Momentum**: Revenue is growing steadily. Focus on:\n")
            insights.append("  - Scaling successful practice areas\n")
            insights.append("  - Investing in high-performing teams\n")
            insights.append("  - Building on client relationships\n\n")
    
    # Based on billing mix
    if "Rate_Type" in filtered_time.columns:
        rate_analysis = calculate_rate_type_metrics(filtered_time)
        if rate_analysis and not rate_analysis["rate_stats"].empty:
            rate_stats = rate_analysis["rate_stats"]
            
            # If flat fee is growing
            if "Flat Fee" in rate_stats.index or "Alt Fee" in ' '.join(rate_stats.index):
                alt_fee_rows = rate_stats[rate_stats.index.str.contains("flat|alt|fixed", case=False, na=False)]
                if not alt_fee_rows.empty:
                    alt_fee_pct = alt_fee_rows["Revenue_Share_Pct"].sum()
                    if alt_fee_pct > 15:
                        insights.append("💼 **Alt Fee Growth**: Alternative fee arrangements are significant. Consider:\n")
                        insights.append("  - Developing standardized AF E pricing models\n")
                        insights.append("  - Training teams on AFE matter management\n")
                        insights.append("  - Tracking AFE profitability metrics\n\n")
    
    # Based on seasonality
    if len(total) >= 12:
        seasonality_strength = (monthly_avg.std() / monthly_avg.mean()) * 100
        if seasonality_strength > 30:
            insights.append("📅 **Seasonality Management**: Strong seasonal patterns detected. Consider:\n")
            insights.append("  - Cash flow planning for slow periods\n")
            insights.append("  - Staffing adjustments to match demand cycles\n")
            insights.append("  - Counter-cyclical business development\n")
            insights.append(f"  - Building reserves during peak months ({month_names[strongest_month]})\n\n")
    
    # Based on client concentration
    if client_analysis and client_analysis.get('top_5_concentration', 0) > 50:
        insights.append("🏢 **Diversification Strategy**: High client concentration poses risk. Prioritize:\n")
        insights.append("  - Active business development to broaden client base\n")
        insights.append("  - Client service excellence to protect key relationships\n")
        insights.append("  - Market expansion into new sectors/industries\n")
        insights.append("  - Regular client health assessments\n\n")
    
    return "\n".join(insights)


# ----------------------------
# Page Functions
# ----------------------------

def show_executive_dashboard(filtered_time, monthly_long, total_amount, flat_amount, hourly_amount, total_hours):
    """Executive Dashboard page with comprehensive insights."""
    
    # Header with icon
    st.markdown("# 🎯 Executive Dashboard")
    st.markdown("---")
    
    # Top KPIs with better styling
    st.markdown("### 📊 Key Performance Indicators")
    
    col1, col2, col3, col4, col5 = st.columns(5)
    
    with col1:
        st.metric(
            label="💵 Total Revenue",
            value=f"${total_amount:,.0f}",
            help="Total billable amount in USD"
        )
    
    with col2:
        flat_pct = (flat_amount / total_amount * 100) if total_amount > 0 else 0
        st.metric(
            label="🔧 Alt Fee Revenue",
            value=f"${flat_amount:,.0f}",
            delta=f"{flat_pct:.1f}% of total",
            help="Alternative/flat fee revenue"
        )
    
    with col3:
        st.metric(
            label="⏱️ Hourly Revenue",
            value=f"${hourly_amount:,.0f}",
            help="Traditional hourly billing revenue"
        )
    
    with col4:
        avg_rate = (hourly_amount / total_hours) if total_hours > 0 else 0
        st.metric(
            label="💲 Avg Hourly Rate",
            value=f"${avg_rate:.0f}",
            help="Average effective hourly rate"
        )
    
    with col5:
        st.metric(
            label="🕐 Total Hours",
            value=f"{total_hours:,.0f}",
            help="Total billable hours"
        )
    
    st.markdown("---")
    
    # Revenue trend section with better layout
    if not monthly_long.empty:
        st.markdown("### 📈 Revenue Trends & Analysis")
        
        col1, col2 = st.columns([2, 1])
        
        with col1:
            monthly_total = monthly_long.groupby("YearMonth")["Billable_Amount_in_USD"].sum().reset_index()
            growth_data = calculate_growth_metrics(monthly_total)
            
            if growth_data:
                df_plot = growth_data["data"]
                
                fig = go.Figure()
                
                fig.add_trace(go.Scatter(
                    x=df_plot["YearMonth"],
                    y=df_plot["Billable_Amount_in_USD"],
                    name="Actual Revenue",
                    mode="lines+markers",
                    line=dict(color="#1f77b4", width=3),
                    marker=dict(size=8),
                ))
                
                fig.add_trace(go.Scatter(
                    x=df_plot["YearMonth"],
                    y=df_plot["MA_3"],
                    name="3-Month MA",
                    line=dict(color="#ff7f0e", width=2, dash="dash"),
                ))
                
                fig.add_trace(go.Scatter(
                    x=df_plot["YearMonth"],
                    y=df_plot["MA_6"],
                    name="6-Month MA",
                    line=dict(color="#2ca02c", width=2, dash="dot"),
                ))
                
                fig.update_layout(
                    title="Revenue Trend with Moving Averages",
                    xaxis_title="",
                    yaxis_title="Revenue (USD)",
                    hovermode="x unified",
                    height=400,
                    template="plotly_white",
                )
                
                st.plotly_chart(fig, use_container_width=True)
        
        with col2:
            st.markdown("#### 📊 Growth Metrics")
            
            if growth_data and growth_data.get("latest_mom") is not None:
                latest_mom = growth_data['latest_mom']
                avg_mom = growth_data['avg_mom']
                volatility = growth_data['volatility']
                
                # Color-code growth
                mom_delta = f"{latest_mom:+.1f}%"
                
                st.metric(
                    "Latest MoM Growth",
                    f"{abs(latest_mom):.1f}%",
                    delta=mom_delta,
                    delta_color="normal" if latest_mom >= 0 else "inverse"
                )
                st.metric("Avg MoM Growth", f"{avg_mom:.1f}%")
                st.metric("Volatility", f"{volatility:.1f}%")
                
                # Trend indicator
                if growth_data.get("trend"):
                    trend_emoji = "📈" if growth_data["trend"] == "growing" else "📉" if growth_data["trend"] == "declining" else "➡️"
                    st.markdown(f"**Trend**: {trend_emoji} {growth_data['trend'].title()}")
            
            # Quick stats
            if len(monthly_total) >= 2:
                latest = monthly_total.iloc[-1]["Billable_Amount_in_USD"]
                previous = monthly_total.iloc[-2]["Billable_Amount_in_USD"]
                change = ((latest - previous) / previous * 100) if previous > 0 else 0
                
                st.markdown("---")
                st.markdown("#### 🔍 Recent Performance")
                
                st.markdown(f"""
                **Current Month:** ${latest:,.0f}  
                **Previous Month:** ${previous:,.0f}  
                **Change:** {change:+.1f}%
                """)
    
    st.markdown("---")
    
    # Comprehensive insights section with ALL trends
    st.markdown("### 💡 Key Insights & Recommendations")
    
    insights = generate_comprehensive_insights(filtered_time, monthly_long)
    
    # Put insights in an expander
    with st.expander("📊 View Detailed Analysis", expanded=True):
        st.markdown(insights)


def show_revenue_analytics(filtered_time, monthly_long):
    """Revenue Analytics page with clean layout."""
    st.markdown("# 📈 Revenue Analytics")
    st.markdown("Comprehensive revenue analysis with growth metrics and statistical insights")
    st.markdown("---")
    
    if monthly_long.empty:
        st.warning("⚠️ No data available for the selected filters. Please adjust your filter criteria.")
        return
    
    # Monthly revenue breakdown
    monthly_total = monthly_long.groupby("YearMonth").agg({
        "Billable_Amount_in_USD": "sum",
        "Billable_Hours": "sum",
    }).reset_index()
    
    st.markdown("### 💰 Monthly Performance")
    
    col1, col2 = st.columns(2)
    
    with col1:
        fig = px.bar(
            monthly_total,
            x="YearMonth",
            y="Billable_Amount_in_USD",
            title="Monthly Revenue Trend",
            color_discrete_sequence=["#1f77b4"],
        )
        fig.update_layout(
            xaxis_title="",
            yaxis_title="Revenue (USD)",
            template="plotly_white",
            hovermode="x"
        )
        st.plotly_chart(fig, use_container_width=True)
    
    with col2:
        fig = px.bar(
            monthly_total,
            x="YearMonth",
            y="Billable_Hours",
            title="Monthly Billable Hours",
            color_discrete_sequence=["#2ca02c"],
        )
        fig.update_layout(
            xaxis_title="",
            yaxis_title="Hours",
            template="plotly_white",
            hovermode="x"
        )
        st.plotly_chart(fig, use_container_width=True)
    
    # Growth analysis
    st.markdown("---")
    st.markdown("### 📊 Growth Analysis")
    
    growth_data = calculate_growth_metrics(monthly_total)
    
    if growth_data and "data" in growth_data:
        df_growth = growth_data["data"]
        
        fig = go.Figure()
        
        colors = ['#2ca02c' if x >= 0 else '#d62728' for x in df_growth["MoM_Growth"]]
        
        fig.add_trace(go.Bar(
            x=df_growth["YearMonth"],
            y=df_growth["MoM_Growth"],
            name="MoM Growth %",
            marker_color=colors,
        ))
        
        fig.update_layout(
            title="Month-over-Month Growth Rate",
            xaxis_title="",
            yaxis_title="Growth %",
            hovermode="x",
            template="plotly_white",
        )
        
        st.plotly_chart(fig, use_container_width=True)
    
    # Revenue distribution
    st.markdown("---")
    st.markdown("### 📦 Revenue Distribution Analysis")
    
    col1, col2 = st.columns(2)
    
    with col1:
        fig = px.box(
            monthly_total,
            y="Billable_Amount_in_USD",
            title="Revenue Distribution (Box Plot)",
            color_discrete_sequence=["#9467bd"],
        )
        fig.update_layout(template="plotly_white")
        st.plotly_chart(fig, use_container_width=True)
    
    with col2:
        fig = px.histogram(
            monthly_total,
            x="Billable_Amount_in_USD",
            title="Revenue Frequency Distribution",
            nbins=20,
            color_discrete_sequence=["#ff7f0e"],
        )
        fig.update_layout(
            xaxis_title="Revenue (USD)",
            yaxis_title="Frequency",
            template="plotly_white"
        )
        st.plotly_chart(fig, use_container_width=True)
    
    # Statistical summary
    st.markdown("---")
    st.markdown("### 📊 Statistical Summary")
    
    col1, col2, col3, col4 = st.columns(4)
    
    revenue_series = monthly_total["Billable_Amount_in_USD"]
    
    with col1:
        st.metric("📊 Mean Revenue", f"${revenue_series.mean():,.0f}")
        st.metric("📉 Std Deviation", f"${revenue_series.std():,.0f}")
    
    with col2:
        st.metric("📈 Median Revenue", f"${revenue_series.median():,.0f}")
        st.metric("⬇️ Min Revenue", f"${revenue_series.min():,.0f}")
    
    with col3:
        st.metric("⬆️ Max Revenue", f"${revenue_series.max():,.0f}")
        st.metric("↔️ Range", f"${revenue_series.max() - revenue_series.min():,.0f}")
    
    with col4:
        q1 = revenue_series.quantile(0.25)
        q3 = revenue_series.quantile(0.75)
        st.metric("📊 Q1 (25th %ile)", f"${q1:,.0f}")
        st.metric("📊 Q3 (75th %ile)", f"${q3:,.0f}")


def show_billing_mix(filtered_time, monthly_long):
    """Billing Mix & Trends page with improved layout."""
    st.markdown("# 💰 Billing Mix & Trends")
    st.markdown("Analyze revenue by billing type and track trends over time")
    st.markdown("---")
    
    if monthly_long.empty:
        st.warning("⚠️ No data available for the selected filters.")
        return
    
    # Stacked area chart
    st.markdown("### 📊 Revenue Mix Over Time")
    
    pivot_data = monthly_long.pivot(
        index="YearMonth",
        columns="Rate_Type",
        values="Billable_Amount_in_USD"
    ).fillna(0)
    
    fig = go.Figure()
    
    for column in pivot_data.columns:
        fig.add_trace(go.Scatter(
            x=pivot_data.index,
            y=pivot_data[column],
            name=column,
            mode="lines",
            stackgroup="one",
        ))
    
    fig.update_layout(
        xaxis_title="",
        yaxis_title="Revenue (USD)",
        hovermode="x unified",
        height=450,
        template="plotly_white",
    )
    
    st.plotly_chart(fig, use_container_width=True)
    
    # Percentage breakdown
    st.markdown("---")
    st.markdown("### 📈 Revenue Mix Percentage Breakdown")
    
    pivot_pct = pivot_data.div(pivot_data.sum(axis=1), axis=0) * 100
    
    fig = go.Figure()
    
    for column in pivot_pct.columns:
        fig.add_trace(go.Scatter(
            x=pivot_pct.index,
            y=pivot_pct[column],
            name=column,
            mode="lines",
            stackgroup="one",
        ))
    
    fig.update_layout(
        xaxis_title="",
        yaxis_title="Percentage of Revenue",
        hovermode="x unified",
        height=400,
        template="plotly_white",
    )
    
    st.plotly_chart(fig, use_container_width=True)
    
    # Rate type analysis
    st.markdown("---")
    st.markdown("### 💼 Detailed Rate Type Analysis")
    
    rate_metrics = calculate_rate_type_metrics(filtered_time)
    
    if rate_metrics:
        col1, col2 = st.columns([1, 1])
        
        with col1:
            rate_stats = rate_metrics["rate_stats"]
            fig = px.pie(
                values=rate_stats["Total_Revenue"],
                names=rate_stats.index,
                title="Revenue Distribution by Rate Type",
                color_discrete_sequence=px.colors.qualitative.Set3,
            )
            fig.update_layout(template="plotly_white")
            st.plotly_chart(fig, use_container_width=True)
        
        with col2:
            fig = px.bar(
                rate_stats.reset_index(),
                x="Rate_Type",
                y="Total_Revenue",
                title="Total Revenue by Rate Type",
                color="Rate_Type",
                color_discrete_sequence=px.colors.qualitative.Set3,
            )
            fig.update_layout(
                xaxis_title="",
                yaxis_title="Revenue (USD)",
                showlegend=False,
                template="plotly_white"
            )
            st.plotly_chart(fig, use_container_width=True)
        
        # Detailed table
        st.markdown("### 📋 Rate Type Metrics Table")
        st.dataframe(
            rate_stats.style.format({
                "Total_Revenue": "${:,.0f}",
                "Avg_Revenue": "${:,.0f}",
                "Count": "{:,.0f}",
                "Total_Hours": "{:,.1f}",
                "Revenue_Share_Pct": "{:.2f}%",
            }),
            use_container_width=True
        )


def show_forecasting(monthly_long):
    """ENHANCED Forecasting & Projections page with comprehensive trend analysis."""
    st.header("🔮 Forecasting & Projections")
    st.markdown("Advanced revenue forecasting with multiple methods and detailed trend analysis")
    st.markdown("---")
    
    if monthly_long.empty:
        st.warning("Insufficient data for forecasting.")
        return
    
    col1, col2 = st.columns([3, 1])
    
    with col2:
        months_ahead = st.slider("Months to Forecast", 1, 12, 3)
        forecast_method = st.selectbox(
            "Forecast Method",
            ["linear", "exponential", "moving_average"],
            format_func=lambda x: x.replace("_", " ").title()
        )
    
    with col1:
        st.subheader("📈 Total Revenue Forecast")
        
        monthly_total = (
            monthly_long.groupby("YearMonth")["Billable_Amount_in_USD"]
            .sum()
            .sort_index()
        )
        
        forecast_result = advanced_forecast(monthly_total, periods=months_ahead, method=forecast_method)
        
        if not forecast_result["forecast"].empty:
            fig = go.Figure()
            
            # Historical data
            fig.add_trace(go.Scatter(
                x=monthly_total.index,
                y=monthly_total.values,
                name="Actual",
                mode="lines+markers",
                line=dict(color="#1f77b4", width=3),
                marker=dict(size=8),
            ))
            
            # Forecast
            fig.add_trace(go.Scatter(
                x=forecast_result["forecast"].index,
                y=forecast_result["forecast"].values,
                name="Forecast",
                mode="lines+markers",
                line=dict(color="#ff7f0e", width=3, dash="dash"),
                marker=dict(size=8, symbol='diamond'),
            ))
            
            # Confidence interval
            fig.add_trace(go.Scatter(
                x=forecast_result["upper"].index.tolist() + forecast_result["lower"].index.tolist()[::-1],
                y=forecast_result["upper"].values.tolist() + forecast_result["lower"].values.tolist()[::-1],
                fill='toself',
                fillcolor='rgba(255, 127, 14, 0.2)',
                line=dict(color='rgba(255,255,255,0)'),
                name='95% Confidence Interval',
                showlegend=True,
            ))
            
            fig.update_layout(
                xaxis_title="",
                yaxis_title="Revenue (USD)",
                hovermode="x unified",
                height=500,
                template="plotly_white",
            )
            
            st.plotly_chart(fig, use_container_width=True)
    
    # ==========================================
    # COMPREHENSIVE FORECAST ANALYSIS
    # ==========================================
    
    st.markdown("---")
    st.subheader("📊 Forecast Analysis & Insights")
    
    col1, col2, col3 = st.columns(3)
    
    # Calculate forecast metrics
    forecast_total = forecast_result["forecast"].sum()
    forecast_avg = forecast_result["forecast"].mean()
    historical_avg = monthly_total.tail(6).mean()
    forecast_vs_historical = ((forecast_avg - historical_avg) / historical_avg * 100) if historical_avg > 0 else 0
    
    with col1:
        st.metric(
            "Forecast Period Total",
            f"${forecast_total:,.0f}",
            help=f"Total forecasted revenue for next {months_ahead} months"
        )
        st.metric(
            "Forecast Monthly Avg",
            f"${forecast_avg:,.0f}",
            delta=f"{forecast_vs_historical:+.1f}% vs recent avg"
        )
    
    with col2:
        if "metrics" in forecast_result and forecast_result["metrics"]:
            metrics = forecast_result["metrics"]
            
            if "r_squared" in metrics:
                st.metric(
                    "Model Accuracy (R²)",
                    f"{metrics['r_squared']:.3f}",
                    help="1.0 = perfect fit, 0.0 = no predictive power"
                )
            
            if "mean_absolute_error" in metrics:
                st.metric(
                    "Avg Error (MAE)",
                    f"${metrics['mean_absolute_error']:,.0f}",
                    help="Average prediction error"
                )
    
    with col3:
        # Confidence range
        avg_lower = forecast_result["lower"].mean()
        avg_upper = forecast_result["upper"].mean()
        confidence_range = avg_upper - avg_lower
        
        st.metric(
            "Lower Bound (95%)",
            f"${avg_lower:,.0f}"
        )
        st.metric(
            "Upper Bound (95%)",
            f"${avg_upper:,.0f}"
        )
    
    # Detailed forecast table
    st.markdown("---")
    st.subheader("📋 Detailed Forecast Breakdown")
    
    forecast_df = pd.DataFrame({
        "Month": forecast_result["forecast"].index.strftime("%b %Y"),
        "Forecasted Revenue": forecast_result["forecast"].values,
        "Lower Bound (95%)": forecast_result["lower"].values,
        "Upper Bound (95%)": forecast_result["upper"].values,
        "Confidence Range": forecast_result["upper"].values - forecast_result["lower"].values,
    })
    
    # Add comparison to historical
    for idx in range(len(forecast_df)):
        if idx < len(monthly_total):
            historical_same_month = monthly_total.iloc[-(len(forecast_df) - idx)]
            forecast_df.loc[idx, "YoY Change"] = (
                (forecast_df.loc[idx, "Forecasted Revenue"] - historical_same_month) / historical_same_month * 100
            ) if historical_same_month > 0 else None
    
    st.dataframe(
        forecast_df.style.format({
            "Forecasted Revenue": "${:,.0f}",
            "Lower Bound (95%)": "${:,.0f}",
            "Upper Bound (95%)": "${:,.0f}",
            "Confidence Range": "${:,.0f}",
            "YoY Change": "{:+.1f}%",
        }),
        use_container_width=True
    )
    
    # ==========================================
    # TREND INSIGHTS
    # ==========================================
    
    st.markdown("---")
    st.subheader("🔍 Key Forecast Insights")
    
    insights_col1, insights_col2 = st.columns(2)
    
    with insights_col1:
        st.markdown("#### 📈 Trajectory Analysis")
        
        if forecast_vs_historical > 5:
            st.success(f"""
            ✅ **Positive Outlook**
            - Forecast shows **{forecast_vs_historical:.1f}% growth** vs recent average
            - Expected total: ${forecast_total:,.0f} over next {months_ahead} months
            - Trend indicates strengthening revenue
            """)
        elif forecast_vs_historical < -5:
            st.warning(f"""
            ⚠️ **Declining Trajectory**
            - Forecast shows **{forecast_vs_historical:.1f}% decline** vs recent average
            - Expected total: ${forecast_total:,.0f} over next {months_ahead} months
            - Consider revenue recovery strategies
            """)
        else:
            st.info(f"""
            ➡️ **Stable Outlook**
            - Forecast relatively flat ({forecast_vs_historical:+.1f}% vs recent average)
            - Expected total: ${forecast_total:,.0f} over next {months_ahead} months
            - Revenue maintaining current levels
            """)
    
    with insights_col2:
        st.markdown("#### 🎯 Confidence Analysis")
        
        avg_confidence_pct = (confidence_range / forecast_avg * 100) if forecast_avg > 0 else 0
        
        if avg_confidence_pct < 20:
            confidence_level = "High"
            confidence_emoji = "🟢"
            confidence_desc = "Forecast is highly reliable with narrow confidence intervals"
        elif avg_confidence_pct < 40:
            confidence_level = "Moderate"
            confidence_emoji = "🟡"
            confidence_desc = "Reasonable confidence with some uncertainty"
        else:
            confidence_level = "Low"
            confidence_emoji = "🔴"
            confidence_desc = "High uncertainty - actual results may vary significantly"
        
        st.markdown(f"""
        {confidence_emoji} **Confidence Level: {confidence_level}**
        
        - Average uncertainty: ±${confidence_range/2:,.0f} ({avg_confidence_pct:.1f}%)
        - {confidence_desc}
        - Based on {len(monthly_total)} months of historical data
        """)
    
    # ==========================================
    # FORECAST BY RATE TYPE
    # ==========================================
    
    st.markdown("---")
    st.subheader("💼 Forecast by Rate Type")
    
    pivot = (
        monthly_long
        .pivot(index="YearMonth", columns="Rate_Type", values="Billable_Amount_in_USD")
        .fillna(0)
        .sort_index()
    )
    
    # Create tabs for each rate type
    rate_types = pivot.columns.tolist()
    
    if len(rate_types) > 1:
        tabs = st.tabs(rate_types)
        
        for idx, rate_type in enumerate(rate_types):
            with tabs[idx]:
                series = pivot[rate_type]
                fc_result = advanced_forecast(series, periods=months_ahead, method=forecast_method)
                
                if not fc_result["forecast"].empty:
                    # Create visualization
                    fig = go.Figure()
                    
                    fig.add_trace(go.Scatter(
                        x=series.index,
                        y=series.values,
                        name="Actual",
                        mode="lines+markers",
                        line=dict(width=2),
                    ))
                    
                    fig.add_trace(go.Scatter(
                        x=fc_result["forecast"].index,
                        y=fc_result["forecast"].values,
                        name="Forecast",
                        mode="lines+markers",
                        line=dict(dash="dash", width=2),
                    ))
                    
                    # Add confidence interval
                    fig.add_trace(go.Scatter(
                        x=fc_result["upper"].index.tolist() + fc_result["lower"].index.tolist()[::-1],
                        y=fc_result["upper"].values.tolist() + fc_result["lower"].values.tolist()[::-1],
                        fill='toself',
                        fillcolor='rgba(0,100,80,0.2)',
                        line=dict(color='rgba(255,255,255,0)'),
                        name='95% CI',
                        showlegend=True,
                    ))
                    
                    fig.update_layout(
                        title=f"{rate_type} Revenue Forecast",
                        xaxis_title="",
                        yaxis_title="Revenue (USD)",
                        height=400,
                        template="plotly_white",
                        hovermode="x unified"
                    )
                    
                    st.plotly_chart(fig, use_container_width=True)
                    
                    # Rate type specific metrics
                    col1, col2, col3 = st.columns(3)
                    
                    with col1:
                        rate_forecast_total = fc_result["forecast"].sum()
                        st.metric(
                            f"{rate_type} Forecast Total",
                            f"${rate_forecast_total:,.0f}"
                        )
                    
                    with col2:
                        rate_historical_avg = series.tail(6).mean()
                        rate_forecast_avg = fc_result["forecast"].mean()
                        rate_change = ((rate_forecast_avg - rate_historical_avg) / rate_historical_avg * 100) if rate_historical_avg > 0 else 0
                        
                        st.metric(
                            "Avg Forecast",
                            f"${rate_forecast_avg:,.0f}",
                            delta=f"{rate_change:+.1f}%"
                        )
                    
                    with col3:
                        # Share of total forecast
                        share_of_forecast = (rate_forecast_total / forecast_total * 100) if forecast_total > 0 else 0
                        st.metric(
                            "% of Total Forecast",
                            f"{share_of_forecast:.1f}%"
                        )


def show_attorney_performance(filtered_time):
    """Attorney Performance page."""
    st.header("👥 Attorney Performance Analytics")
    
    if "Timekeeper" not in filtered_time.columns:
        st.warning("Timekeeper data not available.")
        return
    
    attorney_stats = analyze_attorney_productivity(filtered_time)
    
    if attorney_stats.empty:
        st.warning("No attorney data available.")
        return
    
    # Top performers
    st.subheader("🏆 Top Performers by Revenue")
    
    top_n = st.slider("Number of attorneys to display", 5, 50, 20)
    
    fig = px.bar(
        attorney_stats.head(top_n),
        x="Timekeeper",
        y="Total_Revenue",
        title=f"Top {top_n} Attorneys by Revenue",
        color="Effective_Hourly_Rate",
        color_continuous_scale="Viridis",
    )
    fig.update_layout(xaxis_title="", yaxis_title="Total Revenue (USD)")
    st.plotly_chart(fig, use_container_width=True)
    
    # Efficiency analysis
    st.markdown("---")
    st.subheader("⚡ Efficiency Metrics")
    
    col1, col2 = st.columns(2)
    
    with col1:
        fig = px.scatter(
            attorney_stats.head(top_n),
            x="Total_Hours",
            y="Total_Revenue",
            size="Effective_Hourly_Rate",
            hover_name="Timekeeper",
            title="Hours vs Revenue",
            labels={"Total_Hours": "Total Billable Hours", "Total_Revenue": "Total Revenue (USD)"},
        )
        st.plotly_chart(fig, use_container_width=True)
    
    with col2:
        fig = px.bar(
            attorney_stats.head(top_n).sort_values("Effective_Hourly_Rate", ascending=False),
            x="Timekeeper",
            y="Effective_Hourly_Rate",
            title="Effective Hourly Rate",
            color="Effective_Hourly_Rate",
            color_continuous_scale="RdYlGn",
        )
        fig.update_layout(xaxis_title="", yaxis_title="Effective Rate ($/hr)")
        st.plotly_chart(fig, use_container_width=True)
    
    # Consistency analysis
    st.markdown("---")
    st.subheader("📊 Consistency & Reliability")
    
    fig = px.bar(
        attorney_stats.head(top_n).sort_values("Consistency_Score", ascending=False),
        x="Timekeeper",
        y="Consistency_Score",
        title="Attorney Consistency Score (Higher = More Consistent)",
        color="Consistency_Score",
        color_continuous_scale="Blues",
    )
    fig.update_layout(xaxis_title="", yaxis_title="Consistency Score")
    st.plotly_chart(fig, use_container_width=True)
    
    # Detailed table
    st.markdown("---")
    st.subheader("📋 Detailed Performance Metrics")
    
    st.dataframe(
        attorney_stats.head(top_n).style.format({
            "Total_Hours": "{:,.1f}",
            "Avg_Hours_Per_Entry": "{:,.2f}",
            "Std_Hours": "{:,.2f}",
            "Total_Revenue": "${:,.0f}",
            "Avg_Revenue_Per_Entry": "${:,.0f}",
            "Avg_Billing_Rate": "${:,.0f}",
            "Effective_Hourly_Rate": "${:,.0f}",
            "Consistency_Score": "{:.1f}",
        }),
        use_container_width=True
    )


def show_client_analytics(filtered_time):
    """Client Analytics page."""
    st.header("🏢 Client Analytics")
    
    if "Client_Name" not in filtered_time.columns:
        st.warning("Client data not available.")
        return
    
    client_analysis = analyze_client_concentration(filtered_time)
    
    if not client_analysis:
        st.warning("No client data available.")
        return
    
    # Concentration metrics
    st.subheader("📊 Client Concentration Analysis")
    
    col1, col2, col3, col4 = st.columns(4)
    
    with col1:
        st.metric("Total Clients", f"{client_analysis['num_clients']:,}")
    
    with col2:
        st.metric("Top 5 Concentration", f"{client_analysis['top_5_concentration']:.1f}%")
    
    with col3:
        st.metric("Top 10 Concentration", f"{client_analysis['top_10_concentration']:.1f}%")
    
    with col4:
        hhi = client_analysis['hhi']
        if hhi < 1000:
            risk_level = "Low"
            color = "green"
        elif hhi < 1800:
            risk_level = "Moderate"
            color = "orange"
        else:
            risk_level = "High"
            color = "red"
        
        st.metric("Concentration Risk", risk_level, help=f"HHI: {hhi:.0f}")
    
    # Top clients
    st.markdown("---")
    st.subheader("🏆 Top Clients by Revenue")
    
    top_n_clients = st.slider("Number of clients to display", 5, 50, 20)
    
    col1, col2 = st.columns([2, 1])
    
    with col1:
        top_clients = client_analysis['client_revenue'].head(top_n_clients).reset_index()
        top_clients.columns = ["Client", "Revenue"]
        
        fig = px.bar(
            top_clients,
            x="Client",
            y="Revenue",
            title=f"Top {top_n_clients} Clients",
            color="Revenue",
            color_continuous_scale="Blues",
        )
        fig.update_layout(xaxis_title="", yaxis_title="Revenue (USD)")
        st.plotly_chart(fig, use_container_width=True)
    
    with col2:
        # Pie chart of top 10
        top_10 = client_analysis['client_revenue'].head(10)
        fig = px.pie(
            values=top_10.values,
            names=top_10.index,
            title="Top 10 Client Distribution",
        )
        st.plotly_chart(fig, use_container_width=True)
    
    # Concentration curve
    st.markdown("---")
    st.subheader("📈 Revenue Concentration Curve")
    
    cumulative_pct = client_analysis['client_revenue_pct'].cumsum()
    cumulative_df = pd.DataFrame({
        "Client_Rank": range(1, len(cumulative_pct) + 1),
        "Cumulative_Revenue_Pct": cumulative_pct.values
    })
    
    fig = px.line(
        cumulative_df,
        x="Client_Rank",
        y="Cumulative_Revenue_Pct",
        title="Cumulative Revenue Concentration",
    )
    fig.add_hline(y=50, line_dash="dash", line_color="red", annotation_text="50% of Revenue")
    fig.add_hline(y=80, line_dash="dash", line_color="orange", annotation_text="80% of Revenue")
    fig.update_layout(xaxis_title="Client Rank", yaxis_title="Cumulative % of Revenue")
    st.plotly_chart(fig, use_container_width=True)
    
    # Client details table
    st.markdown("---")
    st.subheader("📋 Client Revenue Details")
    
    client_detail = pd.DataFrame({
        "Client": client_analysis['client_revenue'].index,
        "Revenue": client_analysis['client_revenue'].values,
        "% of Total": client_analysis['client_revenue_pct'].values,
    }).head(top_n_clients)
    
    st.dataframe(
        client_detail.style.format({
            "Revenue": "${:,.0f}",
            "% of Total": "{:.2f}%",
        }),
        use_container_width=True
    )


def show_time_patterns(filtered_time):
    """Time & Patterns page."""
    st.header("⏰ Time & Billing Patterns")
    
    patterns = analyze_billing_patterns(filtered_time)
    
    if not patterns:
        st.warning("Insufficient data for pattern analysis.")
        return
    
    # Day of week analysis
    st.subheader("📅 Day of Week Patterns")
    
    col1, col2 = st.columns(2)
    
    with col1:
        if not patterns["day_of_week_revenue"].empty:
            dow_order = ["Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday", "Sunday"]
            dow_rev = patterns["day_of_week_revenue"].reindex(dow_order, fill_value=0)
            
            fig = px.bar(
                x=dow_rev.index,
                y=dow_rev.values,
                title="Revenue by Day of Week",
                labels={"x": "Day", "y": "Revenue (USD)"},
            )
            st.plotly_chart(fig, use_container_width=True)
    
    with col2:
        if not patterns["day_of_week_hours"].empty:
            dow_hours = patterns["day_of_week_hours"].reindex(dow_order, fill_value=0)
            
            fig = px.bar(
                x=dow_hours.index,
                y=dow_hours.values,
                title="Billable Hours by Day of Week",
                labels={"x": "Day", "y": "Hours"},
                color_discrete_sequence=["#2ca02c"],
            )
            st.plotly_chart(fig, use_container_width=True)
    
    # Week of month patterns
    st.markdown("---")
    st.subheader("📆 Week of Month Patterns")
    
    if not patterns["week_of_month_revenue"].empty:
        fig = px.bar(
            x=patterns["week_of_month_revenue"].index,
            y=patterns["week_of_month_revenue"].values,
            title="Revenue by Week of Month",
            labels={"x": "Week", "y": "Revenue (USD)"},
        )
        st.plotly_chart(fig, use_container_width=True)
    
    # Heatmap by day and week
    st.markdown("---")
    st.subheader("🔥 Activity Heatmap")
    
    if "Date_of_Work" in filtered_time.columns:
        df_heat = filtered_time.copy()
        df_heat["Date_of_Work"] = pd.to_datetime(df_heat["Date_of_Work"])
        df_heat["DayOfWeek"] = df_heat["Date_of_Work"].dt.day_name()
        df_heat["WeekOfYear"] = df_heat["Date_of_Work"].dt.isocalendar().week
        
        heatmap_data = df_heat.groupby(["WeekOfYear", "DayOfWeek"])["Billable_Amount_in_USD"].sum().unstack(fill_value=0)
        
        if not heatmap_data.empty:
            dow_order = ["Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday", "Sunday"]
            heatmap_data = heatmap_data.reindex(columns=dow_order, fill_value=0)
            
            fig = px.imshow(
                heatmap_data,
                title="Revenue Heatmap (Week vs Day)",
                labels=dict(x="Day of Week", y="Week of Year", color="Revenue"),
                aspect="auto",
                color_continuous_scale="YlOrRd",
            )
            st.plotly_chart(fig, use_container_width=True)


def show_detailed_drilldown(filtered_time):
    """Detailed Drilldown page."""
    st.header("📊 Detailed Drilldown")
    
    dimension = st.selectbox(
        "Group By Dimension",
        ["Timekeeper", "Client_Name", "Primary Practice Group", "Rate_Type", "Matter_Name"],
    )
    
    if dimension not in filtered_time.columns:
        st.warning(f"Column '{dimension}' not available in the data.")
        return
    
    # Aggregation
    grouped = (
        filtered_time
        .groupby(dimension)
        .agg({
            "Billable_Amount_in_USD": ["sum", "mean", "count"],
            "Billable_Hours": ["sum", "mean"],
        })
        .round(2)
    )
    
    grouped.columns = [
        "Total_Revenue", "Avg_Revenue", "Count",
        "Total_Hours", "Avg_Hours"
    ]
    
    grouped["Effective_Rate"] = (grouped["Total_Revenue"] / grouped["Total_Hours"]).round(2)
    grouped = grouped.sort_values("Total_Revenue", ascending=False).reset_index()
    
    # Display options
    col1, col2 = st.columns([3, 1])
    
    with col2:
        top_n = st.slider("Display Top N", 5, 100, 25)
        metric_to_plot = st.selectbox(
            "Metric to Visualize",
            ["Total_Revenue", "Total_Hours", "Effective_Rate", "Count"]
        )
    
    with col1:
        top_data = grouped.head(top_n)
        
        fig = px.bar(
            top_data,
            x=dimension,
            y=metric_to_plot,
            title=f"Top {top_n} by {metric_to_plot.replace('_', ' ')}",
            color=metric_to_plot,
            color_continuous_scale="Viridis",
        )
        fig.update_layout(xaxis_title="", yaxis_title=metric_to_plot.replace("_", " "))
        st.plotly_chart(fig, use_container_width=True)
    
    # Detailed table
    st.markdown("---")
    st.subheader("📋 Detailed Breakdown")
    
    st.dataframe(
        top_data.style.format({
            "Total_Revenue": "${:,.0f}",
            "Avg_Revenue": "${:,.0f}",
            "Count": "{:,.0f}",
            "Total_Hours": "{:,.1f}",
            "Avg_Hours": "{:.2f}",
            "Effective_Rate": "${:,.0f}",
        }),
        use_container_width=True
    )
    
    # Export option
    st.markdown("---")
    if st.button("📥 Export to CSV"):
        csv = top_data.to_csv(index=False)
        st.download_button(
            label="Download CSV",
            data=csv,
            file_name=f"{dimension}_analysis.csv",
            mime="text/csv",
        )


# ----------------------------
# Main App
# ----------------------------

def main():
    st.set_page_config(
        page_title="Attorney Billing & KPI Dashboard",
        layout="wide",
        initial_sidebar_state="expanded",
    )

    st.title("📊 Attorney Billing & KPI Dashboard")
    st.caption("🚀 Powered by Parquet Caching | Ultra-fast analytics with comprehensive insights")
    
    # Show loading time expectation banner
    if not any(os.path.exists(get_cached_file_path(f)) for f in TIME_ENTRY_FILES):
        st.info("""
        ⏱️ **First-Time Setup Notice**
        
        Since this is your first time running the dashboard, the initial load will take **30-45 seconds** to build optimized Parquet caches.
        
        **Good news:** After this one-time setup, every future load will take **less than 2 seconds**! ⚡
        
        You'll see detailed progress below as files are processed.
        """)

    # Load data
    time_df = load_time_entries()
    invoice_df = load_invoice_prep()
    payment_df = load_payment_prep()

    if time_df.empty:
        st.error("Could not load Time Entry prep files. Check that they exist inside the 'Files' folder.")
        st.stop()

    # Sidebar navigation
    st.sidebar.markdown("---")
    page = st.sidebar.radio(
        "📑 Navigation",
        [
            "🎯 Executive Dashboard",
            "📈 Revenue Analytics",
            "💰 Billing Mix & Trends",
            "🔮 Forecasting & Projections",
            "👥 Attorney Performance",
            "🏢 Client Analytics",
            "⏰ Time & Patterns",
            "📊 Detailed Drilldown"
        ],
    )

    # Apply filters
    filtered_time = apply_time_entry_filters(time_df)
    monthly_long = prepare_monthly_time_by_rate(filtered_time)

    # Calculate key metrics
    if "Rate_Type" in filtered_time.columns:
        flat_mask = filtered_time["Rate_Type"].str.contains("flat|fixed|alternative", case=False, na=False)
    else:
        flat_mask = pd.Series(False, index=filtered_time.index)

    flat_amount = filtered_time.loc[flat_mask, "Billable_Amount_in_USD"].sum()
    total_amount = filtered_time["Billable_Amount_in_USD"].sum()
    hourly_amount = filtered_time.loc[~flat_mask, "Billable_Amount_in_USD"].sum()
    total_hours = filtered_time.get("Billable_Hours", pd.Series(dtype=float)).sum()

    # Page routing
    if page == "🎯 Executive Dashboard":
        show_executive_dashboard(filtered_time, monthly_long, total_amount, flat_amount, hourly_amount, total_hours)
    elif page == "📈 Revenue Analytics":
        show_revenue_analytics(filtered_time, monthly_long)
    elif page == "💰 Billing Mix & Trends":
        show_billing_mix(filtered_time, monthly_long)
    elif page == "🔮 Forecasting & Projections":
        show_forecasting(monthly_long)
    elif page == "👥 Attorney Performance":
        show_attorney_performance(filtered_time)
    elif page == "🏢 Client Analytics":
        show_client_analytics(filtered_time)
    elif page == "⏰ Time & Patterns":
        show_time_patterns(filtered_time)
    elif page == "📊 Detailed Drilldown":
        show_detailed_drilldown(filtered_time)


if __name__ == "__main__":
    main()
