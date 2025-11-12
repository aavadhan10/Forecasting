import os
import numpy as np
import pandas as pd
import streamlit as st
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots
from datetime import datetime, timedelta
from scipy import stats

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
# Basic password protection with session persistence
# ----------------------------

PASSWORD = "TrendsAI2025"


def check_password() -> bool:
    """Simple password gate using session_state with persistence."""
    if st.session_state.get("password_correct", False):
        return True
    
    def password_entered():
        if st.session_state.get("password") == PASSWORD:
            st.session_state["password_correct"] = True
            del st.session_state["password"]
        else:
            st.session_state["password_correct"] = False

    st.text_input(
        "Enter password",
        type="password",
        on_change=password_entered,
        key="password",
    )
    
    if st.session_state.get("password_correct") == False:
        st.error("❌ Incorrect password.")
    
    return False


# ----------------------------
# Data loading & cleaning (optimized)
# ----------------------------

@st.cache_data(show_spinner=True, ttl=3600)
def load_time_entries() -> pd.DataFrame:
    """
    Optimized loader with parquet caching.
    """
    parquet_path = os.path.join(DATA_DIR, "time_entries.parquet")

    if os.path.exists(parquet_path):
        try:
            df = pd.read_parquet(parquet_path)
            return df
        except Exception:
            pass

    frames = []
    for filename in TIME_ENTRY_FILES:
        path = os.path.join(DATA_DIR, filename)
        try:
            df_raw = pd.read_excel(path, engine="openpyxl")
        except FileNotFoundError:
            continue

        if "ELIMINATED BILLING ORIGINATORS AND ALL Non-Billable Hours" in df_raw.columns:
            header_row = df_raw.iloc[0]
            df = df_raw[1:].copy()
            df.columns = header_row
        else:
            df = df_raw.copy()

        frames.append(df)

    if not frames:
        return pd.DataFrame()

    df = pd.concat(frames, ignore_index=True)

    # Standardize columns
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

    try:
        df.to_parquet(parquet_path, index=False)
    except Exception:
        pass

    return df


@st.cache_data(show_spinner=True, ttl=3600)
def load_invoice_prep() -> pd.DataFrame:
    """Load Invoice Prep file."""
    path = os.path.join(DATA_DIR, INVOICE_FILE)
    try:
        df = pd.read_excel(path, engine="openpyxl")
    except FileNotFoundError:
        return pd.DataFrame()

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

    return df


@st.cache_data(show_spinner=True, ttl=3600)
def load_payment_prep() -> pd.DataFrame:
    """Load Payment Prep file."""
    path = os.path.join(DATA_DIR, PAYMENT_FILE)
    try:
        df_raw = pd.read_excel(path, engine="openpyxl")
    except FileNotFoundError:
        return pd.DataFrame()

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
    
    return {
        "data": df,
        "latest_mom": df["MoM_Growth"].iloc[-1] if len(df) > 0 else None,
        "avg_mom": df["MoM_Growth"].mean(),
        "volatility": df["MoM_Growth"].std(),
    }


def calculate_realization_rates(time_df: pd.DataFrame, invoice_df: pd.DataFrame) -> pd.DataFrame:
    """Calculate realization rates (billed vs collected)."""
    if time_df.empty or invoice_df.empty:
        return pd.DataFrame()
    
    # Group by month
    time_monthly = time_df.groupby(
        pd.to_datetime(time_df["Date_of_Work"]).dt.to_period("M")
    )["Billable_Amount_in_USD"].sum()
    
    invoice_monthly = invoice_df.groupby(
        pd.to_datetime(invoice_df["Invoice Date"]).dt.to_period("M")
    )["Net Labor Billings"].sum()
    
    combined = pd.DataFrame({
        "Time_Billed": time_monthly,
        "Invoice_Amount": invoice_monthly
    }).fillna(0)
    
    combined["Realization_Rate"] = np.where(
        combined["Time_Billed"] > 0,
        (combined["Invoice_Amount"] / combined["Time_Billed"]) * 100,
        0
    )
    
    return combined.reset_index()


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
    """
    series = series.dropna()
    if len(series) < 3:
        return {"forecast": pd.Series(dtype=float), "lower": pd.Series(dtype=float), "upper": pd.Series(dtype=float)}

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
        
        # 95% confidence interval
        margin = 1.96 * std_error
        
    elif method == "exponential":
        # Exponential smoothing
        alpha = 0.3
        forecast_values = []
        level = y[0]
        
        for val in y:
            level = alpha * val + (1 - alpha) * level
        
        for _ in range(periods):
            forecast_values.append(level)
        
        forecast_values = np.array(forecast_values)
        std_error = np.std(y - level)
        margin = 1.96 * std_error
    
    else:  # moving average
        window = min(3, len(series))
        ma_value = series.tail(window).mean()
        forecast_values = np.full(periods, ma_value)
        std_error = series.tail(window).std()
        margin = 1.96 * std_error
    
    # Ensure non-negative
    forecast_values = np.maximum(forecast_values, 0)
    
    future_index = pd.period_range(
        start=(series.index[-1] + 1),
        periods=periods,
        freq="M",
    ).to_timestamp()
    
    forecast_series = pd.Series(forecast_values, index=future_index)
    lower_bound = pd.Series(np.maximum(forecast_values - margin, 0), index=future_index)
    upper_bound = pd.Series(forecast_values + margin, index=future_index)
    
    return {
        "forecast": forecast_series,
        "lower": lower_bound,
        "upper": upper_bound,
    }


def generate_comprehensive_insights(filtered_time: pd.DataFrame, monthly_long: pd.DataFrame) -> str:
    """Generate comprehensive business insights."""
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
    
    if len(total) >= 2:
        recent_3m = total.tail(3).mean()
        prior_3m = total.iloc[-6:-3].mean() if len(total) >= 6 else total.iloc[:-3].mean() if len(total) > 3 else total.iloc[0]
        
        if prior_3m > 0:
            growth = ((recent_3m - prior_3m) / prior_3m) * 100
            insights.append(f"**Revenue Trend**: The most recent 3-month average revenue (${recent_3m:,.0f}) is "
                          f"{'**up ' + f'{growth:.1f}%**' if growth > 0 else '**down ' + f'{abs(growth):.1f}%**'} "
                          f"compared to the prior 3-month period (${prior_3m:,.0f}).\n")
    
    # Volatility assessment
    if len(total) >= 6:
        volatility = total.pct_change().std() * 100
        if volatility < 10:
            stability = "very stable"
        elif volatility < 20:
            stability = "moderately stable"
        elif volatility < 30:
            stability = "somewhat volatile"
        else:
            stability = "highly volatile"
        
        insights.append(f"**Revenue Stability**: Month-over-month revenue shows {stability} patterns "
                       f"(volatility: {volatility:.1f}%).\n")
    
    # Billing mix analysis
    if len(pivot.columns) > 1:
        insights.append("\n## 💼 Billing Mix Analysis\n")
        
        for col in pivot.columns:
            col_total = pivot[col].sum()
            col_pct = (col_total / total.sum()) * 100
            col_trend = pivot[col].tail(3).mean() - pivot[col].iloc[-6:-3].mean() if len(pivot) >= 6 else 0
            
            insights.append(f"**{col}**: ${col_total:,.0f} ({col_pct:.1f}% of total) - "
                          f"{'Trending up' if col_trend > 0 else 'Trending down' if col_trend < 0 else 'Stable'} "
                          f"in recent months.\n")
    
    # Seasonality
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
    
    return "\n".join(insights)


# ----------------------------
# Streamlit app layout
# ----------------------------

def main():
    st.set_page_config(
        page_title="Attorney Billing & KPI Dashboard",
        layout="wide",
        initial_sidebar_state="expanded",
    )

    if not check_password():
        st.stop()

    st.title("📊 Attorney Billing & KPI Dashboard")
    st.caption("Comprehensive analytics with forecasting, trends, and actionable insights")

    # Load data
    with st.spinner("Loading data..."):
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


def show_executive_dashboard(filtered_time, monthly_long, total_amount, flat_amount, hourly_amount, total_hours):
    """Executive Dashboard page."""
    st.header("🎯 Executive Dashboard")
    
    # Top KPIs
    col1, col2, col3, col4, col5 = st.columns(5)
    
    with col1:
        st.metric("💵 Total Revenue", f"${total_amount:,.0f}")
    
    with col2:
        flat_pct = (flat_amount / total_amount * 100) if total_amount > 0 else 0
        st.metric("🔧 Alt Fee Revenue", f"${flat_amount:,.0f}", delta=f"{flat_pct:.1f}%")
    
    with col3:
        st.metric("⏱️ Hourly Revenue", f"${hourly_amount:,.0f}")
    
    with col4:
        avg_rate = (hourly_amount / total_hours) if total_hours > 0 else 0
        st.metric("💲 Avg Hourly Rate", f"${avg_rate:.0f}")
    
    with col5:
        st.metric("🕐 Total Hours", f"{total_hours:,.0f}")
    
    st.markdown("---")
    
    # Revenue trend with moving averages
    if not monthly_long.empty:
        col1, col2 = st.columns([2, 1])
        
        with col1:
            st.subheader("📊 Revenue Trend & Moving Averages")
            
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
                    xaxis_title="",
                    yaxis_title="Revenue (USD)",
                    hovermode="x unified",
                    height=400,
                )
                
                st.plotly_chart(fig, use_container_width=True)
        
        with col2:
            st.subheader("📈 Growth Metrics")
            
            if growth_data and growth_data.get("latest_mom") is not None:
                st.metric("Latest MoM Growth", f"{growth_data['latest_mom']:.1f}%")
                st.metric("Avg MoM Growth", f"{growth_data['avg_mom']:.1f}%")
                st.metric("Volatility (Std Dev)", f"{growth_data['volatility']:.1f}%")
            
            # Quick stats
            if len(monthly_total) >= 2:
                latest = monthly_total.iloc[-1]["Billable_Amount_in_USD"]
                previous = monthly_total.iloc[-2]["Billable_Amount_in_USD"]
                change = ((latest - previous) / previous * 100) if previous > 0 else 0
                
                st.markdown("---")
                st.markdown("**Recent Performance:**")
                st.write(f"Current Month: ${latest:,.0f}")
                st.write(f"Previous Month: ${previous:,.0f}")
                st.write(f"Change: {change:+.1f}%")
    
    st.markdown("---")
    
    # Comprehensive insights
    st.subheader("💡 Key Insights & Recommendations")
    insights = generate_comprehensive_insights(filtered_time, monthly_long)
    st.markdown(insights)


def show_revenue_analytics(filtered_time, monthly_long):
    """Revenue Analytics page."""
    st.header("📈 Revenue Analytics")
    
    if monthly_long.empty:
        st.warning("No data available for the selected filters.")
        return
    
    # Monthly revenue breakdown
    monthly_total = monthly_long.groupby("YearMonth").agg({
        "Billable_Amount_in_USD": "sum",
        "Billable_Hours": "sum",
    }).reset_index()
    
    col1, col2 = st.columns(2)
    
    with col1:
        st.subheader("💰 Monthly Revenue")
        fig = px.bar(
            monthly_total,
            x="YearMonth",
            y="Billable_Amount_in_USD",
            title="Monthly Revenue Trend",
        )
        fig.update_layout(xaxis_title="", yaxis_title="Revenue (USD)")
        st.plotly_chart(fig, use_container_width=True)
    
    with col2:
        st.subheader("⏱️ Monthly Hours")
        fig = px.bar(
            monthly_total,
            x="YearMonth",
            y="Billable_Hours",
            title="Monthly Billable Hours",
            color_discrete_sequence=["#2ca02c"],
        )
        fig.update_layout(xaxis_title="", yaxis_title="Hours")
        st.plotly_chart(fig, use_container_width=True)
    
    # Growth analysis
    st.markdown("---")
    st.subheader("📊 Growth Analysis")
    
    growth_data = calculate_growth_metrics(monthly_total)
    
    if growth_data and "data" in growth_data:
        df_growth = growth_data["data"]
        
        fig = go.Figure()
        
        fig.add_trace(go.Bar(
            x=df_growth["YearMonth"],
            y=df_growth["MoM_Growth"],
            name="MoM Growth %",
            marker_color=np.where(df_growth["MoM_Growth"] >= 0, "#2ca02c", "#d62728"),
        ))
        
        fig.update_layout(
            title="Month-over-Month Growth Rate",
            xaxis_title="",
            yaxis_title="Growth %",
            hovermode="x",
        )
        
        st.plotly_chart(fig, use_container_width=True)
    
    # Revenue distribution
    st.markdown("---")
    st.subheader("📦 Revenue Distribution")
    
    col1, col2 = st.columns(2)
    
    with col1:
        # Box plot
        fig = px.box(
            monthly_total,
            y="Billable_Amount_in_USD",
            title="Revenue Distribution (Box Plot)",
        )
        st.plotly_chart(fig, use_container_width=True)
    
    with col2:
        # Histogram
        fig = px.histogram(
            monthly_total,
            x="Billable_Amount_in_USD",
            title="Revenue Frequency Distribution",
            nbins=20,
        )
        st.plotly_chart(fig, use_container_width=True)
    
    # Statistical summary
    st.markdown("---")
    st.subheader("📊 Statistical Summary")
    
    col1, col2, col3, col4 = st.columns(4)
    
    revenue_series = monthly_total["Billable_Amount_in_USD"]
    
    with col1:
        st.metric("Mean Revenue", f"${revenue_series.mean():,.0f}")
        st.metric("Std Deviation", f"${revenue_series.std():,.0f}")
    
    with col2:
        st.metric("Median Revenue", f"${revenue_series.median():,.0f}")
        st.metric("Min Revenue", f"${revenue_series.min():,.0f}")
    
    with col3:
        st.metric("Max Revenue", f"${revenue_series.max():,.0f}")
        st.metric("Range", f"${revenue_series.max() - revenue_series.min():,.0f}")
    
    with col4:
        q1 = revenue_series.quantile(0.25)
        q3 = revenue_series.quantile(0.75)
        st.metric("Q1 (25th percentile)", f"${q1:,.0f}")
        st.metric("Q3 (75th percentile)", f"${q3:,.0f}")


def show_billing_mix(filtered_time, monthly_long):
    """Billing Mix & Trends page."""
    st.header("💰 Billing Mix & Trends")
    
    if monthly_long.empty:
        st.warning("No data available for the selected filters.")
        return
    
    # Stacked area chart
    st.subheader("📊 Revenue Mix Over Time")
    
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
            fillmode="tonexty",
        ))
    
    fig.update_layout(
        xaxis_title="",
        yaxis_title="Revenue (USD)",
        hovermode="x unified",
        height=400,
    )
    
    st.plotly_chart(fig, use_container_width=True)
    
    # Percentage breakdown
    st.markdown("---")
    st.subheader("📈 Revenue Mix Percentage")
    
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
    )
    
    st.plotly_chart(fig, use_container_width=True)
    
    # Rate type analysis
    st.markdown("---")
    st.subheader("💼 Rate Type Analysis")
    
    rate_metrics = calculate_rate_type_metrics(filtered_time)
    
    if rate_metrics:
        col1, col2 = st.columns([1, 1])
        
        with col1:
            # Pie chart
            rate_stats = rate_metrics["rate_stats"]
            fig = px.pie(
                values=rate_stats["Total_Revenue"],
                names=rate_stats.index,
                title="Revenue Distribution by Rate Type",
            )
            st.plotly_chart(fig, use_container_width=True)
        
        with col2:
            # Bar chart
            fig = px.bar(
                rate_stats.reset_index(),
                x="Rate_Type",
                y="Total_Revenue",
                title="Total Revenue by Rate Type",
                color="Rate_Type",
            )
            fig.update_layout(xaxis_title="", yaxis_title="Revenue (USD)", showlegend=False)
            st.plotly_chart(fig, use_container_width=True)
        
        # Detailed table
        st.dataframe(rate_stats, use_container_width=True)


def show_forecasting(monthly_long):
    """Forecasting & Projections page."""
    st.header("🔮 Forecasting & Projections")
    
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
            ))
            
            # Forecast
            fig.add_trace(go.Scatter(
                x=forecast_result["forecast"].index,
                y=forecast_result["forecast"].values,
                name="Forecast",
                mode="lines+markers",
                line=dict(color="#ff7f0e", width=3, dash="dash"),
            ))
            
            # Confidence interval
            fig.add_trace(go.Scatter(
                x=forecast_result["upper"].index,
                y=forecast_result["upper"].values,
                name="Upper Bound (95%)",
                mode="lines",
                line=dict(width=0),
                showlegend=True,
            ))
            
            fig.add_trace(go.Scatter(
                x=forecast_result["lower"].index,
                y=forecast_result["lower"].values,
                name="Lower Bound (95%)",
                mode="lines",
                line=dict(width=0),
                fillcolor="rgba(255, 127, 14, 0.2)",
                fill="tonexty",
                showlegend=True,
            ))
            
            fig.update_layout(
                xaxis_title="",
                yaxis_title="Revenue (USD)",
                hovermode="x unified",
                height=500,
            )
            
            st.plotly_chart(fig, use_container_width=True)
            
            # Forecast summary
            st.markdown("---")
            st.subheader("📊 Forecast Summary")
            
            forecast_df = pd.DataFrame({
                "Month": forecast_result["forecast"].index.strftime("%b %Y"),
                "Forecasted Revenue": forecast_result["forecast"].values,
                "Lower Bound": forecast_result["lower"].values,
                "Upper Bound": forecast_result["upper"].values,
            })
            
            st.dataframe(
                forecast_df.style.format({
                    "Forecasted Revenue": "${:,.0f}",
                    "Lower Bound": "${:,.0f}",
                    "Upper Bound": "${:,.0f}",
                }),
                use_container_width=True
            )
    
    # Forecast by rate type
    st.markdown("---")
    st.subheader("💼 Forecast by Rate Type")
    
    pivot = (
        monthly_long
        .pivot(index="YearMonth", columns="Rate_Type", values="Billable_Amount_in_USD")
        .fillna(0)
        .sort_index()
    )
    
    for rate_type in pivot.columns:
        with st.expander(f"📊 {rate_type} Forecast"):
            series = pivot[rate_type]
            fc_result = advanced_forecast(series, periods=months_ahead, method=forecast_method)
            
            if not fc_result["forecast"].empty:
                fig = go.Figure()
                
                fig.add_trace(go.Scatter(
                    x=series.index,
                    y=series.values,
                    name="Actual",
                    mode="lines+markers",
                ))
                
                fig.add_trace(go.Scatter(
                    x=fc_result["forecast"].index,
                    y=fc_result["forecast"].values,
                    name="Forecast",
                    mode="lines+markers",
                    line=dict(dash="dash"),
                ))
                
                fig.update_layout(
                    xaxis_title="",
                    yaxis_title="Revenue (USD)",
                    height=300,
                )
                
                st.plotly_chart(fig, use_container_width=True)


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


if __name__ == "__main__":
    main()
