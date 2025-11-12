# main.py

import os
import numpy as np
import pandas as pd
import streamlit as st
import plotly.express as px

from filters import apply_time_entry_filters

# ----------------------------
# CONFIG: where the Excel files live
# ----------------------------
DATA_DIR = "10.31.25 (AI)"   # folder in your GitHub repo

# Filenames INSIDE that folder (change if your names differ)
TIME_ENTRY_FILES = [
    "Time Entry Prep File (10.31).xlsx",
    "Time Entry Prep File (10.31) - FY25.xlsx",
]
INVOICE_FILE = "Invoice Prep File (10.31).xlsx"
PAYMENT_FILE = "Payment Prep File (10.31).xlsx"

# ----------------------------
# Basic password protection
# ----------------------------

PASSWORD = "TrendsAI2025"


def check_password() -> bool:
    """Simple password gate using session_state."""
    def password_entered():
        if st.session_state.get("password") == PASSWORD:
            st.session_state["password_correct"] = True
            del st.session_state["password"]  # don't store the raw password
        else:
            st.session_state["password_correct"] = False

    if "password_correct" not in st.session_state:
        st.text_input(
            "Enter password",
            type="password",
            on_change=password_entered,
            key="password",
        )
        return False
    elif not st.session_state["password_correct"]:
        st.text_input(
            "Enter password",
            type="password",
            on_change=password_entered,
            key="password",
        )
        st.error("❌ Incorrect password.")
        return False
    else:
        return True


# ----------------------------
# Data loading & cleaning
# ----------------------------

@st.cache_data(show_spinner=True)
def load_time_entries() -> pd.DataFrame:
    """
    Load and clean the Time Entry prep files from DATA_DIR.
    """
    frames = []
    for filename in TIME_ENTRY_FILES:
        path = os.path.join(DATA_DIR, filename)
        try:
            df_raw = pd.read_excel(path)
        except FileNotFoundError:
            continue

        if (
            "ELIMINATED BILLING ORIGINATORS AND ALL Non-Billable Hours"
            in df_raw.columns
        ):
            # Header row lives in the first row of data
            header_row = df_raw.iloc[0]
            df = df_raw[1:].copy()
            df.columns = header_row
        else:
            df = df_raw.copy()

        frames.append(df)

    if not frames:
        return pd.DataFrame()

    df = pd.concat(frames, ignore_index=True)

    # Standardize important columns
    for col in ["Date_of_Work", "Time_Creation_Date", "Invoice Date", "Period of Invoice"]:
        if col in df.columns:
            df[col] = pd.to_datetime(df[col], errors="coerce")

    # Numeric conversions
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


@st.cache_data(show_spinner=True)
def load_invoice_prep() -> pd.DataFrame:
    """
    Load Invoice Prep file from DATA_DIR.
    """
    path = os.path.join(DATA_DIR, INVOICE_FILE)
    try:
        df = pd.read_excel(path)
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


@st.cache_data(show_spinner=True)
def load_payment_prep() -> pd.DataFrame:
    """
    Load and clean Payment Prep file from DATA_DIR.
    """
    path = os.path.join(DATA_DIR, PAYMENT_FILE)
    try:
        df_raw = pd.read_excel(path)
    except FileNotFoundError:
        return pd.DataFrame()

    # Header row seems to be row index 1 for this file
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
# Helper analytics functions
# ----------------------------

def prepare_monthly_time_by_rate(df: pd.DataFrame) -> pd.DataFrame:
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
        df.groupby(group_cols)["Billable_Amount_in_USD"]
        .sum()
        .reset_index()
        .sort_values("YearMonth")
    )
    return monthly


def simple_linear_forecast(series: pd.Series, periods: int = 3) -> pd.Series:
    series = series.dropna()
    if len(series) < 2:
        return pd.Series(dtype=float)

    x = np.arange(len(series))
    y = series.values
    slope, intercept = np.polyfit(x, y, 1)

    future_x = np.arange(len(series), len(series) + periods)
    future_y = intercept + slope * future_x
    future_y = np.where(future_y < 0, 0, future_y)

    future_index = pd.period_range(
        start=(series.index[-1] + 1),
        periods=periods,
        freq="M",
    ).to_timestamp()

    return pd.Series(future_y, index=future_index)


def generate_trend_summary(monthly_long: pd.DataFrame) -> str:
    if monthly_long.empty:
        return "No data available for trend analysis."

    pivot = (
        monthly_long
        .pivot(index="YearMonth", columns="Rate_Type", values="Billable_Amount_in_USD")
        .fillna(0)
        .sort_index()
    )

    total = pivot.sum(axis=1)
    if total.empty:
        return "No data available for trend analysis."

    lines = []

    # 1. Total revenue change last month vs prior month
    if len(total) >= 2:
        last_month, prev_month = total.iloc[-1], total.iloc[-2]
        lm_label = total.index[-1].strftime("%b %Y")
        pm_label = total.index[-2].strftime("%b %Y")
        if prev_month != 0:
            pct_change = (last_month - prev_month) / prev_month * 100
            direction = "up" if pct_change > 0 else "down"
            lines.append(
                f"• Total billed revenue in {lm_label} was {direction} about {abs(pct_change):.1f}% vs {pm_label}."
            )

    flat_col = None
    hourly_col = None
    for col in pivot.columns:
        col_lower = str(col).lower()
        if any(k in col_lower for k in ["flat", "fixed"]):
            flat_col = col
        if "hour" in col_lower:
            hourly_col = col

    if flat_col is not None:
        flat_share = pivot[flat_col] / total
        if len(flat_share) >= 2:
            early_window = flat_share.iloc[: max(1, len(flat_share) // 3)].mean()
            recent_window = flat_share.iloc[-max(1, len(flat_share) // 3):].mean()
            delta = recent_window - early_window

            if delta > 0.03:
                lines.append(
                    f"• Flat / alternative fee work has grown by about {delta*100:.1f} percentage points "
                    f"in share of revenue vs the earlier period."
                )
            elif delta < -0.03:
                lines.append(
                    f"• Flat / alternative fee work has decreased by about {abs(delta)*100:.1f} percentage points "
                    f"in share of revenue vs the earlier period."
                )
            else:
                lines.append(
                    "• The mix between flat/alternative fees and other billing methods has been relatively stable."
                )

        if len(flat_share) >= 2:
            last_flat = pivot[flat_col].iloc[-1]
            prev_flat = pivot[flat_col].iloc[-2]
            lm_label = pivot.index[-1].strftime("%b %Y")
            pm_label = pivot.index[-2].strftime("%b %Y")
            mid_val = np.mean([last_flat, prev_flat])
            if mid_val > 0:
                diff_pct = abs(last_flat - prev_flat) / mid_val
                if diff_pct < 0.1:
                    lines.append(
                        f"• {lm_label} and {pm_label} had very similar flat-fee revenue "
                        f"(around ${mid_val/1_000_000:.2f}M combined), suggesting a consistent trend."
                    )

    if hourly_col is not None and flat_col is not None:
        average_flat_share = (pivot[flat_col] / total).mean() * 100
        lines.append(
            f"• On average over the selected period, flat/alternative fees accounted for "
            f"about {average_flat_share:.1f}% of billed revenue."
        )

    if not lines:
        return "No clear shifts in billing patterns were detected in the selected period."
    return "\n".join(lines)


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
    st.caption("Forecasting, KPI tracking, and billing-behavior insights.")

    # Load data
    time_df = load_time_entries()
    invoice_df = load_invoice_prep()
    payment_df = load_payment_prep()

    if time_df.empty:
        st.error(
            "Could not load Time Entry prep files. "
            "Check that they exist inside the folder '10.31.25 (AI)'."
        )
        st.stop()

    st.sidebar.markdown("---")
    page = st.sidebar.radio(
        "View",
        ["KPI Overview", "Billing Mix & Trends", "Forecasting", "Attorney/Client Drilldown"],
    )

    filtered_time = apply_time_entry_filters(time_df)
    monthly_long = prepare_monthly_time_by_rate(filtered_time)

    if "Rate_Type" in filtered_time.columns:
        flat_mask = filtered_time["Rate_Type"].str.contains(
            "flat|fixed", case=False, na=False
        )
    else:
        flat_mask = pd.Series(False, index=filtered_time.index)

    flat_amount = filtered_time.loc[flat_mask, "Billable_Amount_in_USD"].sum()
    total_amount = filtered_time["Billable_Amount_in_USD"].sum()
    hourly_amount = filtered_time.loc[~flat_mask, "Billable_Amount_in_USD"].sum()
    total_hours = filtered_time.get("Billable_Hours", pd.Series(dtype=float)).sum()

    if page == "KPI Overview":
        st.subheader("Key Performance Indicators")

        col1, col2, col3, col4 = st.columns(4)
        with col1:
            st.metric("Total Billed (USD)", f"${total_amount:,.0f}")
        with col2:
            st.metric(
                "Flat / Alt Fee Billed",
                f"${flat_amount:,.0f}",
                delta=f"{(flat_amount / total_amount * 100):.1f}% of total"
                if total_amount > 0
                else "n/a",
            )
        with col3:
            st.metric("Hourly Billed (USD)", f"${hourly_amount:,.0f}")
        with col4:
            st.metric("Total Billable Hours", f"{total_hours:,.1f}")

        st.markdown("---")
        st.subheader("Monthly Revenue Trend")

        if not monthly_long.empty:
            monthly_total = (
                monthly_long.groupby("YearMonth")["Billable_Amount_in_USD"]
                .sum()
                .reset_index()
            )
            fig = px.line(
                monthly_total,
                x="YearMonth",
                y="Billable_Amount_in_USD",
                title="Total Billable Amount by Month",
                markers=True,
            )
            fig.update_layout(xaxis_title="", yaxis_title="Billable Amount (USD)")
            st.plotly_chart(fig, use_container_width=True)
        else:
            st.info("No monthly data available for the selected filters.")

        st.markdown("---")
        st.subheader("High-Level Takeaways & Trends")
        summary = generate_trend_summary(monthly_long)
        st.write(summary)

    elif page == "Billing Mix & Trends":
        st.subheader("Billing Mix: Hourly vs Flat / Alternative Fees")

        if not monthly_long.empty:
            fig_mix = px.bar(
                monthly_long,
                x="YearMonth",
                y="Billable_Amount_in_USD",
                color="Rate_Type",
                title="Monthly Billed Amount by Rate Type",
                barmode="stack",
            )
            fig_mix.update_layout(
                xaxis_title="",
                yaxis_title="Billable Amount (USD)",
                legend_title="Rate Type",
            )
            st.plotly_chart(fig_mix, use_container_width=True)

            st.markdown("#### Mix Takeaways")
            flat_summary = generate_trend_summary(monthly_long)
            st.write(flat_summary)
        else:
            st.info("No billing mix data available for the selected filters.")

    elif page == "Forecasting":
        st.subheader("Revenue & Mix Forecast")

        if monthly_long.empty:
            st.info("Not enough data to generate forecasts for the selected filters.")
        else:
            months_to_forecast = st.slider(
                "Months to forecast",
                min_value=1,
                max_value=12,
                value=3,
            )

            monthly_total = (
                monthly_long.groupby("YearMonth")["Billable_Amount_in_USD"]
                .sum()
                .sort_index()
            )
            total_forecast = simple_linear_forecast(monthly_total, periods=months_to_forecast)

            if not total_forecast.empty:
                hist_df = monthly_total.reset_index()
                hist_df["Type"] = "Actual"
                fc_df = total_forecast.reset_index()
                fc_df.columns = ["YearMonth", "Billable_Amount_in_USD"]
                fc_df["Type"] = "Forecast"
                combined = pd.concat([hist_df, fc_df], ignore_index=True)

                fig_fc = px.line(
                    combined,
                    x="YearMonth",
                    y="Billable_Amount_in_USD",
                    color="Type",
                    title="Total Billed Revenue: Actuals & Forecast",
                    markers=True,
                )
                fig_fc.update_layout(
                    xaxis_title="",
                    yaxis_title="Billable Amount (USD)",
                )
                st.plotly_chart(fig_fc, use_container_width=True)

            st.markdown("#### Mix Forecast by Rate Type")
            pivot = (
                monthly_long
                .pivot(index="YearMonth", columns="Rate_Type", values="Billable_Amount_in_USD")
                .fillna(0)
                .sort_index()
            )

            for rate_type in pivot.columns:
                st.markdown(f"**{rate_type}**")
                series = pivot[rate_type]
                fc = simple_linear_forecast(series, periods=months_to_forecast)
                if fc.empty:
                    st.write("Not enough history to forecast.")
                    continue

                hist_df = series.reset_index()
                hist_df.columns = ["YearMonth", "Amount"]
                hist_df["Type"] = "Actual"

                fc_df = fc.reset_index()
                fc_df.columns = ["YearMonth", "Amount"]
                fc_df["Type"] = "Forecast"

                combined = pd.concat([hist_df, fc_df], ignore_index=True)

                fig_rt = px.line(
                    combined,
                    x="YearMonth",
                    y="Amount",
                    color="Type",
                    markers=True,
                )
                fig_rt.update_layout(
                    xaxis_title="",
                    yaxis_title="Billable Amount (USD)",
                    showlegend=True,
                )
                st.plotly_chart(fig_rt, use_container_width=True)

    elif page == "Attorney/Client Drilldown":
        st.subheader("Attorney / Client Drilldown")

        dimension = st.selectbox(
            "Group by",
            ["Timekeeper", "Client_Name", "Primary Practice Group", "Rate_Type"],
        )

        if dimension not in filtered_time.columns:
            st.warning(f"Column '{dimension}' not found in the time entry data.")
        else:
            grouped = (
                filtered_time
                .groupby(dimension)[["Billable_Amount_in_USD", "Billable_Hours"]]
                .sum()
                .reset_index()
                .sort_values("Billable_Amount_in_USD", ascending=False)
            )

            st.dataframe(grouped, use_container_width=True)

            fig_bar = px.bar(
                grouped.head(25),
                x=dimension,
                y="Billable_Amount_in_USD",
                title=f"Top {dimension} by Billed Amount (USD)",
            )
            fig_bar.update_layout(
                xaxis_title="",
                yaxis_title="Billable Amount (USD)",
            )
            st.plotly_chart(fig_bar, use_container_width=True)


if __name__ == "__main__":
    main()
