import streamlit as st
import pandas as pd
from datetime import datetime, timedelta


def apply_time_entry_filters(df: pd.DataFrame) -> pd.DataFrame:
    """
    Apply comprehensive filters to time entry data via sidebar.
    Returns filtered dataframe.
    """
    if df.empty:
        return df
    
    st.sidebar.markdown("## 🔍 Filters")
    
    filtered_df = df.copy()
    
    # Date range filter
    st.sidebar.markdown("### 📅 Date Range")
    
    if "Date_of_Work" in filtered_df.columns:
        filtered_df["Date_of_Work"] = pd.to_datetime(filtered_df["Date_of_Work"], errors="coerce")
        
        min_date = filtered_df["Date_of_Work"].min()
        max_date = filtered_df["Date_of_Work"].max()
        
        if pd.notna(min_date) and pd.notna(max_date):
            # Quick date presets
            date_preset = st.sidebar.selectbox(
                "Date Preset",
                [
                    "All Time",
                    "Last 30 Days",
                    "Last 90 Days",
                    "Last 6 Months",
                    "Last 12 Months",
                    "Year to Date",
                    "This Quarter",
                    "Last Quarter",
                    "Custom Range",
                ]
            )
            
            today = datetime.now()
            
            if date_preset == "Last 30 Days":
                start_date = today - timedelta(days=30)
                end_date = today
            elif date_preset == "Last 90 Days":
                start_date = today - timedelta(days=90)
                end_date = today
            elif date_preset == "Last 6 Months":
                start_date = today - timedelta(days=180)
                end_date = today
            elif date_preset == "Last 12 Months":
                start_date = today - timedelta(days=365)
                end_date = today
            elif date_preset == "Year to Date":
                start_date = datetime(today.year, 1, 1)
                end_date = today
            elif date_preset == "This Quarter":
                quarter = (today.month - 1) // 3 + 1
                start_date = datetime(today.year, 3 * quarter - 2, 1)
                end_date = today
            elif date_preset == "Last Quarter":
                quarter = (today.month - 1) // 3
                if quarter == 0:
                    quarter = 4
                    year = today.year - 1
                else:
                    year = today.year
                start_date = datetime(year, 3 * quarter - 2, 1)
                if quarter == 4:
                    end_date = datetime(year, 12, 31)
                else:
                    end_date = datetime(year, 3 * quarter + 1, 1) - timedelta(days=1)
            elif date_preset == "Custom Range":
                col1, col2 = st.sidebar.columns(2)
                with col1:
                    start_date = st.date_input(
                        "Start Date",
                        value=min_date.date(),
                        min_value=min_date.date(),
                        max_value=max_date.date(),
                    )
                with col2:
                    end_date = st.date_input(
                        "End Date",
                        value=max_date.date(),
                        min_value=min_date.date(),
                        max_value=max_date.date(),
                    )
                start_date = pd.to_datetime(start_date)
                end_date = pd.to_datetime(end_date)
            else:  # All Time
                start_date = min_date
                end_date = max_date
            
            # Apply date filter
            filtered_df = filtered_df[
                (filtered_df["Date_of_Work"] >= pd.to_datetime(start_date)) &
                (filtered_df["Date_of_Work"] <= pd.to_datetime(end_date))
            ]
    
    # Timekeeper filter
    st.sidebar.markdown("### 👤 Timekeeper")
    if "Timekeeper" in filtered_df.columns:
        unique_timekeepers = sorted(filtered_df["Timekeeper"].dropna().unique())
        if len(unique_timekeepers) > 0:
            selected_timekeepers = st.sidebar.multiselect(
                "Select Timekeepers",
                options=["All"] + unique_timekeepers,
                default=["All"],
            )
            
            if "All" not in selected_timekeepers:
                filtered_df = filtered_df[filtered_df["Timekeeper"].isin(selected_timekeepers)]
    
    # Client filter
    st.sidebar.markdown("### 🏢 Client")
    if "Client_Name" in filtered_df.columns:
        unique_clients = sorted(filtered_df["Client_Name"].dropna().unique())
        if len(unique_clients) > 0:
            # Search box for clients
            client_search = st.sidebar.text_input("Search Clients", "")
            
            if client_search:
                filtered_clients = [c for c in unique_clients if client_search.lower() in c.lower()]
            else:
                filtered_clients = unique_clients[:50]  # Show top 50 by default
            
            selected_clients = st.sidebar.multiselect(
                "Select Clients",
                options=["All"] + filtered_clients,
                default=["All"],
            )
            
            if "All" not in selected_clients:
                filtered_df = filtered_df[filtered_df["Client_Name"].isin(selected_clients)]
    
    # Practice group filter
    st.sidebar.markdown("### 📚 Practice Group")
    if "Primary Practice Group" in filtered_df.columns:
        unique_groups = sorted(filtered_df["Primary Practice Group"].dropna().unique())
        if len(unique_groups) > 0:
            selected_groups = st.sidebar.multiselect(
                "Select Practice Groups",
                options=["All"] + unique_groups,
                default=["All"],
            )
            
            if "All" not in selected_groups:
                filtered_df = filtered_df[filtered_df["Primary Practice Group"].isin(selected_groups)]
    
    # Rate type filter
    st.sidebar.markdown("### 💰 Rate Type")
    if "Rate_Type" in filtered_df.columns:
        unique_rates = sorted(filtered_df["Rate_Type"].dropna().unique())
        if len(unique_rates) > 0:
            selected_rates = st.sidebar.multiselect(
                "Select Rate Types",
                options=["All"] + unique_rates,
                default=["All"],
            )
            
            if "All" not in selected_rates:
                filtered_df = filtered_df[filtered_df["Rate_Type"].isin(selected_rates)]
    
    # Revenue range filter
    st.sidebar.markdown("### 💵 Revenue Range")
    if "Billable_Amount_in_USD" in filtered_df.columns:
        min_revenue = float(filtered_df["Billable_Amount_in_USD"].min())
        max_revenue = float(filtered_df["Billable_Amount_in_USD"].max())
        
        if min_revenue < max_revenue:
            revenue_range = st.sidebar.slider(
                "Revenue Range (USD)",
                min_value=min_revenue,
                max_value=max_revenue,
                value=(min_revenue, max_revenue),
                format="$%d",
            )
            
            filtered_df = filtered_df[
                (filtered_df["Billable_Amount_in_USD"] >= revenue_range[0]) &
                (filtered_df["Billable_Amount_in_USD"] <= revenue_range[1])
            ]
    
    # Hours range filter
    st.sidebar.markdown("### ⏱️ Hours Range")
    if "Billable_Hours" in filtered_df.columns:
        min_hours = float(filtered_df["Billable_Hours"].min())
        max_hours = float(filtered_df["Billable_Hours"].max())
        
        if min_hours < max_hours:
            hours_range = st.sidebar.slider(
                "Hours Range",
                min_value=min_hours,
                max_value=max_hours,
                value=(min_hours, max_hours),
                format="%.1f",
            )
            
            filtered_df = filtered_df[
                (filtered_df["Billable_Hours"] >= hours_range[0]) &
                (filtered_df["Billable_Hours"] <= hours_range[1])
            ]
    
    # Filter summary
    st.sidebar.markdown("---")
    st.sidebar.markdown("### 📊 Filter Summary")
    total_records = len(df)
    filtered_records = len(filtered_df)
    pct_filtered = (filtered_records / total_records * 100) if total_records > 0 else 0
    
    st.sidebar.metric("Records Shown", f"{filtered_records:,}", delta=f"{pct_filtered:.1f}% of total")
    
    if filtered_records < total_records:
        st.sidebar.info(f"Filtered out {total_records - filtered_records:,} records")
    
    # Reset filters button
    if st.sidebar.button("🔄 Reset All Filters"):
        st.rerun()
    
    return filtered_df
