import streamlit as st
import pandas as pd
from datetime import datetime, timedelta


def apply_time_entry_filters(df: pd.DataFrame) -> pd.DataFrame:
    """
    ✅ PRODUCTION FILTERS - Optimized for attorney billing data
    
    DEFAULT: Last Quarter
    Clean, fast, professional
    """
    if df.empty:
        return df
    
    st.sidebar.markdown("## 🔍 Filters")
    
    filtered_df = df.copy()
    
    # ========================================
    # DATE RANGE FILTER - DEFAULT: LAST QUARTER
    # ========================================
    
    st.sidebar.markdown("### 📅 Date Range")
    
    if "Date_of_Work" in filtered_df.columns:
        filtered_df["Date_of_Work"] = pd.to_datetime(filtered_df["Date_of_Work"], errors="coerce")
        
        min_date = filtered_df["Date_of_Work"].min()
        max_date = filtered_df["Date_of_Work"].max()
        
        if pd.notna(min_date) and pd.notna(max_date):
            # Date presets - Last Quarter is default
            date_preset = st.sidebar.selectbox(
                "Date Preset",
                [
                    "Last Quarter",      # ✅ DEFAULT
                    "This Month",
                    "Last Month",
                    "Last 3 Months",
                    "Last 6 Months",
                    "Year to Date",
                    "Fiscal Year",       # Oct-Sep
                    "Custom Range",
                    "All Time",
                ],
                index=0,
            )
            
            today = datetime.now()
            
            # Calculate date ranges
            if date_preset == "This Month":
                start_date = datetime(today.year, today.month, 1)
                end_date = today
                
            elif date_preset == "Last Month":
                first_of_this_month = datetime(today.year, today.month, 1)
                last_day_of_last_month = first_of_this_month - timedelta(days=1)
                start_date = datetime(last_day_of_last_month.year, last_day_of_last_month.month, 1)
                end_date = last_day_of_last_month
                
            elif date_preset == "Last 3 Months":
                start_date = today - timedelta(days=90)
                end_date = today
                
            elif date_preset == "Last 6 Months":
                start_date = today - timedelta(days=180)
                end_date = today
                
            elif date_preset == "Year to Date":
                start_date = datetime(today.year, 1, 1)
                end_date = today
                
            elif date_preset == "Last Quarter":
                # Most recent complete quarter
                current_quarter = (today.month - 1) // 3 + 1
                
                if current_quarter == 1:
                    # Q4 of previous year
                    start_date = datetime(today.year - 1, 10, 1)
                    end_date = datetime(today.year - 1, 12, 31)
                else:
                    # Previous quarter this year
                    last_quarter = current_quarter - 1
                    start_month = 3 * last_quarter - 2
                    start_date = datetime(today.year, start_month, 1)
                    
                    if last_quarter == 4:
                        end_date = datetime(today.year, 12, 31)
                    else:
                        next_quarter_start = datetime(today.year, 3 * last_quarter + 1, 1)
                        end_date = next_quarter_start - timedelta(days=1)
                        
            elif date_preset == "Fiscal Year":
                # Law firm fiscal year: Oct 1 - Sep 30
                if today.month >= 10:
                    start_date = datetime(today.year, 10, 1)
                    end_date = today
                else:
                    start_date = datetime(today.year - 1, 10, 1)
                    end_date = today
                    
            elif date_preset == "Custom Range":
                col1, col2 = st.sidebar.columns(2)
                with col1:
                    start_date = st.date_input(
                        "Start",
                        value=min_date.date(),
                        min_value=min_date.date(),
                        max_value=max_date.date(),
                    )
                with col2:
                    end_date = st.date_input(
                        "End",
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
            
            # Show date range (except for All Time)
            if date_preset != "All Time":
                st.sidebar.info(
                    f"📅 **{start_date.strftime('%B %Y')}** to **{end_date.strftime('%B %Y')}**"
                )
    
    # ========================================
    # TIMEKEEPER FILTER
    # ========================================
    
    st.sidebar.markdown("### 👤 Timekeeper")
    if "Timekeeper" in filtered_df.columns:
        unique_timekeepers = sorted(filtered_df["Timekeeper"].dropna().unique())
        if len(unique_timekeepers) > 0:
            selected_timekeepers = st.sidebar.multiselect(
                "Select Attorneys",
                options=["All"] + unique_timekeepers,
                default=["All"],
            )
            
            if "All" not in selected_timekeepers:
                filtered_df = filtered_df[filtered_df["Timekeeper"].isin(selected_timekeepers)]
    
    # ========================================
    # CLIENT FILTER
    # ========================================
    
    st.sidebar.markdown("### 🏢 Client")
    if "Client_Name" in filtered_df.columns:
        unique_clients = sorted(filtered_df["Client_Name"].dropna().unique())
        if len(unique_clients) > 0:
            # Search box
            client_search = st.sidebar.text_input("Search Clients", "")
            
            if client_search:
                filtered_clients = [c for c in unique_clients if client_search.lower() in c.lower()]
            else:
                filtered_clients = unique_clients[:50]
            
            selected_clients = st.sidebar.multiselect(
                "Select Clients",
                options=["All"] + filtered_clients,
                default=["All"],
            )
            
            if "All" not in selected_clients:
                filtered_df = filtered_df[filtered_df["Client_Name"].isin(selected_clients)]
    
    # ========================================
    # PRACTICE GROUP FILTER
    # ========================================
    
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
    
    # ========================================
    # RATE TYPE FILTER
    # ========================================
    
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
    
    # ========================================
    # FILTER SUMMARY
    # ========================================
    
    st.sidebar.markdown("---")
    st.sidebar.markdown("### 📊 Data Summary")
    
    total_records = len(df)
    filtered_records = len(filtered_df)
    pct_filtered = (filtered_records / total_records * 100) if total_records > 0 else 0
    
    col1, col2 = st.sidebar.columns(2)
    with col1:
        st.metric("Records", f"{filtered_records:,}")
    with col2:
        st.metric("% of Total", f"{pct_filtered:.0f}%")
    
    if filtered_records < total_records:
        st.sidebar.caption(f"📌 Filtered out {total_records - filtered_records:,} records")
    
    # ========================================
    # RESET BUTTON
    # ========================================
    
    if st.sidebar.button("🔄 Reset All Filters", use_container_width=True):
        st.rerun()
    
    return filtered_df
