# filters.py

import streamlit as st
import pandas as pd


def apply_time_entry_filters(df: pd.DataFrame) -> pd.DataFrame:
    """
    Adds sidebar filters and returns the filtered dataframe.
    Assumes df has at least:
      - Date_of_Work
      - (optionally) Primary Practice Group, Timekeeper, Client_Name, Rate_Type
    """
    df = df.copy()
    if "Date_of_Work" not in df.columns:
        st.warning("Date_of_Work column not found in time entry data.")
        return df

    df["Date_of_Work"] = pd.to_datetime(df["Date_of_Work"], errors="coerce")
    df = df.dropna(subset=["Date_of_Work"])

    st.sidebar.header("Filters")

    # Date range filter
    min_date = df["Date_of_Work"].min().date()
    max_date = df["Date_of_Work"].max().date()
    start_date, end_date = st.sidebar.date_input(
        "Date range (Date of Work)",
        value=(min_date, max_date),
    )

    if not isinstance(start_date, pd.Timestamp):
        # Streamlit returns Python dates; convert in mask
        mask = (
            (df["Date_of_Work"].dt.date >= start_date)
            & (df["Date_of_Work"].dt.date <= end_date)
        )
    else:
        # Fallback if Streamlit returns timestamps
        mask = (df["Date_of_Work"] >= start_date) & (df["Date_of_Work"] <= end_date)

    # Primary Practice Group filter (if present)
    if "Primary Practice Group" in df.columns:
        practice_options = sorted(df["Primary Practice Group"].dropna().unique())
        selected_practices = st.sidebar.multiselect(
            "Primary Practice Group",
            practice_options,
            default=practice_options,
        )
        if selected_practices:
            mask &= df["Primary Practice Group"].isin(selected_practices)

    # Timekeeper filter (if present)
    if "Timekeeper" in df.columns:
        timekeeper_options = sorted(df["Timekeeper"].dropna().unique())
        selected_timekeepers = st.sidebar.multiselect(
            "Timekeeper",
            timekeeper_options,
        )
        if selected_timekeepers:
            mask &= df["Timekeeper"].isin(selected_timekeepers)

    # Client filter (if present)
    if "Client_Name" in df.columns:
        client_options = sorted(df["Client_Name"].dropna().unique())
        selected_clients = st.sidebar.multiselect(
            "Client",
            client_options,
        )
        if selected_clients:
            mask &= df["Client_Name"].isin(selected_clients)

    # Rate type filter (if present)
    if "Rate_Type" in df.columns:
        rate_type_options = sorted(df["Rate_Type"].dropna().unique())
        selected_rate_types = st.sidebar.multiselect(
            "Rate Type",
            rate_type_options,
            default=rate_type_options,
        )
        if selected_rate_types:
            mask &= df["Rate_Type"].isin(selected_rate_types)

    return df[mask]
