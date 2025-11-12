
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
