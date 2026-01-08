import streamlit as st
import pandas as pd
from gx_wrapper import GXRunner
import sys
import os

# Add parent directory to path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

st.set_page_config(page_title="GX Lender Dashboard", layout="wide")
st.title("🛡️ Data Warehouse Quality Control")

try:
    runner = GXRunner(secrets_path="secrets.toml", rules_path="config/gx_rules.yaml")
    lenders = list(runner.secrets.keys())
    # UPDATE: Get table names from YAML config
    available_tables = list(runner.rules['tables'].keys())
except Exception as e:
    st.error(f"Configuration Error: {e}")
    st.stop()

# Sidebar
st.sidebar.header("Configuration")
selected_lender = st.sidebar.selectbox("Select Lender", ["ALL"] + lenders)

# UPDATE: Dropdown for tables instead of text input
# We add "ALL TABLES" option for manual full scans
target_table_selection = st.sidebar.selectbox("Target Table", ["ALL TABLES"] + available_tables)

run_btn = st.sidebar.button("Run Diagnostics", type="primary")

if run_btn:
    # Determine the argument to pass
    # If "ALL TABLES" is selected, we pass None to the function
    table_arg = None if target_table_selection == "ALL TABLES" else target_table_selection
    
    st.write(f"### ⏳ Running Validation: {target_table_selection}")
    
    if selected_lender == "ALL":
        all_dfs = []
        progress = st.progress(0)
        for i, lender in enumerate(lenders):
            with st.spinner(f"Validating {lender}..."):
                df = runner.run_validation(lender, specific_table=table_arg)
                all_dfs.append(df)
                progress.progress((i + 1) / len(lenders))
        final_df = pd.concat(all_dfs, ignore_index=True) if all_dfs else pd.DataFrame()
    else:
        with st.spinner(f"Validating {selected_lender}..."):
            final_df = runner.run_validation(selected_lender, specific_table=table_arg)
            
    if not final_df.empty:
        # Move 'table' column to the front for better readability
        cols = ['lender', 'table', 'test_name', 'failed_rows', 'status', 'severity']
        # Handle cases where error might prevent 'table' col from existing
        existing_cols = [c for c in cols if c in final_df.columns]
        final_df = final_df[existing_cols]

        def color_status(val):
            color = 'red' if val != 'PASS' else 'green'
            return f'color: {color}; font-weight: bold'

        st.dataframe(final_df.style.applymap(color_status, subset=['status']), use_container_width=True)
        
        fails = final_df[final_df['status'] != 'PASS']
        if not fails.empty:
            st.error(f"⚠️ Found {len(fails)} failures.")
        else:
            st.success("✅ All systems green.")
    else:
        st.warning("No results returned.")