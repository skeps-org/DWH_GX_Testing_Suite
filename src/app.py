import streamlit as st
import pandas as pd
import sys
import os
import toml
import yaml

# 1. Setup Page (Instant)
st.set_page_config(page_title="GX Lender Dashboard", layout="wide")

# Add parent directory to path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

st.title("🛡️ Data Warehouse Quality Control")

# ---------------------------------------------------------
# PHASE 1: LIGHTWEIGHT CONFIG LOADING (Instant)
# ---------------------------------------------------------
@st.cache_data
def load_config_data():
    """Reads config files directly without initializing the heavy Engine."""
    try:
        # Load Lenders
        secrets = toml.load("secrets.toml")
        lenders_list = list(secrets['lenders'].keys())
        
        # Load Tables
        with open("config/gx_rules.yaml", 'r') as f:
            rules = yaml.safe_load(f)
        tables_list = list(rules['tables'].keys())
        
        return lenders_list, tables_list
    except Exception as e:
        return [], []

lenders, available_tables = load_config_data()

if not lenders:
    st.error("Could not load configurations. Check secrets.toml and gx_rules.yaml.")
    st.stop()

# ---------------------------------------------------------
# PHASE 2: UI RENDERING (Instant)
# ---------------------------------------------------------
st.sidebar.header("Configuration")
selected_lender = st.sidebar.selectbox("Select Lender", ["ALL"] + lenders)
target_table_selection = st.sidebar.selectbox("Target Table", ["ALL TABLES"] + available_tables)
run_btn = st.sidebar.button("Run Diagnostics", type="primary")

# ---------------------------------------------------------
# PHASE 3: HEAVY LIFTING (Lazy Loaded)
# ---------------------------------------------------------
@st.cache_resource
def get_runner():
    """Only imports GX when absolutely necessary."""
    from gx_wrapper import GXRunner 
    return GXRunner(secrets_path="secrets.toml", rules_path="config/gx_rules.yaml")

if run_btn:
    # 1. Initialize Engine (First run only)
    with st.spinner("Initializing Engine..."):
        try:
            runner = get_runner()
        except Exception as e:
            st.error(f"Failed to start engine: {e}")
            st.stop()

    table_arg = None if target_table_selection == "ALL TABLES" else target_table_selection
    
    st.write(f"### ⏳ Running Validation: {target_table_selection}")
    
    final_df = pd.DataFrame()

    # 2. EXECUTION LOGIC (Sequential)
    if selected_lender == "ALL":
        all_dfs = []
        progress_bar = st.progress(0)
        
        for i, lender in enumerate(lenders):
            with st.spinner(f"Analyzing {lender} ({i+1}/{len(lenders)})..."):
                df = runner.run_validation(lender, specific_table=table_arg)
                all_dfs.append(df)
            progress_bar.progress((i + 1) / len(lenders))
        
        if all_dfs:
            final_df = pd.concat(all_dfs, ignore_index=True)
            
    else:
        # Single Lender Case
        with st.spinner(f"Validating {selected_lender}..."):
            final_df = runner.run_validation(selected_lender, specific_table=table_arg)

    # 3. DISPLAY RESULTS
    if not final_df.empty:
        # Reorder columns: Status first, then Names, then Numbers
        cols = ['status', 'lender', 'table', 'test_name', 'failed_rows', 'total_rows', 'severity']
        existing_cols = [c for c in cols if c in final_df.columns]
        final_df = final_df[existing_cols]

        def color_status(val):
            color = 'red' if val != 'PASS' else 'green'
            return f'color: {color}; font-weight: bold'

        # Use column_config to force Number format (remove decimals)
        st.dataframe(
            final_df.style.applymap(color_status, subset=['status']), 
            use_container_width=True,
            column_config={
                "failed_rows": st.column_config.NumberColumn(format="%d"),
                "total_rows": st.column_config.NumberColumn(format="%d")
            }
        )
        
        fails = final_df[final_df['status'] != 'PASS']
        if not fails.empty:
            st.error(f"⚠️ Found {len(fails)} failures.")
        else:
            st.success("✅ All systems green.")
    else:
        st.warning("No results returned.")