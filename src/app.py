import streamlit as st
import pandas as pd
import sys
import os
import toml
import yaml

# 1. Setup Page
st.set_page_config(page_title="GX Lender Dashboard", layout="wide")
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
st.title("🛡️ Data Warehouse Quality Control")

# ---------------------------------------------------------
# PHASE 1: CONFIG LOADING
# ---------------------------------------------------------
@st.cache_data
def load_config_data():
    try:
        secrets = toml.load("secrets.toml")
        lenders_list = list(secrets['lenders'].keys())
        with open("config/gx_rules.yaml", 'r') as f:
            rules = yaml.safe_load(f)
        tables_list = list(rules['tables'].keys())
        return lenders_list, tables_list
    except Exception as e:
        return [], []

lenders, available_tables = load_config_data()

if not lenders:
    st.error("Config Error: Check secrets.toml and gx_rules.yaml")
    st.stop()

# ---------------------------------------------------------
# PHASE 2: UI RENDERING
# ---------------------------------------------------------
st.sidebar.header("Configuration")
selected_lender = st.sidebar.selectbox("Select Lender", ["ALL"] + lenders)
target_table_selection = st.sidebar.selectbox("Target Table", ["ALL TABLES"] + available_tables)
run_btn = st.sidebar.button("Run Diagnostics", type="primary")

# ---------------------------------------------------------
# PHASE 3: EXECUTION
# ---------------------------------------------------------
@st.cache_resource
def get_runner():
    from gx_wrapper import GXRunner 
    return GXRunner(secrets_path="secrets.toml", rules_path="config/gx_rules.yaml")

if run_btn:
    with st.spinner("Initializing Engine..."):
        try:
            runner = get_runner()
        except Exception as e:
            st.error(f"Failed to start engine: {e}")
            st.stop()

    table_arg = None if target_table_selection == "ALL TABLES" else target_table_selection
    st.write(f"### ⏳ Running Validation: {target_table_selection}")
    
    final_df = pd.DataFrame()

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
        with st.spinner(f"Validating {selected_lender}..."):
            final_df = runner.run_validation(selected_lender, specific_table=table_arg)

    # 3. DISPLAY RESULTS
    if not final_df.empty:
        cols = ['status', 'lender', 'table', 'test_name', 'failed_rows', 'total_rows', 'severity', 'error_msg']
        existing_cols = [c for c in cols if c in final_df.columns]
        final_df = final_df[existing_cols]

        def color_status(val):
            if val == 'PASS': color = 'green'
            elif val == 'ERROR': color = '#ff9900'
            else: color = 'red'
            return f'color: {color}; font-weight: bold'

        st.dataframe(
            final_df.style.applymap(color_status, subset=['status']), 
            use_container_width=True,
            column_config={
                "failed_rows": st.column_config.NumberColumn(format="%d"),
                "total_rows": st.column_config.NumberColumn(format="%d", help="Total rows in the table"),
                "error_msg": st.column_config.TextColumn("Details / Error", width="large", help="Hover to see full text or check details below.")
            }
        )
        
        # --- ROBUST FILTER: CATCH ANYTHING THAT IS NOT PASS ---
        fails_or_errors = final_df[final_df['status'] != 'PASS']
        
        if not fails_or_errors.empty:
            st.error(f"⚠️ Found {len(fails_or_errors)} issues.")
            
            st.write("---")
            st.subheader("🔍 Failure Details")
            
            for index, row in fails_or_errors.iterrows():
                with st.expander(f"{row['status']}: {row['test_name']} ({row['table']})", expanded=True):
                    if row['status'] == 'ERROR':
                        st.code(row['error_msg'], language="sql")
                    else:
                        st.info(f"Failed Rows: {row['failed_rows']} / {row['total_rows']}")
        else:
            st.success("✅ All systems green.")
    else:
        st.warning("No results returned.")