from src.gx_wrapper import GXRunner
from src.notifier import send_summary_email
import concurrent.futures
import pandas as pd
import logging
import logging.config
from datetime import datetime

logging.config.fileConfig('config/logging.conf')
logger = logging.getLogger('dq_engine')

def run_wrapper(lender):
    # UPDATE: No longer passing specific table_name. 
    # This tells the runner to look at 'tables' in YAML and run ALL of them.
    # Instantiate locally to avoid PicklingError with ProcessPoolExecutor
    runner = GXRunner()
    return runner.run_validation(lender)

def main():
    logger.info("=== Starting GX Daily Check (Multi-Table) ===")
    
    try:
        # We still need a temporary runner just to get the list of lenders
        temp_runner = GXRunner()
        lenders = list(temp_runner.secrets.keys())
    except Exception as e:
        logger.critical(f"Config Error: {e}")
        return

    all_results = []
    # 5 Workers is safe for GX memory usage
    # UPDATE: Switched to ProcessPoolExecutor because GX Context is not thread-safe.
    # This fixes "Could not find datasource" errors by giving each job its own memory space.
    with concurrent.futures.ProcessPoolExecutor(max_workers=5) as executor:
        future_to_lender = {executor.submit(run_wrapper, l): l for l in lenders}
        
        for future in concurrent.futures.as_completed(future_to_lender):
            lender = future_to_lender[future]
            try:
                df = future.result()
                all_results.append(df)
                logger.info(f"Completed {lender}")
            except Exception as exc:
                logger.error(f"{lender} failed: {exc}")

    if all_results:
        final_df = pd.concat(all_results, ignore_index=True)
        
        # Generate HTML Summary Report
        cols = ['status', 'lender', 'table', 'test_name', 'failed_rows', 'total_rows', 'severity', 'error_msg']
        existing_cols = [c for c in cols if c in final_df.columns]
        summary_df = final_df[existing_cols]

        def color_status(val):
            if val == 'PASS': color = 'green'
            elif val == 'ERROR': color = '#ff9900'
            else: color = 'red'
            return f'color: {color}; font-weight: bold'

        html_table = summary_df.style.map(color_status, subset=['status']).to_html(index=False)
        
        # Streamlit-like CSS
        streamlit_style = """
        <style>
            body { font-family: "Source Sans Pro", sans-serif; padding: 20px; color: #31333F; }
            h2 { color: #31333F; font-weight: 600; }
            table { 
                border-collapse: collapse; 
                width: 100%; 
                margin-top: 10px;
                font-size: 14px;
                border-radius: 5px;
                overflow: hidden;
                box-shadow: 0 2px 4px rgba(0,0,0,0.1);
            }
            th { 
                background-color: #f0f2f6; 
                color: #31333F; 
                font-weight: 600; 
                text-align: left; 
                padding: 12px 16px; 
                border-bottom: 1px solid #e6e9ef;
            }
            td { 
                padding: 10px 16px; 
                border-bottom: 1px solid #e6e9ef; 
            }
            tr:last-child td { border-bottom: none; }
            tr:hover { background-color: #f8f9fa; }
        </style>
        """
        
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        report_filename = f'summary_report_{timestamp}.html'
        excel_filename = f'summary_report_{timestamp}.xlsx'
        
        html_output = f"<!DOCTYPE html>\n<html>\n<head>\n<meta charset='utf-8'>\n<title>GX Summary Report - {timestamp}</title>\n{streamlit_style}\n</head>\n<body>\n<h2>🛡️ Data Warehouse Quality Control - Summary</h2>\n{html_table}\n</body>\n</html>"

        with open(report_filename, 'w', encoding='utf-8') as f:
            f.write(html_output)
            
        logger.info(f"Saved HTML summary report to '{report_filename}'.")
        
        summary_df.to_excel(excel_filename, index=False)
        logger.info(f"Saved Excel summary report to '{excel_filename}'.")

        # Filter failures
        failures = final_df[final_df['status'] != 'PASS']
        
        if not failures.empty:
            logger.warning(f"Detected {len(failures)} failures. Check 'failed_rows/' directory for CSV reports.")
        else:
            logger.info("All GX checks passed across all tables.")
            
        # Send the HTML Summary via email
        send_summary_email(html_output, len(failures), attachment_path=excel_filename)

    else:
        logger.warning("No results generated.")

if __name__ == "__main__":
    main()