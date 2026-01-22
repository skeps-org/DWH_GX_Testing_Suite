from src.gx_wrapper import GXRunner
import concurrent.futures
import pandas as pd
import logging
import logging.config

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
        # Filter failures
        failures = final_df[final_df['status'] != 'PASS']
        
        if not failures.empty:
            logger.warning(f"Detected {len(failures)} failures. Check 'failed_rows/' directory for CSV reports.")
        else:
            logger.info("All GX checks passed across all tables.")
    else:
        logger.warning("No results generated.")

if __name__ == "__main__":
    main()