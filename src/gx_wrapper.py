import great_expectations as gx
import toml
import yaml
import pandas as pd
import logging
import logging.config
import os
import warnings
import sqlalchemy

# Ensure logs dir exists
if not os.path.exists('logs'):
    os.makedirs('logs')

logging.config.fileConfig('config/logging.conf')
logger = logging.getLogger('dq_engine')

class GXRunner:
    def __init__(self, secrets_path="secrets.toml", rules_path="config/gx_rules.yaml"):
        self.secrets = toml.load(secrets_path)['lenders']
        with open(rules_path, 'r') as f:
            self.rules = yaml.safe_load(f)

    def _build_connection_string(self, creds):
        return f"mysql+mysqlconnector://{creds['user']}:{creds['password']}@{creds['host']}:{creds.get('port', 3306)}/{creds['db']}"

    def _get_table_count(self, creds, table_name):
        try:
            conn_str = self._build_connection_string(creds)
            engine = sqlalchemy.create_engine(conn_str)
            with engine.connect() as conn:
                query = sqlalchemy.text(f"SELECT COUNT(*) FROM {table_name}")
                result = conn.execute(query).scalar()
                return int(result)
        except Exception as e:
            logger.warning(f"Could not fetch row count for {table_name}: {e}")
            return 0 
    
    def run_validation(self, lender_id, specific_table=None):
        logger.info(f"Initializing GX for {lender_id}...")
        all_results = []
        
        try:
            context = gx.get_context(mode="ephemeral")
            creds = self.secrets[lender_id]
            ds_name = f"ds_{lender_id}"
            
            conn_str = self._build_connection_string(creds)
            data_source = context.data_sources.add_sql(name=ds_name, connection_string=conn_str)
            
            if specific_table:
                if specific_table not in self.rules['tables']:
                    logger.warning(f"Table {specific_table} requested but not found in rules YAML.")
                    return pd.DataFrame()
                target_tables = [specific_table]
            else:
                target_tables = list(self.rules['tables'].keys())
            
            for table_name in target_tables:
                logger.info(f"[{lender_id}] Starting validation for table: {table_name}")
                asset_name = f"asset_{lender_id}_{table_name}"
                try:
                    data_asset = data_source.get_asset(asset_name)
                except LookupError:
                    data_asset = data_source.add_table_asset(name=asset_name, table_name=table_name)
                
                batch_def = data_asset.add_batch_definition_whole_table(f"batch_{table_name}")
                suite_name = f"suite_{lender_id}_{table_name}"
                suite = context.suites.add(gx.ExpectationSuite(name=suite_name))
                
                table_rules = self.rules['tables'][table_name]
                
                from great_expectations import expectations as gxe
                
                with warnings.catch_warnings():
                    warnings.filterwarnings("ignore", message=".*unexpected_rows_query should contain the {batch} parameter.*")
                    
                    for exp_config in table_rules:
                        meta_data = exp_config.get('meta', {})
                        meta_data['test_alias'] = exp_config.get('name') 

                        if exp_config['type'] == "unexpected_rows_expectation":
                            exp_instance = gxe.UnexpectedRowsExpectation(**exp_config['kwargs'])
                            exp_instance.meta = meta_data
                            suite.add_expectation(exp_instance)
                        else:
                            camel_name = "".join([x.capitalize() for x in exp_config['type'].split('_')])
                            if hasattr(gxe, camel_name):
                                exp_class = getattr(gxe, camel_name)
                                exp_instance = exp_class(**exp_config['kwargs'])
                                exp_instance.meta = meta_data
                                suite.add_expectation(exp_instance)
                            else:
                                logger.warning(f"Expectation {camel_name} not found.")

                    val_def = context.validation_definitions.add(
                        gx.ValidationDefinition(data=batch_def, suite=suite, name=f"val_{lender_id}_{table_name}")
                    )
                    
                    checkpoint = context.checkpoints.add(
                        gx.Checkpoint(name=f"chk_{lender_id}_{table_name}", validation_definitions=[val_def], result_format={"result_format": "COMPLETE"})
                    )
                    
                    result = checkpoint.run()

                # Pass table_name to parse_results for better logging context
                df = self._parse_results(lender_id, result, table_name, creds)
                all_results.append(df)

            if all_results:
                return pd.concat(all_results, ignore_index=True)
            return pd.DataFrame()

        except Exception as e:
            logger.error(f"GX Critical Failure for {lender_id}: {e}")
            return pd.DataFrame([{
                "lender": lender_id,
                "table": "SYSTEM",
                "status": "CRITICAL_ERROR",
                "test_name": "GX_Execution",
                "error_msg": str(e),
                "severity": "critical",
                "failed_rows": 0,
                "total_rows": 0
            }])

    def _extract_error_message(self, info_dict):
        if not isinstance(info_dict, dict):
            return None
        if info_dict.get("exception_message"):
            return info_dict.get("exception_message")
        if info_dict.get("exception_traceback"):
            return info_dict.get("exception_traceback").strip().split('\n')[-1]
        for key, value in info_dict.items():
            if isinstance(value, dict):
                found = self._extract_error_message(value)
                if found:
                    return found
        return None

    def _parse_results(self, lender_id, checkpoint_result, table_name, creds):
        parsed_rows = []
        
        run_result = list(checkpoint_result.run_results.values())[0]
        if isinstance(run_result, dict) and 'validation_result' in run_result:
            validation_result = run_result['validation_result']
        else:
            validation_result = run_result
        
        cached_table_count = None
        
        for res in validation_result.results:
            success = res.success
            unexpected_count = int(res.result.get('unexpected_count', 0))
            raw_element_count = int(res.result.get('element_count', 0))

            if raw_element_count == 0:
                if cached_table_count is None:
                    cached_table_count = self._get_table_count(creds, table_name)
                element_count = cached_table_count
            else:
                element_count = raw_element_count

            if success:
                status = "PASS"
                error_msg = ""
            elif unexpected_count > 0:
                status = "FAIL"
                error_msg = f"Found {unexpected_count} data failures"
            else:
                status = "ERROR"
                raw_msg = self._extract_error_message(res.exception_info)
                if raw_msg:
                    error_msg = str(raw_msg)[:2000]
                else:
                    error_msg = "Unknown execution error (Check logs)"
            
            exp_config = res.expectation_config
            meta = exp_config.meta or {}
            
            if 'test_alias' in meta and meta['test_alias']:
                display_name = meta['test_alias']
            else:
                if exp_config.type == "unexpected_rows_expectation":
                    display_name = "Custom SQL Check"
                else:
                    display_name = exp_config.type

            severity = meta.get('severity', 'warning')

            
            # --- NEW: Extract Integers ---
            unexpected_count = int(res.result.get('unexpected_count', 0))
            element_count = int(res.result.get('element_count', 0))
            
            # --- NEW: Detailed Logging ---
            log_msg = f"[{lender_id}] [{table_name}] Test: {display_name} | Status: {status}"
            if status in ["FAIL", "ERROR"]:
                logger.warning(f"{log_msg} - {error_msg}")
            else:
                logger.info(log_msg)
            
            parsed_rows.append({
                "lender": lender_id,
                "table": table_name,
                "test_name": display_name,
                "status": status,
                "failed_rows": unexpected_count,
                "total_rows": element_count,
                "severity": severity,
                "error_msg": error_msg
            })
            
        return pd.DataFrame(parsed_rows)