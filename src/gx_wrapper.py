import great_expectations as gx
import toml
import yaml
import pandas as pd
import logging
import logging.config
import os
from great_expectations import expectations as gxe

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

    def run_validation(self, lender_id, specific_table=None):
        """
        Runs validation.
        If specific_table is provided, runs only that table.
        If None, runs ALL tables defined in YAML.
        """
        logger.info(f"Initializing GX for {lender_id}...")
        all_results = []
        
        try:
            context = gx.get_context(mode="ephemeral")
            creds = self.secrets[lender_id]
            ds_name = f"ds_{lender_id}"
            
            # Setup Data Source
            conn_str = self._build_connection_string(creds)
            data_source = context.data_sources.add_sql(name=ds_name, connection_string=conn_str)
            
            # Decide which tables to process
            if specific_table:
                # If the UI asked for a table that isn't in YAML, we can't test it easily 
                # because we don't have rules for it.
                if specific_table not in self.rules['tables']:
                    logger.warning(f"Table {specific_table} requested but not found in rules YAML.")
                    return pd.DataFrame()
                target_tables = [specific_table]
            else:
                target_tables = list(self.rules['tables'].keys())
            
            for table_name in target_tables:
                # 1. Add Asset
                asset_name = f"asset_{lender_id}_{table_name}"
                try:
                    data_asset = data_source.get_asset(asset_name)
                except LookupError:
                    data_asset = data_source.add_table_asset(name=asset_name, table_name=table_name)
                
                batch_def = data_asset.add_batch_definition_whole_table(f"batch_{table_name}")
                
                # 2. Build Suite
                suite_name = f"suite_{lender_id}_{table_name}"
                suite = context.suites.add(gx.ExpectationSuite(name=suite_name))
                
                table_rules = self.rules['tables'][table_name]
                
                for exp_config in table_rules:
                    # Special handling for SQL Expectations
                    if exp_config['type'] == "unexpected_rows_expectation":
                        suite.add_expectation(gxe.UnexpectedRowsExpectation(**exp_config['kwargs']))
                    else:
                        # Standard handling
                        camel_name = "".join([x.capitalize() for x in exp_config['type'].split('_')])
                        if hasattr(gxe, camel_name):
                            exp_class = getattr(gxe, camel_name)
                            exp_instance = exp_class(**exp_config['kwargs'])
                            exp_instance.meta = exp_config.get('meta', {})
                            suite.add_expectation(exp_instance)
                        else:
                            logger.warning(f"Expectation {camel_name} not found.")

                # 3. Run Checkpoint
                val_def = context.validation_definitions.add(
                    gx.ValidationDefinition(data=batch_def, suite=suite, name=f"val_{lender_id}_{table_name}")
                )
                
                checkpoint = context.checkpoints.add(
                    gx.Checkpoint(name=f"chk_{lender_id}_{table_name}", validation_definitions=[val_def], result_format={"result_format": "COMPLETE"})
                )
                
                result = checkpoint.run()
                df = self._parse_results(lender_id, result)
                
                # Tag the table name so we know where the error came from
                if not df.empty:
                    df['table'] = table_name
                
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
                "failed_rows": 0
            }])

    def _parse_results(self, lender_id, checkpoint_result):
        parsed_rows = []
        run_result = list(checkpoint_result.run_results.values())[0]
        validation_result = run_result['validation_result']
        
        for res in validation_result.results:
            success = res.success
            status = "PASS" if success else "FAIL"
            
            exp_config = res.expectation_config
            severity = exp_config.meta.get('severity', 'warning')
            
            # Handling expectation types differently for display
            if exp_config.type == "unexpected_rows_expectation":
                test_name = "Custom SQL Logic"
                # For SQL expectations, kwargs might not have 'column'
                col_name = "N/A"
            else:
                test_name = exp_config.type
                col_name = exp_config.kwargs.get('column', 'table_level')

            unexpected_count = res.result.get('unexpected_count', 0)
            
            parsed_rows.append({
                "lender": lender_id,
                "test_name": f"{test_name} ({col_name})",
                "status": status,
                "failed_rows": unexpected_count,
                "severity": severity,
                "error_msg": "" if success else f"Found {unexpected_count} unexpected values"
            })
            
        return pd.DataFrame(parsed_rows)