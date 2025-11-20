import os
import pandas as pd
import snowflake.snowpark as snowpark
from openpyxl import load_workbook
import logging

# Set up logging for the Stored Procedure environment
logger = logging.getLogger("stored_procedure_logger")
logger.setLevel(logging.INFO)

def load_excel_worksheet_to_table_local(session, stage_file_path, worksheet_name, target_table):
    """
    Downloads a specific Excel file from a Snowflake stage, reads a specified
    worksheet into a Pandas DataFrame, and loads it into a Snowflake table.
    """
    # Use a standard temporary directory for Stored Procedures
    local_directory = "/tmp/" 
    file_name = os.path.basename(stage_file_path)

    logger.info(f"Attempting to GET file: {stage_file_path}")
    
    # 1. Copy file from stage to local storage
    session.file.get(stage_file_path, local_directory)

    full_local_path = os.path.join(local_directory, file_name)

    # 2. Open the file from the local path and process
    try:
        with open(full_local_path, 'rb') as f:
            workbook = load_workbook(f)
            sheet = workbook[worksheet_name]
            data = sheet.values

            # Get the first line in file as a header line
            columns = next(data)[0:]
            
            # Create a Pandas DataFrame
            df = pd.DataFrame(data, columns=columns)
        
            # 3. Create a Snowpark DataFrame and load into Snowflake table
            df2 = session.create_dataframe(df)
            df2.write.mode("overwrite").save_as_table(target_table)
            
            logger.info(f"Successfully loaded data into table: {target_table}")
    except Exception as e:
        logger.error(f"Error processing file {file_name}: {e}")
        raise e
    finally:
        # 4. Clean up the local file after processing
        if os.path.exists(full_local_path):
            os.remove(full_local_path)

    return True

# This is the required HANDLER function for the Stored Procedure
def main(session: snowpark.Session):
    """
    Main entry point for the stored procedure. Executes the file mapping query
    and orchestrates the loading of all mapped Excel files.
    """
    # 1. Using the explicit UNION query from the lab notebook to get file paths
    mapping_query = """
        SELECT '@INTEGRATIONS.FROSTBYTE_RAW_STAGE/intro/order_detail.xlsx' AS STAGE_FILE_PATH, 'order_detail' AS WORKSHEET_NAME, 'ORDER_DETAIL' AS TARGET_TABLE
        UNION
        SELECT '@INTEGRATIONS.FROSTBYTE_RAW_STAGE/intro/location.xlsx', 'location', 'LOCATION'
    """
    
    # Execute query and collect results into local Row objects
    mapping_df_rows = session.sql(mapping_query).collect() 
    
    logger.info(f"Found {len(mapping_df_rows)} files to process.")

    # 2. Iterate and process files
    for row in mapping_df_rows:
        stage_file_path = row['STAGE_FILE_PATH']
        worksheet_name = row['WORKSHEET_NAME']
        target_table = row['TARGET_TABLE']
        
        load_excel_worksheet_to_table_local(
            session, 
            stage_file_path, 
            worksheet_name, 
            target_table
        )

    logger.info("06_load_excel_files end")
    return "SUCCESS: Excel files loaded into Snowflake tables."