import snowflake.snowpark as snowpark

# This function is what Snowflake is looking for!
def main(session: snowpark.Session):
    # NOTE: If this procedure is part of the lab,
    # you need to insert the actual logic of the lab here.

    # For now, we return a simple success message to confirm creation.
    return "SUCCESS: Procedure DEV_06_load_excel_files was created."