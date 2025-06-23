import os
import pandas as pd
from google.cloud import storage

# --- Configuration ---
# This should be set as an environment variable in your Cloud Function.
BUCKET_NAME = os.environ.get('BUCKET_NAME', 'your-default-bucket-name')
# -------------------

def process_csv_to_text(df, file_type, bucket):
    """
    A generic function to convert rows of a DataFrame into text files.
    """
    print(f"Processing {file_type} data...")
    processed_count = 0
    
    # Loop through each row of the CSV data
    for index, row in df.iterrows():
        content = ""
        output_filename = ""

        # --- UPDATED: This section is customized for YOUR column names ---
        if file_type == 'items':
            # Uses: item_id, item_name, purchased_store_name
            content = (
                f"Regarding item ID {row['item_id']}: "
                f"The item is a '{row['item_name']}', which was purchased from '{row['purchased_store_name']}'."
            )
            output_filename = f"processed_for_rag/items/item_{row['item_id']}.txt"

        elif file_type == 'branches':
            # Uses: branch_id, branch_name, branch_location, head_of_branch
            content = (
                f"Information for branch ID {row['branch_id']}: "
                f"The branch name is '{row['branch_name']}', located in {row['branch_location']}. "
                f"The head of this branch is {row['head_of_branch']}."
            )
            output_filename = f"processed_for_rag/branches/branch_{row['branch_id']}.txt"

        elif file_type == 'sales':
            # Uses: sale_id, quantity, item_name, branch_name (from the merge)
            content = (
                f"A sales transaction with ID {row['sale_id']} was recorded. "
                f"It involved selling {row['quantity']} unit(s) of the item '{row['item_name']}' "
                f"at the '{row['branch_name']}' branch."
            )
            output_filename = f"processed_for_rag/sales/sale_{row['sale_id']}.txt"

        if content and output_filename:
            # Upload the new text file to the destination folder
            blob = bucket.blob(output_filename)
            blob.upload_from_string(content, content_type='text/plain')
            processed_count += 1
    
    print(f"Finished processing {processed_count} records for {file_type}.")


def process_all_csvs(request):
    """
    Main Cloud Function entry point. Reads all CSVs and processes them.
    """
    storage_client = storage.Client()
    bucket = storage_client.bucket(BUCKET_NAME)

    try:
        # --- Load CSVs into Pandas DataFrames ---
        # IMPORTANT: Make sure your uploaded files are named items.csv, branches.csv, and sales.csv
        print("Loading CSV files from GCS...")
        items_df = pd.read_csv(f"gs://{BUCKET_NAME}/raw_data/csvs/items.csv")
        branches_df = pd.read_csv(f"gs://{BUCKET_NAME}/raw_data/csvs/branches.csv")
        sales_df = pd.read_csv(f"gs://{BUCKET_NAME}/raw_data/csvs/sales.csv")

        # --- Smartly combine sales data with item and branch names ---
        # This is like doing a SQL JOIN, but in Python with Pandas. This part remains the same.
        sales_with_names = pd.merge(sales_df, items_df[['item_id', 'item_name']], on='item_id')
        sales_with_names = pd.merge(sales_with_names, branches_df[['branch_id', 'branch_name']], on='branch_id')

        # --- Process each DataFrame ---
        process_csv_to_text(items_df, 'items', bucket)
        process_csv_to_text(branches_df, 'branches', bucket)
        process_csv_to_text(sales_with_names, 'sales', bucket)

        return "CSV processing completed successfully.", 200

    except Exception as e:
        print(f"An error occurred: {e}")
        return f"An error occurred: {e}", 500