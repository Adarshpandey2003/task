
import pandas as pd
import logging 
import time
from datetime import datetime

# ============================================================================
# CONFIGURATION
# ============================================================================

# Configure logging to write only to file
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S',
    filename='log.txt',
    filemode='w', 
)

print("Logging configured - output will be written to log.txt")

# ============================================================================
# DATA LOADING
# ============================================================================
logging.info("="*80)
logging.info("Starting data loading process...")
start_time = time.time()

try:
    data = {
        'materials': pd.read_excel('materials.xlsx', sheet_name='materials'),
        'plants': pd.read_excel('plants.xlsx', sheet_name='plants'),
        'storage': pd.read_excel('storage.xlsx', sheet_name='storage'),
        'suppliers': pd.read_excel('suppliers.xlsx', sheet_name='suppliers'),
        'supplier_names': pd.read_excel('supplier-names.xlsx', sheet_name='supplier-names'),
        'manufacturer_names': pd.read_excel('manufacturer-names.xlsx', sheet_name='manufacturer-names')
    }
    
    total_time = time.time() - start_time
    logging.info(f"All files loaded successfully in {total_time:.2f}s")
    logging.info("="*80)
    
except FileNotFoundError as e:
    logging.error(f"File not found: {e}")
    print(f"Error: {e}")
    exit(1)
except Exception as e:
    logging.error(f"An unexpected error occurred: {e}")
    print(f"Error: {e}")
    exit(1)

# ============================================================================
# DATA TRANSFORMATIONS
# ============================================================================
logging.info("="*80)
logging.info("Applying transformations...")

# Replace whitespace with 'ACTIVE' in DeletedStorageLevel
data['storage']['DeletedStorageLevel'].replace(' ', 'ACTIVE', inplace=True)
logging.info("✓ Replaced whitespace with 'ACTIVE' in DeletedStorageLevel")

# ============================================================================
# DATA QUALITY CHECK (Optional - can be commented out for production)
# ============================================================================
print("\nData Quality Summary:")
for key, df in data.items():
    print(f"\n{'='*60}")
    print(f"DataFrame: '{key}'")
    print(f"{'='*60}")
    print(f"Number of rows: {len(df)}")
    
    null_counts = df.isna().sum()
    if null_counts.sum() > 0:
        print(f"\nNull Values:")
        print(null_counts[null_counts > 0])
        print(f"Total null values: {null_counts.sum()}")
    
    print(f"\nUnique Values:")
    unique_counts = df.nunique()
    print(unique_counts)

# ============================================================================
# HANDLE NULL VALUES IN SOURCE DATA
# ============================================================================

# Fill null TypeCode values with generated codes
print("\nHandling null values in TypeCode...")
data['materials']['TypeCode'] = data['materials'].apply(
    lambda row: 'TC00000000' + row['MaterialReference'][-2:] if pd.isna(row['TypeCode']) else row['TypeCode'],
    axis=1
)
print(f"Null TypeCode count after filling: {data['materials']['TypeCode'].isna().sum()}")
logging.info("✓ Filled null TypeCode values")

# Convert Plant column to string to preserve leading zeros
data['plants']['Plant'] = data['plants']['Plant'].astype(str).str.zfill(4)
data['storage']['Plant'] = data['storage']['Plant'].astype(str).str.zfill(4)
logging.info("✓ Converted Plant column to string with leading zeros")

logging.info("Transformations applied successfully.")
logging.info("="*80)

# ============================================================================
# DATA CLEANING - Remove whitespace
# ============================================================================
logging.info("Starting data cleaning process...")
clean_start = time.time()

for key, df in data.items():
    logging.info(f"Cleaning {key} dataset...")
    # Strip whitespace from all string columns
    for col in df.columns:
        if df[col].dtype == 'object':  # String columns
            df[col] = df[col].astype(str).str.strip()
            # Replace empty strings with NaN
            df[col] = df[col].replace('', pd.NA)
            df[col] = df[col].replace('nan', pd.NA)
    
    data[key] = df
    logging.info(f"✓ Cleaned {key}: {len(df)} rows")

clean_time = time.time() - clean_start
logging.info(f"Data cleaning completed in {clean_time:.2f}s")
print(f"\nData cleaning completed in {clean_time:.2f}s")

# ============================================================================
# DATA AGGREGATION
# Aggregation strategy:
# 1. Start with materials as the base
# 2. Add manufacturer names via ManufacturerID
# 3. Join with plants data via MaterialReference
# 4. Join with suppliers data via MaterialReference
# 5. Add supplier names via SupplierID
# 6. Join with storage data via MaterialReference and Plant
# ============================================================================

logging.info("="*80)
logging.info("Starting data aggregation process...")
aggregation_start = time.time()

# Step 1: Merge materials with manufacturer names
logging.info("Step 1: Merging materials with manufacturer names...")
step1_start = time.time()
result = data['materials'].merge(
    data['manufacturer_names'],
    on='ManufacturerID',
    how='left'
)
result.drop(columns=['ManufacturerID'], inplace=True)
logging.info(f"Step 1 completed: {len(result)} rows in {time.time() - step1_start:.2f}s")

# Step 2: Merge with plants data
logging.info("Step 2: Merging with plants data...")
step2_start = time.time()
result = result.merge(data['plants'], on='MaterialReference', how='left')
logging.info(f"Step 2 completed: {len(result)} rows in {time.time() - step2_start:.2f}s")

# Step 3: Merge with suppliers data
logging.info("Step 3: Merging with suppliers data...")
step3_start = time.time()
result = result.merge(data['suppliers'], on=['MaterialReference'], how='left')
logging.info(f"Step 3 completed: {len(result)} rows in {time.time() - step3_start:.2f}s")

# Step 4: Add supplier names
logging.info("Step 4: Adding supplier names...")
step4_start = time.time()
result = result.merge(data['supplier_names'], on='SupplierID', how='left')
result.drop(columns=['SupplierID'], inplace=True)
logging.info(f"Step 4 completed: {len(result)} rows in {time.time() - step4_start:.2f}s")

# Step 5: Merge with storage data
logging.info("Step 5: Merging with storage data...")
step5_start = time.time()
result = result.merge(data['storage'], on=['MaterialReference', 'Plant'], how='left')
logging.info(f"Step 5 completed: {len(result)} rows in {time.time() - step5_start:.2f}s")

total_aggregation_time = time.time() - aggregation_start
logging.info(f"✓ Aggregation completed successfully in {total_aggregation_time:.2f}s")
logging.info(f"Final result: {len(result)} rows, {len(result.columns)} columns")
logging.info("="*80)

# ============================================================================
# FORMATTING OUTPUT - Reorder columns
# ============================================================================
logging.info("Formatting result columns...")
result = result[[
    'MaterialReference',
    'ManufacturerName',
    'ArticleNumber',
    'TypeCode',
    'ShortText',
    'Plant',
    'Disposition',
    'ReporderPoint',
    'SupplierName',
    'SupplierArticleNumber',
    'StorageLocation',
    'StorageBin',
    'DeletedStorageLevel'
]]

logging.info(f"Columns formatted: {list(result.columns)}")

# ============================================================================
# FILL NULL VALUES WITH N/A
# ============================================================================
logging.info("Filling null values with 'N/A'...")
null_fill_start = time.time()

# Fill all null values with 'N/A'
result = result.fillna('N/A')

# Verify no null values remain
remaining_nulls = result.isna().sum().sum()
print(f"\nRemaining null values: {remaining_nulls}")
logging.info(f"Remaining null values after filling: {remaining_nulls}")

# ============================================================================
# DATA QUALITY REPORT
# ============================================================================
print("\n" + "="*80)
print("FINAL RESULT SUMMARY")
print("="*80)

# Count N/A values
na_counts = (result == 'N/A').sum()
print("\nN/A counts per column:")
print(na_counts)
print(f"\nTotal N/A values: {na_counts.sum()}")

# Count rows with N/A
na_row_counts = result.isin(['N/A']).any(axis=1).sum()
print(f"Total rows with at least one N/A value: {na_row_counts}")
print(f"Total rows: {len(result)}")
print(f"Percentage of rows with N/A: {(na_row_counts/len(result)*100):.2f}%")

print("\nFirst 5 rows of result:")
print(result.head())

# ============================================================================
# EXPORT TO EXCEL
# ============================================================================
logging.info("Starting export to Excel...")
export_start = time.time()

try:
    with pd.ExcelWriter('result.xlsx', engine='openpyxl') as writer:
        result.to_excel(writer, sheet_name='result-template', index=False)
    
    export_time = time.time() - export_start
    logging.info(f"Successfully exported {len(result)} rows to result.xlsx in {export_time:.2f}s")
    print(f"\n✓ Successfully exported {len(result)} rows to result.xlsx")
    
except Exception as e:
    logging.error(f"Error exporting to Excel: {e}")
    print(f"Error exporting to Excel: {e}")
    exit(1)

# ============================================================================
# COMPLETION
# ============================================================================
print("\n" + "="*80)
print("✓ Process completed successfully!")
print("="*80)
print(f"  - Total aggregation time: {total_aggregation_time:.2f}s")
print(f"  - Export time: {export_time:.2f}s")
print(f"  - Result file: result.xlsx ({len(result)} rows)")
print(f"  - Log file: log.txt")
print("="*80)
