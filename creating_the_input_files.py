import pandas as pd

# Load data
file_path = "data/auto_sales_data.csv"  # Assuming the data is stored in a CSV file
data = pd.read_csv(file_path)

data['ORDERDATE'] = pd.to_datetime(data['ORDERDATE'], format='%d/%m/%Y').dt.strftime('%Y-%m-%d')

# Print the data to verify
print(data.head())

# Define columns to remove incrementally
columns_to_remove = ["PHONE", "MSRP", "POSTALCODE"]  # Columns to exclude initially
columns_to_add_back = ["PHONE", "MSRP", "POSTALCODE"]  # Add one back incrementally

# Create files with changing schemas
max_records_per_file = 750
file_count = 1
start_idx = 0

for i in range(4):
    # Adjust columns for the current file
    if i == 0:
        current_columns = [col for col in data.columns if col not in columns_to_remove]
    else:
        current_columns.append(columns_to_add_back[i - 1])
    
    # Slice data for the current file
    end_idx = start_idx + max_records_per_file
    file_data = data.iloc[start_idx:end_idx, :][current_columns]
    
    # Save the current file
    file_data.to_csv(f"data/auto_sales_data_day_{file_count}.csv", index=False)
    print(f"File {file_count} created with {len(current_columns)} columns.")

    start_idx += max_records_per_file
    
    # Increment file count
    file_count += 1
