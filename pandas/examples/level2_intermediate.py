"""
Level 2: Pandas Intermediate - Runnable Examples
Run this with: python level2_intermediate.py
"""

import pandas as pd
import numpy as np

print("=" * 60)
print("PANDAS LEVEL 2: INTERMEDIATE")
print("=" * 60)

# ============================================================================
# 1. DATA CLEANING - MISSING VALUES
# ============================================================================
print("\n1. DATA CLEANING - MISSING VALUES")
print("-" * 60)

# Create data with missing values
df_missing = pd.DataFrame({
    'name': ['Alice', 'Bob', None, 'Diana', 'Eve'],
    'age': [22, None, 21, 22, 25],
    'salary': [50000, 60000, None, 55000, 70000],
    'department': ['Sales', 'IT', 'Sales', None, 'HR']
})

print("\n1.1 Original data with missing values:")
print(df_missing)

print("\n1.2 Check for missing values:")
print(df_missing.isnull())

print("\n1.3 Count missing values per column:")
print(df_missing.isnull().sum())

print("\n1.4 Drop rows with any missing values:")
print(df_missing.dropna())

print("\n1.5 Drop rows where specific column is missing:")
print(df_missing.dropna(subset=['name']))

print("\n1.6 Fill missing values with constant:")
df_filled = df_missing.copy()
df_filled['age'] = df_filled['age'].fillna(0)
df_filled['name'] = df_filled['name'].fillna('Unknown')
print(df_filled)

print("\n1.7 Fill with mean/median:")
df_filled = df_missing.copy()
df_filled['age'] = df_filled['age'].fillna(df_filled['age'].mean())
df_filled['salary'] = df_filled['salary'].fillna(df_filled['salary'].median())
print(df_filled)

print("\n1.8 Forward fill (use previous value):")
df_time = pd.DataFrame({
    'date': pd.date_range('2024-01-01', periods=5),
    'value': [100, None, None, 150, None]
})
print("Original:")
print(df_time)
print("\nAfter forward fill:")
print(df_time.fillna(method='ffill'))

# ============================================================================
# 2. DUPLICATES
# ============================================================================
print("\n\n2. HANDLING DUPLICATES")
print("-" * 60)

df_dup = pd.DataFrame({
    'user_id': [1, 1, 2, 2, 3],
    'name': ['Alice', 'Alice', 'Bob', 'Bob', 'Charlie'],
    'email': ['alice@email.com', 'alice@email.com', 'bob@email.com', 'bob@email.com', 'charlie@email.com']
})

print("\n2.1 Original data with duplicates:")
print(df_dup)

print("\n2.2 Find duplicates:")
print(df_dup.duplicated())

print("\n2.3 Find duplicates (keep=False shows all duplicates):")
print(df_dup.duplicated(keep=False))

print("\n2.4 Drop duplicates (keep first):")
print(df_dup.drop_duplicates())

print("\n2.5 Drop duplicates based on specific column:")
print(df_dup.drop_duplicates(subset=['user_id']))

# ============================================================================
# 3. TYPE CONVERSION
# ============================================================================
print("\n\n3. TYPE CONVERSION")
print("-" * 60)

df_types = pd.DataFrame({
    'age': ['22', '23', '21', '25'],
    'salary': ['50000.50', '60000.75', '55000.00', '70000.25'],
    'active': ['true', 'false', 'true', 'true']
})

print("\n3.1 Original data types:")
print(df_types.dtypes)

print("\n3.2 Convert age to integer:")
df_types['age'] = df_types['age'].astype(int)
print(df_types.dtypes)

print("\n3.3 Convert salary to float:")
df_types['salary'] = df_types['salary'].astype(float)
print(df_types.dtypes)

print("\n3.4 Convert boolean strings:")
df_types['active'] = df_types['active'].map({'true': True, 'false': False})
print(df_types.dtypes)

# ============================================================================
# 4. DATE/TIME OPERATIONS
# ============================================================================
print("\n\n4. DATE/TIME OPERATIONS")
print("-" * 60)

df_dates = pd.DataFrame({
    'date_string': ['2024-01-01', '2024-01-15', '2024-02-01', '2024-03-15'],
    'sales': [1000, 1500, 2000, 1800]
})

print("\n4.1 Original date_string column (object type):")
print(df_dates.dtypes)

print("\n4.2 Convert to datetime:")
df_dates['date'] = pd.to_datetime(df_dates['date_string'])
print(df_dates.dtypes)

print("\n4.3 Extract year, month, day:")
df_dates['year'] = df_dates['date'].dt.year
df_dates['month'] = df_dates['date'].dt.month
df_dates['day'] = df_dates['date'].dt.day
df_dates['dayofweek'] = df_dates['date'].dt.day_name()
print(df_dates[['date', 'year', 'month', 'day', 'dayofweek']])

# ============================================================================
# 5. STRING OPERATIONS
# ============================================================================
print("\n\n5. STRING OPERATIONS")
print("-" * 60)

df_text = pd.DataFrame({
    'name': ['  alice  ', '  BOB  ', '  charlie  '],
    'email': ['ALICE@EMAIL.COM', 'BOB@EMAIL.COM', 'CHARLIE@EMAIL.COM']
})

print("\n5.1 Original data:")
print(df_text)

print("\n5.2 Convert to lowercase:")
df_text['name_lower'] = df_text['name'].str.lower()
print(df_text[['name', 'name_lower']])

print("\n5.3 Convert to uppercase:")
df_text['email_upper'] = df_text['email'].str.upper()
print(df_text[['email', 'email_upper']])

print("\n5.4 Strip whitespace:")
df_text['name_clean'] = df_text['name'].str.strip()
print(df_text[['name', 'name_clean']])

print("\n5.5 Title case:")
df_text['name_title'] = df_text['name'].str.strip().str.title()
print(df_text[['name', 'name_title']])

print("\n5.6 Contains substring:")
df_text['has_email'] = df_text['email'].str.contains('@')
print(df_text[['email', 'has_email']])

print("\n5.7 Replace text:")
df_text['email_replaced'] = df_text['email'].str.replace('@EMAIL', '@mail')
print(df_text[['email', 'email_replaced']])

# ============================================================================
# 6. COLUMN OPERATIONS
# ============================================================================
print("\n\n6. COLUMN OPERATIONS")
print("-" * 60)

df_cols = pd.DataFrame({
    'first_name': ['Alice', 'Bob', 'Charlie'],
    'last_name': ['Smith', 'Jones', 'Brown'],
    'age': [22, 23, 21]
})

print("\n6.1 Rename columns:")
df_renamed = df_cols.rename(columns={
    'first_name': 'fname',
    'last_name': 'lname'
})
print(df_renamed.columns.tolist())

print("\n6.2 Combine columns:")
df_cols['full_name'] = df_cols['first_name'] + ' ' + df_cols['last_name']
print(df_cols[['first_name', 'last_name', 'full_name']])

print("\n6.3 Drop columns:")
df_dropped = df_cols.drop(['first_name', 'last_name'], axis=1)
print(df_dropped.columns.tolist())

# ============================================================================
# 7. GROUPBY & AGGREGATION
# ============================================================================
print("\n\n7. GROUPBY & AGGREGATION")
print("-" * 60)

df_sales = pd.DataFrame({
    'date': pd.date_range('2024-01-01', periods=10),
    'region': ['North', 'South', 'East', 'West', 'North', 'South', 'East', 'West', 'North', 'South'],
    'product': ['Laptop', 'Phone', 'Tablet', 'Monitor', 'Laptop', 'Phone', 'Tablet', 'Monitor', 'Laptop', 'Phone'],
    'sales': [1000, 500, 300, 250, 1200, 600, 350, 280, 1100, 550],
    'quantity': [5, 10, 15, 8, 6, 12, 18, 9, 5, 11]
})

print("\n7.1 Group by single column and sum:")
print(df_sales.groupby('region')['sales'].sum())

print("\n7.2 Group by single column with multiple aggregations:")
print(df_sales.groupby('region')['sales'].agg(['sum', 'mean', 'count']))

print("\n7.3 Group by multiple columns:")
print(df_sales.groupby(['region', 'product'])['sales'].sum())

print("\n7.4 Aggregate multiple columns:")
result = df_sales.groupby('region').agg({
    'sales': ['sum', 'mean'],
    'quantity': 'sum'
})
print(result)

print("\n7.5 Named aggregations:")
result = df_sales.groupby('region').agg(
    total_sales=('sales', 'sum'),
    avg_sales=('sales', 'mean'),
    num_transactions=('sales', 'count'),
    total_quantity=('quantity', 'sum')
)
print(result)

# ============================================================================
# 8. MERGING & JOINING
# ============================================================================
print("\n\n8. MERGING & JOINING")
print("-" * 60)

# Create two dataframes
employees = pd.DataFrame({
    'emp_id': [1, 2, 3, 4],
    'name': ['Alice', 'Bob', 'Charlie', 'Diana'],
    'dept_id': [1, 2, 1, 3]
})

departments = pd.DataFrame({
    'dept_id': [1, 2, 3],
    'dept_name': ['Sales', 'IT', 'HR']
})

print("\n8.1 Employees DataFrame:")
print(employees)

print("\n8.2 Departments DataFrame:")
print(departments)

print("\n8.3 Inner join (only matching rows):")
result = pd.merge(employees, departments, on='dept_id')
print(result)

print("\n8.4 Left join (keep all from left):")
result = pd.merge(employees, departments, on='dept_id', how='left')
print(result)

print("\n8.5 Concatenate DataFrames:")
df1 = pd.DataFrame({'A': [1, 2], 'B': [3, 4]})
df2 = pd.DataFrame({'A': [5, 6], 'B': [7, 8]})
result = pd.concat([df1, df2], ignore_index=True)
print(result)

# ============================================================================
# 9. SORTING
# ============================================================================
print("\n\n9. SORTING")
print("-" * 60)

df_sort = pd.DataFrame({
    'name': ['Alice', 'Bob', 'Charlie', 'Diana'],
    'age': [22, 23, 21, 22],
    'salary': [50000, 60000, 55000, 70000]
})

print("\n9.1 Sort by single column (ascending):")
print(df_sort.sort_values('age'))

print("\n9.2 Sort by single column (descending):")
print(df_sort.sort_values('salary', ascending=False))

print("\n9.3 Sort by multiple columns:")
print(df_sort.sort_values(['age', 'salary'], ascending=[True, False]))

# ============================================================================
# 10. PIVOTING
# ============================================================================
print("\n\n10. PIVOTING")
print("-" * 60)

df_pivot = pd.DataFrame({
    'month': ['Jan', 'Jan', 'Feb', 'Feb', 'Mar', 'Mar'],
    'region': ['North', 'South', 'North', 'South', 'North', 'South'],
    'sales': [1000, 500, 1200, 600, 1100, 550]
})

print("\n10.1 Original data:")
print(df_pivot)

print("\n10.2 Pivot (rows=month, columns=region, values=sales):")
pivoted = df_pivot.pivot_table(
    values='sales',
    index='month',
    columns='region',
    aggfunc='sum'
)
print(pivoted)

print("\n10.3 Unpivot (melt):")
df_wide = pd.DataFrame({
    'month': ['Jan', 'Feb', 'Mar'],
    'North': [1000, 1200, 1100],
    'South': [500, 600, 550]
})
melted = df_wide.melt(
    id_vars=['month'],
    value_vars=['North', 'South'],
    var_name='region',
    value_name='sales'
)
print(melted)

# ============================================================================
# 11. REAL-WORLD EXAMPLE: CUSTOMER DATA CLEANING
# ============================================================================
print("\n\n11. REAL-WORLD EXAMPLE: CUSTOMER DATA CLEANING")
print("-" * 60)

# Messy customer data (realistic!)
raw_customers = pd.DataFrame({
    'customer_id': [1, 2, 2, 3, 4, None],  # Duplicates and null
    'name': ['  ALICE  ', '  bob  ', 'bob', 'Charlie', '  DIANA  ', 'Eve'],  # Inconsistent
    'email': ['alice@email.com', None, 'bob@email.com', 'charlie@email', 'diana@email.com', 'eve@email'],  # Nulls & invalid
    'join_date': ['2024-01-01', '2024-01-02', '2024-01-02', '2024-13-01', '2024-01-05', '2024-01-06'],  # Invalid date
    'amount_spent': ['$100.50', '$200', '200.00', '$300.75', None, '$150']  # Different formats
})

print("\n11.1 Original messy data:")
print(raw_customers)

# Step 1: Remove duplicates
cleaned = raw_customers.drop_duplicates(subset=['customer_id'], keep='first')
print("\n11.2 After removing duplicates:")
print(cleaned)

# Step 2: Handle missing customer IDs
cleaned = cleaned.dropna(subset=['customer_id'])
cleaned['customer_id'] = cleaned['customer_id'].astype(int)
print("\n11.3 After fixing customer IDs:")
print(cleaned)

# Step 3: Clean names
cleaned['name'] = cleaned['name'].str.strip().str.title()
print("\n11.4 After cleaning names:")
print(cleaned[['name']])

# Step 4: Clean emails (handle nulls)
cleaned['email'] = cleaned['email'].fillna('unknown@email.com')
cleaned['email'] = cleaned['email'].str.lower()
print("\n11.5 After cleaning emails:")
print(cleaned[['email']])

# Step 5: Fix dates
cleaned['join_date'] = pd.to_datetime(cleaned['join_date'], errors='coerce')
print("\n11.6 After fixing dates:")
print(cleaned[['join_date']])

# Step 6: Clean amounts (remove $ and convert)
cleaned['amount_spent'] = cleaned['amount_spent'].str.replace('$', '').astype(float)
print("\n11.7 After cleaning amounts:")
print(cleaned[['amount_spent']])

print("\n11.8 Final cleaned data:")
print(cleaned)

print("\n11.9 Data quality report:")
print(f"Total customers: {len(cleaned)}")
print(f"Missing values:\n{cleaned.isnull().sum()}")
print(f"Duplicates: {cleaned.duplicated().sum()}")

# ============================================================================
# SUMMARY
# ============================================================================
print("\n\n" + "=" * 60)
print("SUMMARY - You've learned:")
print("=" * 60)
print("""
✓ Handle missing values (drop, fill, interpolate)
✓ Detect and remove duplicates
✓ Convert data types
✓ Parse and manipulate dates
✓ Clean text data
✓ Rename and combine columns
✓ Group data and aggregate
✓ Merge multiple datasets
✓ Sort by multiple criteria
✓ Pivot and reshape data
✓ Build realistic data cleaning pipelines

Next: Check out Level 3 for time series and advanced operations!
""")
