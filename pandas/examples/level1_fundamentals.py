"""
Level 1: Pandas Fundamentals - Runnable Examples
Run this with: python level1_fundamentals.py
"""

import pandas as pd
import numpy as np

print("=" * 60)
print("PANDAS LEVEL 1: FUNDAMENTALS")
print("=" * 60)

# ============================================================================
# 1. SERIES BASICS
# ============================================================================
print("\n1. SERIES BASICS")
print("-" * 60)

# Create a Series (1-dimensional data)
print("\n1.1 Create a Series from a list:")
s = pd.Series([10, 20, 30, 40, 50])
print(s)

print("\n1.2 Create a Series with labels (index):")
movies = pd.Series(
    [8.5, 9.0, 7.5, 8.0, 9.5],
    index=['Inception', 'Avatar', 'Interstellar', 'Titanic', 'Parasite']
)
print(movies)

print("\n1.3 Access Series values:")
print(f"First value: {movies.iloc[0]}")
print(f"By label - 'Inception': {movies['Inception']}")

print("\n1.4 Series attributes:")
print(f"Data type: {movies.dtype}")
print(f"Index: {movies.index.tolist()}")
print(f"Number of elements: {len(movies)}")

# ============================================================================
# 2. DATAFRAME BASICS
# ============================================================================
print("\n\n2. DATAFRAME BASICS")
print("-" * 60)

print("\n2.1 Create a DataFrame from dictionary:")
data = {
    'name': ['Alice', 'Bob', 'Charlie', 'Diana'],
    'age': [22, 23, 21, 22],
    'grade': [85, 90, 78, 88],
    'major': ['CS', 'Math', 'Physics', 'CS']
}
df = pd.DataFrame(data)
print(df)

print("\n2.2 View DataFrame info:")
print(f"Shape (rows, columns): {df.shape}")
print(f"\nColumn names: {df.columns.tolist()}")
print(f"\nData types:\n{df.dtypes}")

print("\n2.3 Statistical summary:")
print(df.describe())

# ============================================================================
# 3. READING & VIEWING DATA
# ============================================================================
print("\n\n3. READING & VIEWING DATA")
print("-" * 60)

print("\n3.1 View first few rows:")
print(df.head(2))

print("\n3.2 View last few rows:")
print(df.tail(2))

print("\n3.3 Get information about DataFrame:")
df.info()

# ============================================================================
# 4. ACCESSING DATA
# ============================================================================
print("\n\n4. ACCESSING DATA")
print("-" * 60)

print("\n4.1 Access single column:")
print(df['name'])

print("\n4.2 Access multiple columns:")
print(df[['name', 'grade']])

print("\n4.3 Access by row position (iloc):")
print(df.iloc[0])

print("\n4.4 Access by label (loc):")
print(df.loc[0])

print("\n4.5 Access specific cell:")
print(f"Alice's grade: {df.loc[0, 'grade']}")

# ============================================================================
# 5. BASIC OPERATIONS & STATISTICS
# ============================================================================
print("\n\n5. BASIC OPERATIONS & STATISTICS")
print("-" * 60)

print("\n5.1 Column statistics:")
print(f"Average age: {df['age'].mean():.1f}")
print(f"Max grade: {df['grade'].max()}")
print(f"Min grade: {df['grade'].min()}")
print(f"Total students: {len(df)}")

print("\n5.2 Mathematical operations:")
df_copy = df.copy()
df_copy['grade_bonus'] = df_copy['grade'] * 1.1
print(df_copy[['name', 'grade', 'grade_bonus']])

# ============================================================================
# 6. FILTERING DATA
# ============================================================================
print("\n\n6. FILTERING DATA")
print("-" * 60)

print("\n6.1 Filter rows with age > 21:")
above_21 = df[df['age'] > 21]
print(above_21)

print("\n6.2 Filter rows with grade >= 85:")
good_grades = df[df['grade'] >= 85]
print(good_grades)

print("\n6.3 Filter with multiple conditions (AND):")
result = df[(df['age'] > 21) & (df['grade'] >= 85)]
print(result)

print("\n6.4 Filter with OR condition:")
result = df[(df['major'] == 'CS') | (df['major'] == 'Math')]
print(result)

print("\n6.5 Filter by text match:")
cs_students = df[df['major'] == 'CS']
print(cs_students)

# ============================================================================
# 7. SORTING
# ============================================================================
print("\n\n7. SORTING")
print("-" * 60)

print("\n7.1 Sort by grade (ascending):")
print(df.sort_values('grade'))

print("\n7.2 Sort by grade (descending):")
print(df.sort_values('grade', ascending=False))

print("\n7.3 Sort by multiple columns:")
print(df.sort_values(['major', 'grade']))

# ============================================================================
# 8. CREATING & WRITING FILES
# ============================================================================
print("\n\n8. CREATING & WRITING FILES")
print("-" * 60)

# Create sample data
products = pd.DataFrame({
    'product': ['Laptop', 'Phone', 'Tablet', 'Monitor'],
    'price': [1000, 500, 300, 250],
    'quantity': [5, 10, 15, 8]
})

# Save to CSV
csv_path = '/tmp/products.csv'
products.to_csv(csv_path, index=False)
print(f"\n8.1 Saved to CSV: {csv_path}")

# Read back
df_read = pd.read_csv(csv_path)
print("\n8.2 Read back from CSV:")
print(df_read)

# ============================================================================
# 9. ADDING COLUMNS & COMPUTED FIELDS
# ============================================================================
print("\n\n9. ADDING COLUMNS & COMPUTED FIELDS")
print("-" * 60)

df_sales = df.copy()
print("\n9.1 Add new column (total price):")
df_sales['total'] = products['price'] * products['quantity']
print(df_sales)

print("\n9.2 Add column with conditional logic:")
df_copy = df.copy()
df_copy['performance'] = df_copy['grade'].apply(
    lambda x: 'Good' if x >= 85 else 'Average' if x >= 75 else 'Needs Work'
)
print(df_copy[['name', 'grade', 'performance']])

# ============================================================================
# 10. HANDLING MISSING VALUES
# ============================================================================
print("\n\n10. HANDLING MISSING VALUES")
print("-" * 60)

# Create data with missing values
df_missing = pd.DataFrame({
    'name': ['Alice', 'Bob', None, 'Diana'],
    'age': [22, None, 21, 22],
    'grade': [85, 90, 78, None]
})

print("\n10.1 Check for missing values:")
print(df_missing.isnull())

print("\n10.2 Count missing values:")
print(df_missing.isnull().sum())

print("\n10.3 Drop rows with missing values:")
print(df_missing.dropna())

print("\n10.4 Fill missing values:")
df_filled = df_missing.copy()
df_filled['age'] = df_filled['age'].fillna(df_filled['age'].mean())
df_filled['name'] = df_filled['name'].fillna('Unknown')
print(df_filled)

# ============================================================================
# 11. DATA TYPES
# ============================================================================
print("\n\n11. DATA TYPES")
print("-" * 60)

print("\n11.1 Check data types:")
print(df.dtypes)

print("\n11.2 Convert data types:")
df_converted = df.copy()
df_converted['age'] = df_converted['age'].astype(str)
print(f"Age column type after conversion: {df_converted['age'].dtype}")

# ============================================================================
# 12. REAL-WORLD EXAMPLE: SALES ANALYSIS
# ============================================================================
print("\n\n12. REAL-WORLD EXAMPLE: SALES ANALYSIS")
print("-" * 60)

# Create sample sales data
np.random.seed(42)
sales_data = pd.DataFrame({
    'date': pd.date_range('2024-01-01', periods=10),
    'product': ['Laptop', 'Phone', 'Tablet', 'Monitor', 'Keyboard'] * 2,
    'quantity': np.random.randint(1, 10, 10),
    'price': [1000, 500, 300, 250, 100] * 2,
})

print("\n12.1 Sales data:")
print(sales_data)

print("\n12.2 Add total column:")
sales_data['total'] = sales_data['quantity'] * sales_data['price']
print(sales_data)

print("\n12.3 Statistics:")
print(f"Total revenue: ${sales_data['total'].sum():,.0f}")
print(f"Average order value: ${sales_data['total'].mean():,.0f}")
print(f"Highest price product: {sales_data.loc[sales_data['price'].idxmax(), 'product']}")

print("\n12.4 High-value orders (total > $2000):")
high_value = sales_data[sales_data['total'] > 2000]
print(high_value)

print("\n12.5 Sorted by price (descending):")
print(sales_data.sort_values('price', ascending=False))

# ============================================================================
# SUMMARY
# ============================================================================
print("\n\n" + "=" * 60)
print("SUMMARY - You've learned:")
print("=" * 60)
print("""
✓ Create Series (1D) and DataFrames (2D)
✓ Access data by position (iloc) and label (loc)
✓ Filter data with conditions
✓ Sort and explore data
✓ Read and write CSV files
✓ Add computed columns
✓ Handle missing values
✓ Convert data types
✓ Perform basic statistics
✓ Apply real-world analysis

Next: Check out Level 2 for data cleaning and groupby operations!
""")
