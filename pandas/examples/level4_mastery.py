"""
Level 4: Pandas Mastery - Runnable Examples
Run this with: python level4_mastery.py
"""

import pandas as pd
import numpy as np

print("=" * 60)
print("PANDAS LEVEL 4: MASTERY")
print("=" * 60)

# ============================================================================
# 1. PERFORMANCE OPTIMIZATION - DATA TYPES
# ============================================================================
print("\n1. PERFORMANCE OPTIMIZATION - DATA TYPES")
print("-" * 60)

# Create a large dataset
print("\n1.1 Creating sample dataset with 100,000 rows...")
df_large = pd.DataFrame({
    'age': np.random.randint(18, 100, 100000),
    'salary': np.random.randint(30000, 200000, 100000),
    'category': np.random.choice(['A', 'B', 'C'], 100000),
    'is_active': np.random.choice([True, False], 100000)
})

print("\n1.2 Original memory usage:")
memory_before = df_large.memory_usage(deep=True).sum() / 1024 / 1024
print(f"Total: {memory_before:.2f} MB")
print(df_large.memory_usage(deep=True))

print("\n1.3 Optimize int64 → int8 (age: 18-100):")
df_opt = df_large.copy()
df_opt['age'] = df_opt['age'].astype('int8')
print(f"Savings: {df_opt['age'].memory_usage(deep=True) / 1024} KB")

print("\n1.4 Optimize int64 → int32 (salary):")
df_opt['salary'] = df_opt['salary'].astype('int32')
print(f"Savings: {df_opt['salary'].memory_usage(deep=True) / 1024} KB")

print("\n1.5 Optimize object → category (category column):")
df_opt['category'] = df_opt['category'].astype('category')
print(f"Savings: {df_opt['category'].memory_usage(deep=True) / 1024} KB")

print("\n1.6 Optimized memory usage:")
memory_after = df_opt.memory_usage(deep=True).sum() / 1024 / 1024
print(f"Total: {memory_after:.2f} MB")
print(f"Reduction: {(1 - memory_after/memory_before) * 100:.1f}%")

# ============================================================================
# 2. CHUNKING LARGE FILES
# ============================================================================
print("\n\n2. CHUNKING LARGE FILES")
print("-" * 60)

# Create a sample large CSV
print("\n2.1 Creating sample large CSV...")
large_data = pd.DataFrame({
    'id': range(100000),
    'value': np.random.rand(100000),
    'category': np.random.choice(['A', 'B', 'C'], 100000)
})
large_data.to_csv('/tmp/large_file.csv', index=False)

print("\n2.2 Process in chunks (10,000 rows per chunk):")
chunk_count = 0
total_sum = 0

for chunk in pd.read_csv('/tmp/large_file.csv', chunksize=10000):
    chunk_count += 1
    total_sum += chunk['value'].sum()
    print(f"Chunk {chunk_count}: processed {len(chunk)} rows, sum={chunk['value'].sum():.2f}")

print(f"\nTotal chunks: {chunk_count}")
print(f"Total sum: {total_sum:.2f}")

# ============================================================================
# 3. VECTORIZATION vs LOOPS
# ============================================================================
print("\n\n3. VECTORIZATION vs LOOPS")
print("-" * 60)

df_vec = pd.DataFrame({
    'price': [100, 200, 300, 400, 500],
    'quantity': [1, 2, 3, 4, 5]
})

print("\n3.1 BAD: Using loop (slow)")
print("Code: result = []; for row in df: result.append(row['price'] * row['quantity'])")
print("DON'T DO THIS IN PANDAS!")

print("\n3.2 GOOD: Using vectorization (fast)")
df_vec['total'] = df_vec['price'] * df_vec['quantity']
print(df_vec)

print("\n3.3 Speed comparison (1M rows):")
large_df = pd.DataFrame({'A': range(1000000), 'B': range(1000000)})

# Vectorized (fast)
import time
start = time.time()
result_vec = large_df['A'] + large_df['B']
vec_time = time.time() - start

print(f"Vectorized: {vec_time*1000:.2f}ms")
print("Loop would take seconds!")

# ============================================================================
# 4. QUERY METHOD (EFFICIENT FILTERING)
# ============================================================================
print("\n\n4. QUERY METHOD (EFFICIENT FILTERING)")
print("-" * 60)

df_query = pd.DataFrame({
    'age': [22, 25, 30, 35, 40],
    'salary': [50000, 60000, 70000, 80000, 90000],
    'department': ['Sales', 'IT', 'Sales', 'HR', 'IT']
})

print("\n4.1 Traditional filtering:")
result1 = df_query[(df_query['age'] > 25) & (df_query['salary'] > 60000)]
print(result1)

print("\n4.2 Query method (more readable):")
result2 = df_query.query('age > 25 and salary > 60000')
print(result2)

print("\n4.3 Query with variable:")
min_age = 30
result3 = df_query.query('age >= @min_age')
print(result3)

# ============================================================================
# 5. CHAIN OPERATIONS (FLUENT API)
# ============================================================================
print("\n\n5. CHAIN OPERATIONS (FLUENT API)")
print("-" * 60)

df_chain = pd.DataFrame({
    'name': ['Alice', 'Bob', 'Charlie', 'Diana'],
    'age': [22, 25, 30, 35],
    'salary': [50000, 60000, 70000, 80000]
})

print("\n5.1 Chained operations (cleaner code):")
result = (df_chain
    .assign(bonus=lambda x: x['salary'] * 0.1)
    .query('age > 24')
    .sort_values('salary', ascending=False)
    [['name', 'salary', 'bonus']]
)
print(result)

print("\n5.2 Step by step:")
print("Step 1 - Add bonus column")
print("Step 2 - Filter age > 24")
print("Step 3 - Sort by salary descending")
print("Step 4 - Select columns")

# ============================================================================
# 6. DATA VALIDATION & QUALITY CHECKS
# ============================================================================
print("\n\n6. DATA VALIDATION & QUALITY CHECKS")
print("-" * 60)

df_validate = pd.DataFrame({
    'id': [1, 2, 3, 4, 5],
    'age': [25, 30, 150, 35, -5],  # Invalid: 150, -5
    'email': ['alice@email.com', 'bob@email.com', 'charlie.com', 'diana@email.com', None]
})

print("\n6.1 Data profiling:")
print(f"Rows: {len(df_validate)}")
print(f"Columns: {len(df_validate.columns)}")
print(f"Dtypes:\n{df_validate.dtypes}")
print(f"Missing values:\n{df_validate.isnull().sum()}")
print(f"Duplicates: {df_validate.duplicated().sum()}")

print("\n6.2 Data validation (assertions):")
try:
    assert (df_validate['age'] >= 0).all(), "Negative ages found!"
    assert (df_validate['age'] <= 120).all(), "Unrealistic ages found!"
    assert df_validate['email'].str.contains('@').sum() > len(df_validate) * 0.8, "Too many invalid emails"
except AssertionError as e:
    print(f"Validation error: {e}")

print("\n6.3 Outlier detection (Z-score):")
from scipy import stats
df_outlier = pd.DataFrame({
    'value': [10, 12, 11, 100, 13, 12, 11, 10, 1000]  # 100 and 1000 are outliers
})
df_outlier['zscore'] = np.abs(stats.zscore(df_outlier['value']))
print(df_outlier)
outliers = df_outlier[df_outlier['zscore'] > 3]
print(f"\nOutliers (|zscore| > 3):\n{outliers}")

# ============================================================================
# 7. ADVANCED GROUPBY OPERATIONS
# ============================================================================
print("\n\n7. ADVANCED GROUPBY OPERATIONS")
print("-" * 60)

df_adv_group = pd.DataFrame({
    'department': ['Sales', 'Sales', 'IT', 'IT', 'HR', 'HR'],
    'employee': ['Alice', 'Bob', 'Charlie', 'Diana', 'Eve', 'Frank'],
    'salary': [50000, 55000, 70000, 75000, 52000, 48000],
    'years': [2, 3, 5, 4, 1, 1]
})

print("\n7.1 Original data:")
print(df_adv_group)

print("\n7.2 Get group number (0, 1, 2):")
df_adv_group['group_num'] = df_adv_group.groupby('department').ngroup()
print(df_adv_group)

print("\n7.3 Get size of each group:")
group_sizes = df_adv_group.groupby('department').size()
print(group_sizes)

print("\n7.4 Get first employee from each group:")
first_of_group = df_adv_group.groupby('department').first()
print(first_of_group)

print("\n7.5 Apply custom function per group:")
def salary_range(group):
    return group['salary'].max() - group['salary'].min()

ranges = df_adv_group.groupby('department').apply(salary_range)
print(f"Salary range per department:\n{ranges}")

# ============================================================================
# 8. MEMORY EFFICIENCY & SPARSE DATA
# ============================================================================
print("\n\n8. MEMORY EFFICIENCY & SPARSE DATA")
print("-" * 60)

# Create data with many zeros
print("\n8.1 Create data with many zeros:")
df_sparse = pd.DataFrame({
    'A': [1, 0, 0, 0, 2, 0, 0],
    'B': [0, 0, 3, 0, 0, 0, 0],
    'C': [0, 0, 0, 4, 0, 0, 5]
})
print(df_sparse)

print("\n8.2 Convert to sparse array (saves memory):")
for col in df_sparse.columns:
    df_sparse[col] = pd.arrays.SparseArray(df_sparse[col])

print(f"Memory usage:\n{df_sparse.memory_usage(deep=True)}")

# ============================================================================
# 9. PRODUCTION ETL PIPELINE EXAMPLE
# ============================================================================
print("\n\n9. PRODUCTION ETL PIPELINE EXAMPLE")
print("-" * 60)

# Simulate reading raw data
raw_data = pd.DataFrame({
    'customer_id': [1, 2, 2, 3, None],  # Duplicates and null
    'name': ['  ALICE  ', '  bob  ', 'bob', 'Charlie', 'Diana'],  # Inconsistent case
    'email': ['alice@email.com', None, 'bob@email.com', 'charlie@email', 'diana@email.com'],
    'signup_date': ['2024-01-01', '2024-01-02', '2024-01-02', '2024-13-01', '2024-01-05'],
    'lifetime_value': ['$100.50', '200', '$300.75', 'invalid', '$150']
})

print("\n9.1 Raw data:")
print(raw_data)

print("\n9.2 ETL Pipeline:")

# Extract (already loaded)
print("✓ Extract: Data loaded")

# Transform
print("✓ Transform: Cleaning data...")

# Step 1: Remove nulls in key fields
cleaned = raw_data.dropna(subset=['customer_id'])

# Step 2: Remove duplicates
cleaned = cleaned.drop_duplicates(subset=['customer_id'], keep='first')

# Step 3: Clean text
cleaned['name'] = cleaned['name'].str.strip().str.title()
cleaned['email'] = cleaned['email'].str.lower()

# Step 4: Fix dates
cleaned['signup_date'] = pd.to_datetime(cleaned['signup_date'], errors='coerce')

# Step 5: Clean monetary values
cleaned['lifetime_value'] = cleaned['lifetime_value'].str.replace('$', '').astype(float, errors='ignore')
cleaned['lifetime_value'] = pd.to_numeric(cleaned['lifetime_value'], errors='coerce')

# Step 6: Remove nulls after cleaning
cleaned = cleaned.dropna(subset=['lifetime_value'])

print("\n9.3 Cleaned data:")
print(cleaned)

# Load
print("\n9.4 Load: Ready for database/warehouse")
print(f"Rows loaded: {len(cleaned)}")
print(f"Data quality: {(1 - cleaned.isnull().sum().sum() / (len(cleaned) * len(cleaned.columns))) * 100:.1f}%")

# ============================================================================
# 10. PROFILING & DEBUGGING
# ============================================================================
print("\n\n10. PROFILING & DEBUGGING")
print("-" * 60)

df_profile = pd.DataFrame({
    'A': range(100),
    'B': np.random.rand(100),
    'C': np.random.choice(['X', 'Y', 'Z'], 100)
})

print("\n10.1 Profile data:")
print(df_profile.describe())

print("\n10.2 Info:")
df_profile.info()

print("\n10.3 Head + tail (quick check):")
print("Head:")
print(df_profile.head(2))
print("\nTail:")
print(df_profile.tail(2))

print("\n10.4 Check specific rows:")
print(f"Row with max value in B:\n{df_profile[df_profile['B'] == df_profile['B'].max()]}")

# ============================================================================
# SUMMARY
# ============================================================================
print("\n\n" + "=" * 60)
print("SUMMARY - You've achieved MASTERY!")
print("=" * 60)
print("""
✓ Memory optimization (dtypes, sparse arrays)
✓ Processing large files (chunking)
✓ Vectorization (fast operations)
✓ Query method (efficient filtering)
✓ Method chaining (clean code)
✓ Data validation & quality checks
✓ Outlier detection
✓ Advanced groupby operations
✓ Production ETL pipelines
✓ Profiling and debugging
✓ Building robust data systems

YOU ARE NOW A PANDAS EXPERT!

Next steps:
- Apply these patterns in your data pipelines
- Use Pandas with Spark for data cleaning
- Build production ETL systems
- Help others learn Pandas!

Great job! 🎉
""")
