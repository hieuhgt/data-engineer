"""
Level 3: Pandas Advanced - Runnable Examples
Run this with: python level3_advanced.py
"""

import pandas as pd
import numpy as np

print("=" * 60)
print("PANDAS LEVEL 3: ADVANCED")
print("=" * 60)

# ============================================================================
# 1. TIME SERIES FUNDAMENTALS
# ============================================================================
print("\n1. TIME SERIES FUNDAMENTALS")
print("-" * 60)

# Create time series data
dates = pd.date_range('2024-01-01', periods=30, freq='D')
ts_data = pd.DataFrame({
    'date': dates,
    'sales': np.random.randint(100, 500, 30),
    'quantity': np.random.randint(5, 30, 30)
})

print("\n1.1 Original time series data:")
print(ts_data.head())

print("\n1.2 Set date as index:")
ts_indexed = ts_data.set_index('date')
print(ts_indexed.head())

print("\n1.3 Extract date components:")
ts_indexed['year'] = ts_indexed.index.year
ts_indexed['month'] = ts_indexed.index.month
ts_indexed['day'] = ts_indexed.index.day
ts_indexed['weekday'] = ts_indexed.index.day_name()
print(ts_indexed[['sales', 'year', 'month', 'day', 'weekday']].head(10))

print("\n1.4 Filter by date range:")
start_date = '2024-01-10'
end_date = '2024-01-20'
filtered = ts_indexed.loc[start_date:end_date]
print(filtered)

# ============================================================================
# 2. RESAMPLING
# ============================================================================
print("\n\n2. RESAMPLING (CHANGE FREQUENCY)")
print("-" * 60)

print("\n2.1 Daily data (original):")
print(ts_indexed[['sales']].head())

print("\n2.2 Resample to weekly (sum):")
weekly = ts_indexed['sales'].resample('W').sum()
print(weekly)

print("\n2.3 Resample to monthly (mean):")
monthly = ts_indexed['sales'].resample('M').mean()
print(monthly)

print("\n2.4 Resample with multiple aggregations:")
result = ts_indexed['sales'].resample('W').agg(['sum', 'mean', 'count'])
print(result)

# ============================================================================
# 3. ROLLING WINDOWS
# ============================================================================
print("\n\n3. ROLLING WINDOWS")
print("-" * 60)

print("\n3.1 Original sales data:")
print(ts_indexed['sales'].head(10))

print("\n3.2 7-day rolling average:")
ts_indexed['sales_ma7'] = ts_indexed['sales'].rolling(window=7).mean()
print(ts_indexed[['sales', 'sales_ma7']].head(10))

print("\n3.3 7-day rolling standard deviation:")
ts_indexed['sales_std7'] = ts_indexed['sales'].rolling(window=7).std()
print(ts_indexed[['sales', 'sales_std7']].head(10))

print("\n3.4 7-day rolling max and min:")
ts_indexed['sales_max7'] = ts_indexed['sales'].rolling(window=7).max()
ts_indexed['sales_min7'] = ts_indexed['sales'].rolling(window=7).min()
print(ts_indexed[['sales', 'sales_max7', 'sales_min7']].head(10))

# ============================================================================
# 4. EXPANDING WINDOWS
# ============================================================================
print("\n\n4. EXPANDING WINDOWS (CUMULATIVE)")
print("-" * 60)

print("\n4.1 Original sales:")
print(ts_indexed['sales'].head(10))

print("\n4.2 Expanding sum (cumulative):")
ts_indexed['sales_cumsum'] = ts_indexed['sales'].cumsum()
print(ts_indexed[['sales', 'sales_cumsum']].head(10))

print("\n4.3 Expanding mean:")
ts_indexed['sales_cummean'] = ts_indexed['sales'].expanding().mean()
print(ts_indexed[['sales', 'sales_cummean']].head(10))

# ============================================================================
# 5. LAG & LEAD (PREVIOUS/NEXT VALUES)
# ============================================================================
print("\n\n5. LAG & LEAD (SHIFT VALUES)")
print("-" * 60)

print("\n5.1 Original values:")
print(ts_indexed['sales'].head(5))

print("\n5.2 Previous day's sales (lag):")
ts_indexed['sales_prev'] = ts_indexed['sales'].shift(1)
print(ts_indexed[['sales', 'sales_prev']].head(5))

print("\n5.3 Next day's sales (lead):")
ts_indexed['sales_next'] = ts_indexed['sales'].shift(-1)
print(ts_indexed[['sales', 'sales_next']].head(5))

print("\n5.4 Calculate day-over-day change:")
ts_indexed['sales_change'] = ts_indexed['sales'] - ts_indexed['sales_prev']
print(ts_indexed[['sales', 'sales_prev', 'sales_change']].head(5))

print("\n5.5 Calculate percent change:")
ts_indexed['sales_pct_change'] = ts_indexed['sales'].pct_change() * 100
print(ts_indexed[['sales', 'sales_pct_change']].head(5))

# ============================================================================
# 6. APPLY & TRANSFORM WITH FUNCTIONS
# ============================================================================
print("\n\n6. APPLY & TRANSFORM WITH FUNCTIONS")
print("-" * 60)

df_func = pd.DataFrame({
    'age': [22, 23, 21, 25, 30, 35, 40],
    'salary': [50000, 60000, 55000, 70000, 80000, 90000, 100000]
})

print("\n6.1 Apply function to Series (lambda):")
df_func['age_group'] = df_func['age'].apply(
    lambda x: 'Young' if x < 30 else 'Middle' if x < 40 else 'Senior'
)
print(df_func[['age', 'age_group']])

print("\n6.2 Apply custom function:")
def categorize_salary(salary):
    if salary < 60000:
        return 'Entry'
    elif salary < 80000:
        return 'Mid'
    else:
        return 'Senior'

df_func['salary_level'] = df_func['salary'].apply(categorize_salary)
print(df_func[['salary', 'salary_level']])

print("\n6.3 Apply to entire rows (axis=1):")
df_func['bonus'] = df_func.apply(
    lambda row: row['salary'] * 0.15 if row['age'] > 30 else row['salary'] * 0.10,
    axis=1
)
print(df_func[['age', 'salary', 'bonus']])

print("\n6.4 Transform (keeps shape) - percentage of total:")
df_sales2 = pd.DataFrame({
    'region': ['North', 'North', 'South', 'South', 'East', 'East'],
    'sales': [1000, 1200, 800, 900, 1100, 1000]
})
df_sales2['pct_of_region'] = df_sales2.groupby('region')['sales'].transform(
    lambda x: (x / x.sum() * 100).round(1)
)
print(df_sales2)

# ============================================================================
# 7. RANKING
# ============================================================================
print("\n\n7. RANKING")
print("-" * 60)

df_rank = pd.DataFrame({
    'student': ['Alice', 'Bob', 'Charlie', 'Diana', 'Eve'],
    'grade': [85, 90, 85, 95, 90]
})

print("\n7.1 Original grades:")
print(df_rank)

print("\n7.2 Rank (1st, 2nd, 3rd...):")
df_rank['rank'] = df_rank['grade'].rank()
print(df_rank)

print("\n7.3 Rank dense (no gaps for ties):")
df_rank['rank_dense'] = df_rank['grade'].rank(method='dense')
print(df_rank)

print("\n7.4 Rank descending (highest grade = 1):")
df_rank['rank_desc'] = df_rank['grade'].rank(ascending=False)
print(df_rank)

# ============================================================================
# 8. GROUPBY TRANSFORM & FILTER
# ============================================================================
print("\n\n8. GROUPBY TRANSFORM & FILTER")
print("-" * 60)

df_group = pd.DataFrame({
    'department': ['Sales', 'Sales', 'IT', 'IT', 'HR', 'HR', 'Sales'],
    'employee': ['Alice', 'Bob', 'Charlie', 'Diana', 'Eve', 'Frank', 'Grace'],
    'salary': [50000, 55000, 70000, 75000, 52000, 48000, 60000]
})

print("\n8.1 Original data:")
print(df_group)

print("\n8.2 Get percent of department total:")
df_group['pct_of_dept'] = df_group.groupby('department')['salary'].transform(
    lambda x: (x / x.sum() * 100).round(1)
)
print(df_group)

print("\n8.3 Get zscore (deviation from dept mean):")
df_group['salary_zscore'] = df_group.groupby('department')['salary'].transform(
    lambda x: (x - x.mean()) / x.std()
)
print(df_group[['employee', 'salary', 'salary_zscore']])

print("\n8.4 Filter: Keep only large departments (>2 employees):")
result = df_group.groupby('department').filter(lambda x: len(x) > 2)
print(result)

print("\n8.5 Filter: Keep only high-salary employees in their dept:")
result = df_group.groupby('department').filter(
    lambda x: (x['salary'] > x['salary'].median()).any()
)
print(result)

# ============================================================================
# 9. CATEGORICAL DATA
# ============================================================================
print("\n\n9. CATEGORICAL DATA")
print("-" * 60)

df_cat = pd.DataFrame({
    'color': ['red', 'blue', 'green', 'red', 'blue', 'green'],
    'size': ['S', 'M', 'L', 'M', 'S', 'L'],
    'count': [10, 20, 15, 25, 30, 18]
})

print("\n9.1 Convert to categorical (saves memory):")
df_cat['color'] = df_cat['color'].astype('category')
df_cat['size'] = df_cat['size'].astype('category')
print(f"Data types:\n{df_cat.dtypes}")

print("\n9.2 Check memory usage:")
df_before = pd.DataFrame({'color': ['red', 'blue'] * 1000})
df_after = df_before.copy()
df_after['color'] = df_after['color'].astype('category')
print(f"Before: {df_before.memory_usage(deep=True).sum()} bytes")
print(f"After: {df_after.memory_usage(deep=True).sum()} bytes")

print("\n9.3 Categorical operations:")
print(f"Categories: {df_cat['color'].cat.categories.tolist()}")
print(f"Category count: {df_cat['color'].cat.codes}")

# ============================================================================
# 10. MULTI-INDEX
# ============================================================================
print("\n\n10. MULTI-INDEX")
print("-" * 60)

df_multi = pd.DataFrame({
    'year': [2023, 2023, 2023, 2024, 2024, 2024],
    'quarter': ['Q1', 'Q2', 'Q3', 'Q1', 'Q2', 'Q3'],
    'region': ['North', 'South', 'East', 'North', 'South', 'East'],
    'sales': [100, 120, 110, 150, 140, 130]
})

print("\n10.1 Create multi-index:")
df_mi = df_multi.set_index(['year', 'quarter', 'region'])
print(df_mi)

print("\n10.2 Access by level:")
print("\nAll sales for 2024:")
print(df_mi.loc[2024])

print("\nAll sales for 2024 Q1:")
print(df_mi.loc[(2024, 'Q1')])

print("\n10.3 Unstack (pivot):")
unstacked = df_mi.unstack()
print(unstacked)

print("\n10.4 Stack (unpivot):")
stacked = unstacked.stack()
print(stacked.head(10))

# ============================================================================
# 11. REAL-WORLD EXAMPLE: TIME SERIES ANALYSIS
# ============================================================================
print("\n\n11. REAL-WORLD EXAMPLE: STOCK PRICE ANALYSIS")
print("-" * 60)

np.random.seed(42)
stock_data = pd.DataFrame({
    'date': pd.date_range('2024-01-01', periods=60, freq='D'),
    'AAPL': 150 + np.cumsum(np.random.randn(60) * 2),
    'GOOGL': 100 + np.cumsum(np.random.randn(60) * 1.5),
    'MSFT': 300 + np.cumsum(np.random.randn(60) * 2.5)
})

stock_data.set_index('date', inplace=True)

print("\n11.1 Original closing prices:")
print(stock_data.head())

print("\n11.2 Calculate daily returns (%):")
returns = stock_data.pct_change() * 100
print(returns.head())

print("\n11.3 20-day moving average:")
ma20 = stock_data.rolling(window=20).mean()
print(ma20.tail())

print("\n11.4 Volatility (20-day std):")
volatility = stock_data.rolling(window=20).std()
print(volatility.tail())

print("\n11.5 Cumulative returns (%):")
cum_returns = (1 + returns).cumprod() * 100 - 100
print(cum_returns.tail())

print("\n11.6 Rank stocks by performance:")
performance = cum_returns.iloc[-1].sort_values(ascending=False)
print(performance)

# ============================================================================
# SUMMARY
# ============================================================================
print("\n\n" + "=" * 60)
print("SUMMARY - You've learned:")
print("=" * 60)
print("""
✓ Time series indexing and operations
✓ Resampling (change frequency)
✓ Rolling windows (moving averages)
✓ Expanding windows (cumulative)
✓ Lag and lead (shift operations)
✓ Apply and transform functions
✓ Ranking and percentile operations
✓ GroupBy transform and filter
✓ Categorical data (memory efficient)
✓ Multi-index DataFrames
✓ Real-world financial analysis

Next: Check out Level 4 for production patterns and optimization!
""")
