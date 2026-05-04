# Pandas: Complete Guide (Zero to Hero)

## Table of Contents

1. [Level 1: Fundamentals (Beginner)](#level-1-fundamentals-beginner)
2. [Level 2: Intermediate](#level-2-intermediate)
3. [Level 3: Advanced](#level-3-advanced)
4. [Level 4: Mastery (Expert)](#level-4-mastery-expert)
5. [Real-World Scenarios](#real-world-scenarios)
6. [Performance Optimization](#performance-optimization)

---

## LEVEL 1: Fundamentals (Beginner)

### 1.1 What is Pandas?

**Simple Definition**: Pandas is a **data manipulation library** that makes working with data easy.

**Real-World Analogy**:
```
Think of Pandas like Excel:
- Rows and columns (like spreadsheet)
- Functions to manipulate data
- Can read/write files
- Can filter, sort, group data
```

**Key Concepts**:
- **Series**: 1-dimensional data (like a column)
- **DataFrame**: 2-dimensional data (like a spreadsheet)
- **Index**: Labels for rows and columns

### 1.2 Creating Data

```python
import pandas as pd

# Create from dictionary
data = {
    'name': ['Alice', 'Bob', 'Charlie'],
    'age': [25, 30, 35],
    'salary': [50000, 60000, 75000]
}
df = pd.DataFrame(data)

# Create from lists
df = pd.DataFrame({
    'A': [1, 2, 3],
    'B': [4, 5, 6]
})

# Create from CSV
df = pd.read_csv('data.csv')

# Create from Excel
df = pd.read_excel('data.xlsx')

# Create from JSON
df = pd.read_json('data.json')
```

### 1.3 Basic Operations

```python
# View data
df.head()           # First 5 rows
df.tail()           # Last 5 rows
df.info()           # Data info
df.describe()       # Statistics

# Access data
df['name']          # Get column
df['name'][0]       # Get value
df.loc[0]           # Get row by index
df.iloc[0]          # Get row by position

# Basic stats
df.shape            # (rows, columns)
df.columns          # Column names
df.dtypes           # Data types
df['age'].mean()    # Average age
df['salary'].max()  # Max salary
```

### 1.4 Filtering Data

```python
# Filter rows
df[df['age'] > 25]          # Age greater than 25
df[df['name'] == 'Alice']   # Name equals Alice
df[(df['age'] > 25) & (df['salary'] > 55000)]  # Multiple conditions

# Filter columns
df[['name', 'age']]         # Select specific columns
df.drop('salary', axis=1)   # Remove column
```

### 1.5 Simple Example

```python
import pandas as pd

# Create sample data
data = {
    'product': ['Laptop', 'Phone', 'Tablet'],
    'price': [1000, 500, 300],
    'quantity': [5, 10, 15]
}

df = pd.DataFrame(data)

# Add new column
df['total'] = df['price'] * df['quantity']

# Filter expensive items
expensive = df[df['price'] > 400]

# Sort by price
sorted_df = df.sort_values('price', ascending=False)

print(df)
```

---

## LEVEL 2: Intermediate

### 2.1 Data Cleaning

```python
# Handle missing values
df.isnull()                    # Find nulls
df.fillna(0)                   # Fill with 0
df.dropna()                    # Remove nulls
df.interpolate()               # Interpolate values

# Handle duplicates
df.duplicated()                # Find duplicates
df.drop_duplicates()           # Remove duplicates

# Type conversion
df['age'] = df['age'].astype(int)
df['date'] = pd.to_datetime(df['date'])

# Rename columns
df.rename(columns={'old': 'new'})

# String operations
df['name'].str.upper()         # Uppercase
df['name'].str.lower()         # Lowercase
df['name'].str.strip()         # Remove spaces
```

### 2.2 GroupBy & Aggregation

```python
# Group by category
grouped = df.groupby('category')

# Sum by group
df.groupby('category')['sales'].sum()

# Multiple aggregations
df.groupby('category').agg({
    'sales': 'sum',
    'price': 'mean',
    'quantity': 'max'
})

# Custom aggregation
df.groupby('category').apply(lambda x: x['sales'].sum() / x['quantity'].sum())
```

### 2.3 Merging & Joining

```python
# Merge dataframes
df1 = pd.DataFrame({'key': [1, 2], 'A': ['a', 'b']})
df2 = pd.DataFrame({'key': [1, 2], 'B': ['x', 'y']})

# Inner join
merged = pd.merge(df1, df2, on='key')

# Left join
merged = pd.merge(df1, df2, on='key', how='left')

# Concatenate
df_combined = pd.concat([df1, df2])

# Append
df1.append(df2)
```

### 2.4 Sorting & Ranking

```python
# Sort by column
df.sort_values('age')
df.sort_values('age', ascending=False)

# Sort by multiple columns
df.sort_values(['category', 'price'])

# Ranking
df['rank'] = df['sales'].rank()

# Cumulative sum
df['cumsum'] = df['sales'].cumsum()
```

### 2.5 Pivoting

```python
# Pivot table
pivot = df.pivot_table(
    values='sales',
    index='category',
    columns='month',
    aggfunc='sum'
)

# Melt (unpivot)
melted = df.melt(
    id_vars=['product'],
    value_vars=['Q1', 'Q2', 'Q3'],
    var_name='quarter',
    value_name='sales'
)
```

---

## LEVEL 3: Advanced

### 3.1 Apply & Map

```python
# Apply function to column
df['age_group'] = df['age'].apply(lambda x: 'Young' if x < 30 else 'Old')

# Map values
df['category'] = df['type'].map({'A': 'Type A', 'B': 'Type B'})

# Apply to rows
df.apply(lambda row: row['salary'] * 1.1, axis=1)

# Custom functions
def categorize_age(age):
    if age < 20:
        return 'Teenager'
    elif age < 30:
        return 'Young Adult'
    else:
        return 'Adult'

df['age_category'] = df['age'].apply(categorize_age)
```

### 3.2 Time Series

```python
# Parse datetime
df['date'] = pd.to_datetime(df['date'])

# Extract components
df['year'] = df['date'].dt.year
df['month'] = df['date'].dt.month
df['day'] = df['date'].dt.day
df['weekday'] = df['date'].dt.day_name()

# Resample (change frequency)
df.set_index('date').resample('M').sum()  # Monthly sum

# Rolling window
df['sales_ma'] = df['sales'].rolling(window=7).mean()  # 7-day average

# Date range
dates = pd.date_range('2024-01-01', '2024-12-31', freq='D')
```

### 3.3 Window Functions

```python
# Rolling statistics
df['rolling_mean'] = df['sales'].rolling(window=3).mean()
df['rolling_std'] = df['sales'].rolling(window=3).std()

# Expanding window
df['expanding_sum'] = df['sales'].expanding().sum()
df['expanding_mean'] = df['sales'].expanding().mean()

# Lag/Lead
df['previous_sales'] = df['sales'].shift(1)
df['next_sales'] = df['sales'].shift(-1)
```

### 3.4 Categorical Data

```python
# Create categorical
df['category'] = pd.Categorical(df['type'], categories=['A', 'B', 'C'])

# Reduce memory
df['category'] = df['type'].astype('category')

# Operations
df['category'].cat.categories
df['category'].cat.rename_categories(['X', 'Y', 'Z'])
df['category'].cat.add_categories(['D'])
```

### 3.5 Multi-Index

```python
# Create multi-index
df.set_index(['year', 'month'])

# Access multi-index data
df.loc[(2024, 1)]
df.loc[(2024, slice(1, 3))]

# Unstack
df.unstack()

# Stack
df.stack()
```

---

## LEVEL 4: Mastery (Expert)

### 4.1 Performance Optimization

```python
# Use appropriate dtypes
df['age'] = df['age'].astype('int8')      # Smaller int
df['category'] = df['category'].astype('category')  # Categorical

# Use vectorization (not loops!)
# Bad:
result = []
for val in df['price']:
    result.append(val * 1.1)

# Good:
df['new_price'] = df['price'] * 1.1

# Chunking large files
for chunk in pd.read_csv('huge_file.csv', chunksize=10000):
    process(chunk)

# Query (faster filtering)
df.query('age > 25 and salary > 50000')
```

### 4.2 Custom Operations

```python
# Apply with multiple parameters
def calculate_bonus(row):
    if row['salary'] > 100000:
        return row['salary'] * 0.2
    else:
        return row['salary'] * 0.1

df['bonus'] = df.apply(calculate_bonus, axis=1)

# Using NumPy for speed
import numpy as np
df['log_price'] = np.log(df['price'])
df['price_zscore'] = (df['price'] - df['price'].mean()) / df['price'].std()

# Assign (chainable operations)
df_new = (df
    .assign(bonus=lambda x: x['salary'] * 0.1)
    .assign(total=lambda x: x['salary'] + x['bonus'])
    .query('total > 55000')
)
```

### 4.3 Memory Management

```python
# Check memory usage
df.memory_usage(deep=True)

# Optimize memory
df_optimized = df.copy()
for col in df_optimized.columns:
    col_type = df_optimized[col].dtype

    if col_type != 'object':
        c_min = df_optimized[col].min()
        c_max = df_optimized[col].max()

        if str(col_type)[:3] == 'int':
            if c_min > np.iinfo(np.int8).min and c_max < np.iinfo(np.int8).max:
                df_optimized[col] = df_optimized[col].astype(np.int8)

# Use sparse data for many zeros
df['sparse_col'] = pd.arrays.SparseArray(df['col'])
```

### 4.4 Advanced Grouping

```python
# Transform (keep same shape)
df['sales_pct'] = df.groupby('category')['sales'].transform(lambda x: x / x.sum())

# Ngroup (group number)
df['group_num'] = df.groupby('category').ngroup()

# Cumulative by group
df['rank_in_group'] = df.groupby('category')['sales'].rank()

# Filter groups
df.groupby('category').filter(lambda x: x['sales'].sum() > 1000)
```

### 4.5 Data Profiling & Validation

```python
# Data quality report
df.info()
df.describe()
df.isnull().sum()

# Duplicates
df[df.duplicated(subset=['id'], keep=False)]

# Outliers (Z-score)
from scipy import stats
df['zscore'] = np.abs(stats.zscore(df['age']))
outliers = df[df['zscore'] > 3]

# Data validation
assert df['age'].min() >= 0
assert df['age'].max() <= 150
assert df['email'].str.contains('@').all()
```

---

## Real-World Scenarios

### Scenario 1: Sales Analysis

```python
import pandas as pd

# Load data
df = pd.read_csv('sales.csv')

# Clean data
df = df.dropna()
df['date'] = pd.to_datetime(df['date'])
df['amount'] = df['amount'].astype(float)

# Analysis
# Total sales by month
monthly_sales = df.groupby(df['date'].dt.to_period('M'))['amount'].sum()

# Top products
top_products = df.groupby('product')['amount'].sum().nlargest(10)

# Sales trend
daily_sales = df.groupby(df['date'].dt.date)['amount'].sum()

# Customer analysis
customer_stats = df.groupby('customer').agg({
    'amount': ['sum', 'mean', 'count'],
    'date': ['min', 'max']
})
```

### Scenario 2: Data Cleaning

```python
# Handle missing values
df['age'] = df['age'].fillna(df['age'].median())
df['category'] = df['category'].fillna('Unknown')

# Remove duplicates
df = df.drop_duplicates(subset=['id'])

# Fix data types
df['phone'] = df['phone'].astype(str)
df['date'] = pd.to_datetime(df['date'], format='%d/%m/%Y')

# Standardize text
df['name'] = df['name'].str.strip().str.title()

# Handle outliers
Q1 = df['salary'].quantile(0.25)
Q3 = df['salary'].quantile(0.75)
IQR = Q3 - Q1
df = df[(df['salary'] >= Q1 - 1.5*IQR) & (df['salary'] <= Q3 + 1.5*IQR)]
```

### Scenario 3: Data Transformation

```python
# Normalize
df['age_normalized'] = (df['age'] - df['age'].min()) / (df['age'].max() - df['age'].min())

# Standardize
df['age_standardized'] = (df['age'] - df['age'].mean()) / df['age'].std()

# Binning
df['age_group'] = pd.cut(df['age'], bins=[0, 18, 35, 60, 100],
                         labels=['Child', 'Young', 'Adult', 'Senior'])

# Encoding
df['gender_encoded'] = df['gender'].map({'M': 0, 'F': 1})

# One-hot encoding
df = pd.get_dummies(df, columns=['category'])
```

---

## Performance Optimization

### Best Practices

```python
# ✅ DO: Use vectorized operations
df['result'] = df['a'] + df['b']

# ❌ DON'T: Use loops
# result = []
# for i in range(len(df)):
#     result.append(df['a'].iloc[i] + df['b'].iloc[i])

# ✅ DO: Use categorical for repeated strings
df['category'] = df['category'].astype('category')

# ✅ DO: Use query for complex filters
df.query('age > 25 and salary > 50000')

# ✅ DO: Use appropriate dtypes
df['small_int'] = df['small_int'].astype('int8')

# ✅ DO: Use copy() for new dataframe
df_copy = df.copy()

# ❌ DON'T: Use apply for simple operations
# df['result'] = df.apply(lambda row: row['a'] + row['b'], axis=1)
```

---

## Common Pitfalls

### 1. SettingWithCopyWarning
```python
# Bad
df[df['age'] > 25]['salary'] = 0  # Warning!

# Good
df.loc[df['age'] > 25, 'salary'] = 0
```

### 2. Chained Indexing
```python
# Bad
df['new_col'] = df[df['age'] > 25]['salary'] * 2

# Good
df.loc[df['age'] > 25, 'new_col'] = df.loc[df['age'] > 25, 'salary'] * 2
```

### 3. Modifying Original DataFrame
```python
# Bad (modifies original)
df['age'] = df['age'] + 1

# Good (keep original)
df_new = df.copy()
df_new['age'] = df_new['age'] + 1
```

---

## Summary by Level

| Level | Topics | Time |
|-------|--------|------|
| **1** | DataFrames, basic ops, filtering | 1 week |
| **2** | Cleaning, grouping, merging | 2 weeks |
| **3** | Advanced ops, time series | 2 weeks |
| **4** | Performance, optimization, mastery | 2 weeks |

**Total Time to Mastery**: ~7 weeks

---

**You're now ready to work with data like a professional!** 🎉
