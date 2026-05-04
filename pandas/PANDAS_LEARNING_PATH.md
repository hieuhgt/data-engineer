# Pandas Learning Path - 20 Week Structured Journey

## Overview

This is a **professional 20-week learning roadmap** to master Pandas from zero to expert level. Each week has specific goals, practice exercises, and real-world projects.

**Recommended Pace**: 5-7 hours per week
**Total Time**: 20 weeks to proficiency
**Prerequisite**: Basic Python knowledge

---

## Quick Navigation

- **Weeks 1-5**: Level 1 - Fundamentals (Beginner)
- **Weeks 6-10**: Level 2 - Intermediate (Growing Professional)
- **Weeks 11-15**: Level 3 - Advanced (Senior Developer)
- **Weeks 16-20**: Level 4 - Mastery (Expert)

---

## WEEKS 1-5: LEVEL 1 - FUNDAMENTALS (BEGINNER)

### Week 1: Setup & Series Basics
**Goal**: Get comfortable with Pandas environment and Series concept

**Topics**:
- Installation and environment setup
- What is a Series? (1D data)
- Creating Series from lists, dicts, ranges
- Series attributes (index, values, dtype)
- Basic indexing (.loc, .iloc)

**Practice**:
```python
# Task 1: Create a Series of your 5 favorite movies and their ratings
movies = pd.Series([8.5, 9.0, 7.5, 8.0, 9.5],
                    index=['Inception', 'Avatar', 'Interstellar', 'Titanic', 'Parasite'])

# Task 2: Access specific movie ratings
print(movies['Inception'])    # By label
print(movies.iloc[0])         # By position

# Task 3: Perform basic math
print(movies.mean())
print(movies[movies > 8.0])
```

**Milestone**: Can create Series and access data comfortably

---

### Week 2: DataFrames & Basic Operations
**Goal**: Master DataFrame creation and basic operations

**Topics**:
- What is a DataFrame? (2D data like Excel)
- Creating DataFrames from dictionaries, lists, files
- Viewing data (.head(), .tail(), .info(), .describe())
- Column and row access
- Basic statistics

**Practice**:
```python
# Task 1: Create a student DataFrame
students = pd.DataFrame({
    'name': ['Alice', 'Bob', 'Charlie', 'Diana'],
    'age': [22, 23, 21, 22],
    'grade': [85, 90, 78, 88],
    'major': ['CS', 'Math', 'Physics', 'CS']
})

# Task 2: Explore the data
print(students.shape)
print(students.dtypes)
print(students.describe())

# Task 3: Get specific columns
print(students['name'])
print(students[['name', 'grade']])
```

**Milestone**: Can create DataFrames and explore their structure

---

### Week 3: Reading & Writing Files
**Goal**: Work with real data files

**Topics**:
- Reading CSV files
- Reading Excel files
- Reading JSON
- Writing to different formats
- Handling file paths
- Basic error handling

**Practice**:
```python
# Task 1: Create and read a CSV
import csv

data = [
    {'product': 'Laptop', 'price': 1000, 'quantity': 5},
    {'product': 'Phone', 'price': 500, 'quantity': 10},
    {'product': 'Tablet', 'price': 300, 'quantity': 15}
]

# Write to CSV
df = pd.DataFrame(data)
df.to_csv('products.csv', index=False)

# Read it back
df_read = pd.read_csv('products.csv')

# Task 2: Read first 10 rows only
df_sample = pd.read_csv('products.csv', nrows=10)

# Task 3: Specify data types while reading
df = pd.read_csv('products.csv', dtype={'price': float, 'quantity': int})
```

**Milestone**: Can read and write multiple file formats

---

### Week 4: Filtering & Selection
**Goal**: Extract data you need using filtering

**Topics**:
- Boolean indexing
- Multiple conditions (and/or)
- .loc and .iloc for complex selection
- .query() method
- .isin() for membership testing
- .between() for range selection

**Practice**:
```python
# Task 1: Simple filtering
students = pd.DataFrame({
    'name': ['Alice', 'Bob', 'Charlie', 'Diana'],
    'age': [22, 23, 21, 22],
    'grade': [85, 90, 78, 88]
})

above_80 = students[students['grade'] >= 80]

# Task 2: Multiple conditions
over_21_high_grade = students[(students['age'] > 21) & (students['grade'] > 85)]

# Task 3: Using .query()
result = students.query('age > 21 and grade >= 80')

# Task 4: Using .isin()
cs_students = students[students['major'].isin(['CS', 'Math'])]
```

**Milestone**: Can filter data in multiple ways

---

### Week 5: Level 1 Capstone Project
**Goal**: Apply all Level 1 skills to real scenario

**Project**: Sales Data Analysis
```python
# Create sample sales data
sales = pd.DataFrame({
    'date': pd.date_range('2024-01-01', periods=30),
    'product': ['Laptop', 'Phone', 'Tablet'] * 10,
    'quantity': [1, 2, 3, 1, 2, 3, 1, 2, 3, 1, 2, 3, 1, 2, 3, 1, 2, 3, 1, 2, 3, 1, 2, 3, 1, 2, 3, 1, 2, 3],
    'price': [1000, 500, 300] * 10,
    'region': ['North', 'South', 'East'] * 10
})

# Tasks:
# 1. Total sales by product
# 2. Average price per region
# 3. Quantities > 2
# 4. Sales in first 10 days
# 5. Laptops sold in North region

print("✓ Week 5 Complete - You can now analyze real data!")
```

---

## WEEKS 6-10: LEVEL 2 - INTERMEDIATE (GROWING PROFESSIONAL)

### Week 6: Data Cleaning Part 1
**Goal**: Handle missing data and duplicates

**Topics**:
- Identifying missing values (.isnull(), .notnull())
- Handling nulls (drop, fill, interpolate)
- Finding duplicates (.duplicated())
- Removing duplicates (.drop_duplicates())
- Data validation basics

**Practice**:
```python
# Task 1: Create data with missing values
data = pd.DataFrame({
    'name': ['Alice', 'Bob', None, 'Diana'],
    'age': [22, None, 21, 22],
    'salary': [50000, 60000, None, 55000]
})

# Check nulls
print(data.isnull())
print(data.isnull().sum())

# Task 2: Fill missing values
data['age'].fillna(data['age'].mean())
data['name'].fillna('Unknown')

# Task 3: Drop rows with missing values
data.dropna()
```

**Milestone**: Can handle missing data professionally

---

### Week 7: Data Cleaning Part 2 & Type Conversion
**Goal**: Type conversion and data standardization

**Topics**:
- Converting data types (.astype())
- Working with dates (.to_datetime())
- String operations (.str)
- Renaming columns
- Mapping values
- Standardizing text

**Practice**:
```python
# Task 1: Type conversion
df = pd.DataFrame({
    'age': ['22', '23', '21'],  # Strings
    'salary': ['50000', '60000', '55000']
})

df['age'] = df['age'].astype(int)
df['salary'] = df['salary'].astype(float)

# Task 2: Date conversion
df['date'] = pd.to_datetime(df['date'])
df['year'] = df['date'].dt.year
df['month'] = df['date'].dt.month

# Task 3: String operations
df['name'] = df['name'].str.strip().str.title()
df['email'] = df['email'].str.lower()

# Task 4: Mapping values
df['gender'] = df['gender'].map({'M': 'Male', 'F': 'Female'})
```

**Milestone**: Data is clean and properly typed

---

### Week 8: GroupBy & Aggregation
**Goal**: Summarize data by groups

**Topics**:
- .groupby() basics
- Single and multiple grouping
- Aggregation functions (.sum(), .mean(), .count())
- .agg() with multiple functions
- Named aggregations
- Custom aggregation with .apply()

**Practice**:
```python
# Task 1: Simple groupby
df = pd.DataFrame({
    'department': ['Sales', 'Sales', 'IT', 'IT', 'HR'],
    'salary': [50000, 55000, 60000, 65000, 52000],
    'year': [2023, 2024, 2023, 2024, 2023]
})

by_dept = df.groupby('department')['salary'].sum()

# Task 2: Multiple aggregations
summary = df.groupby('department').agg({
    'salary': ['sum', 'mean', 'count'],
    'year': 'max'
})

# Task 3: Custom aggregation
def salary_range(x):
    return x.max() - x.min()

df.groupby('department')['salary'].apply(salary_range)
```

**Milestone**: Can summarize data by groups

---

### Week 9: Merging & Joining
**Goal**: Combine multiple DataFrames

**Topics**:
- .merge() with different join types
- .concat() for combining
- .join() for index-based joining
- Handling duplicate column names
- Many-to-many relationships

**Practice**:
```python
# Task 1: Create two dataframes
employees = pd.DataFrame({
    'emp_id': [1, 2, 3],
    'name': ['Alice', 'Bob', 'Charlie'],
    'dept_id': [1, 2, 1]
})

departments = pd.DataFrame({
    'dept_id': [1, 2],
    'dept_name': ['Sales', 'IT']
})

# Task 2: Inner join
result = pd.merge(employees, departments, on='dept_id')

# Task 3: Left join
result = pd.merge(employees, departments, on='dept_id', how='left')

# Task 4: Concatenate
combined = pd.concat([employees, employees])
```

**Milestone**: Can combine data from multiple sources

---

### Week 10: Level 2 Capstone Project
**Goal**: Complete data cleaning and analysis pipeline

**Project**: Customer Data Analysis
```python
# Real-world scenario: Customer dataset with missing data, duplicates, messy text
customers = pd.DataFrame({
    'customer_id': [1, 2, 2, 3, 4, None],  # Duplicate, null
    'name': ['  Alice  ', 'bob', 'BOB', 'Charlie', 'DIANA', 'Eve'],  # Messy text
    'email': ['alice@email.com', None, 'bob@email.com', 'charlie@email', 'diana@email.com', 'eve@email.com'],  # Nulls
    'purchase_date': ['2024-01-01', '2024-01-02', '2024-01-02', '2024-13-01', '2024-01-05', '2024-01-06'],
    'amount': ['100.50', '200.00', '200', '300.75', None, '150']
})

# Tasks:
# 1. Remove duplicates (keep first)
# 2. Handle missing values
# 3. Clean and standardize text
# 4. Convert dates properly
# 5. Convert amounts to float
# 6. Group by customer and get total spent
# 7. Verify data quality

print("✓ Week 10 Complete - You can clean real datasets!")
```

---

## WEEKS 11-15: LEVEL 3 - ADVANCED (SENIOR DEVELOPER)

### Week 11: Time Series Fundamentals
**Goal**: Work with time-based data

**Topics**:
- DateTime index
- Date/time extraction
- Resampling (changing frequency)
- Shifting and leading/lagging
- Rolling windows
- Time-based filtering

**Practice**:
```python
# Task 1: Create time series data
dates = pd.date_range('2024-01-01', periods=100, freq='D')
data = pd.DataFrame({
    'date': dates,
    'sales': np.random.randint(100, 1000, 100)
})

data.set_index('date', inplace=True)

# Task 2: Resampling
monthly_sales = data.resample('M').sum()  # Monthly
quarterly_sales = data.resample('Q').mean()  # Quarterly

# Task 3: Rolling average
data['sales_ma'] = data['sales'].rolling(window=7).mean()

# Task 4: Previous/next values
data['prev_sales'] = data['sales'].shift(1)
data['next_sales'] = data['sales'].shift(-1)
```

**Milestone**: Can work with time-series data effectively

---

### Week 12: Window Functions & Cumulative Operations
**Goal**: Advanced row-by-row calculations

**Topics**:
- Rolling statistics
- Expanding windows
- Cumulative operations
- Rank and dense rank
- Percent change
- Lag and lead functions

**Practice**:
```python
# Task 1: Expanding calculations
df['cumsum'] = df['sales'].expanding().sum()
df['expanding_mean'] = df['sales'].expanding().mean()

# Task 2: Rolling statistics
df['rolling_std'] = df['sales'].rolling(window=3).std()
df['rolling_max'] = df['sales'].rolling(window=5).max()

# Task 3: Ranking
df['rank'] = df['sales'].rank()
df['dense_rank'] = df['sales'].rank(method='dense')

# Task 4: Percent change
df['pct_change'] = df['sales'].pct_change()
```

**Milestone**: Can perform advanced window calculations

---

### Week 13: Apply, Transform & Custom Functions
**Goal**: Apply custom logic to data

**Topics**:
- .apply() on Series and DataFrames
- .transform() for keeping shape
- .map() for mapping values
- Lambda functions
- Row-wise operations
- Vectorization vs loops

**Practice**:
```python
# Task 1: Apply to Series with lambda
df['age_group'] = df['age'].apply(lambda x: 'Young' if x < 30 else 'Old')

# Task 2: Apply custom function
def categorize_salary(salary):
    if salary < 50000:
        return 'Entry'
    elif salary < 100000:
        return 'Mid'
    else:
        return 'Senior'

df['salary_level'] = df['salary'].apply(categorize_salary)

# Task 3: Apply to rows
df['bonus'] = df.apply(lambda row: row['salary'] * 0.1 if row['department'] == 'Sales' else row['salary'] * 0.05, axis=1)

# Task 4: Transform (keeps shape)
df['sales_pct'] = df.groupby('region')['sales'].transform(lambda x: x / x.sum())
```

**Milestone**: Can apply custom logic efficiently

---

### Week 14: Categorical Data & Multi-Index
**Goal**: Handle categorical data and complex indexes

**Topics**:
- Categorical dtypes (memory efficient)
- Category operations
- Multi-index DataFrames
- Stacking and unstacking
- Pivot operations

**Practice**:
```python
# Task 1: Categorical data
df['color'] = pd.Categorical(df['color'], categories=['Red', 'Blue', 'Green'])
df['color'] = df['color'].astype('category')

# Task 2: Category operations
print(df['color'].cat.categories)
df['color'] = df['color'].cat.rename_categories(['R', 'B', 'G'])

# Task 3: Multi-index
df_multi = df.set_index(['year', 'month', 'day'])
df_multi.loc[(2024, 1)]

# Task 4: Pivot operations
pivot = df.pivot_table(values='sales', index='region', columns='month', aggfunc='sum')
melted = df.melt(id_vars=['date'], value_vars=['A', 'B', 'C'])
```

**Milestone**: Can work with complex data structures

---

### Week 15: Level 3 Capstone Project
**Goal**: Integrate all advanced skills

**Project**: Financial Time Series Analysis
```python
# Real-world scenario: Stock price data analysis
dates = pd.date_range('2023-01-01', periods=365, freq='D')
prices = pd.DataFrame({
    'date': dates,
    'AAPL': np.random.randn(365).cumsum() + 150,
    'GOOGL': np.random.randn(365).cumsum() + 100,
    'MSFT': np.random.randn(365).cumsum() + 300
})

prices.set_index('date', inplace=True)

# Tasks:
# 1. Calculate daily returns
# 2. Rolling 30-day average
# 3. Volatility (rolling std)
# 4. Cumulative returns
# 5. Rank stocks by performance
# 6. Reample to monthly data
# 7. Categorize returns (positive/negative)
# 8. Find best performing days

print("✓ Week 15 Complete - You can analyze complex time series!")
```

---

## WEEKS 16-20: LEVEL 4 - MASTERY (EXPERT)

### Week 16: Performance Optimization
**Goal**: Work with large datasets efficiently

**Topics**:
- Data type optimization
- Memory profiling
- Vectorization over loops
- Chunking large files
- Sparse data
- Query optimization

**Practice**:
```python
# Task 1: Optimize dtypes
def optimize_dtypes(df):
    for col in df.columns:
        col_type = df[col].dtype
        if col_type == 'int64':
            df[col] = df[col].astype('int32')  # or int8, int16
        elif col_type == 'float64':
            df[col] = df[col].astype('float32')
        elif col_type == 'object':
            df[col] = df[col].astype('category')
    return df

# Task 2: Process large files in chunks
for chunk in pd.read_csv('huge_file.csv', chunksize=10000):
    process(chunk)

# Task 3: Use vectorization, not loops
# Bad:
results = []
for val in df['price']:
    results.append(val * 1.1)

# Good:
df['new_price'] = df['price'] * 1.1

# Task 4: Query method (faster filtering)
expensive = df.query('price > 1000 and quantity < 10')
```

**Milestone**: Can optimize for large-scale data processing

---

### Week 17: Advanced Grouping & Windowing
**Goal**: Master complex data aggregations

**Topics**:
- GroupBy transform patterns
- Filtering groups
- Multiple aggregations
- Custom group operations
- Ngroup() and group_number()
- Advanced window functions

**Practice**:
```python
# Task 1: Transform by group (percentage of total)
df['pct_of_group'] = df.groupby('region')['sales'].transform(lambda x: x / x.sum() * 100)

# Task 2: Filter groups (keep only large groups)
large_groups = df.groupby('category').filter(lambda x: len(x) > 10)

# Task 3: Multiple aggregations
summary = df.groupby(['region', 'product']).agg(
    total_sales=('amount', 'sum'),
    avg_price=('price', 'mean'),
    num_transactions=('amount', 'count'),
    max_quantity=('quantity', 'max')
)

# Task 4: Cumulative ranking within groups
df['rank_in_region'] = df.groupby('region')['sales'].rank()
df['group_number'] = df.groupby('category').ngroup()
```

**Milestone**: Can handle complex multi-dimensional aggregations

---

### Week 18: Data Validation & Quality Assurance
**Goal**: Ensure data integrity

**Topics**:
- Data profiling
- Outlier detection
- Data validation rules
- Consistency checks
- Automated quality checks
- Asserting expectations

**Practice**:
```python
# Task 1: Data profiling
def profile_data(df):
    print(df.info())
    print(df.describe())
    print("\nNull values:")
    print(df.isnull().sum())
    print("\nDuplicates:")
    print(df.duplicated().sum())

# Task 2: Outlier detection (Z-score)
from scipy import stats
df['z_score'] = np.abs(stats.zscore(df['salary']))
outliers = df[df['z_score'] > 3]

# Task 3: Validation rules
def validate_data(df):
    assert df['age'].min() >= 0, "Age cannot be negative"
    assert df['age'].max() <= 150, "Age seems unrealistic"
    assert df['salary'].min() >= 0, "Salary cannot be negative"
    assert df['email'].str.contains('@').all(), "Invalid emails found"
    return True

# Task 4: Data quality report
def quality_report(df):
    report = {
        'total_rows': len(df),
        'total_cols': len(df.columns),
        'null_pct': (df.isnull().sum() / len(df) * 100).round(2),
        'duplicate_rows': df.duplicated().sum(),
        'dtypes': df.dtypes.value_counts().to_dict()
    }
    return report
```

**Milestone**: Can validate and ensure data quality

---

### Week 19: Real-World Data Engineering
**Goal**: Handle realistic data scenarios

**Topics**:
- Handling messy real data
- Advanced cleaning strategies
- Data integration from multiple sources
- Complex transformations
- Error handling
- Logging and monitoring

**Practice**:
```python
# Real-world pipeline example
def etl_pipeline(input_file):
    # Extract
    df = pd.read_csv(input_file)

    # Transform
    df['date'] = pd.to_datetime(df['date'], format='%d/%m/%Y')
    df['amount'] = df['amount'].str.replace('$', '').astype(float)
    df['name'] = df['name'].str.strip().str.title()
    df = df.dropna(subset=['id'])
    df = df.drop_duplicates(subset=['id'])

    # Validate
    assert (df['amount'] > 0).all(), "Found negative amounts"
    assert df['date'].max() <= pd.Timestamp.now(), "Future dates found"

    # Load
    df.to_parquet('output.parquet', compression='snappy')

    return df
```

**Milestone**: Can handle real, messy data professionally

---

### Week 20: Level 4 Capstone Project & Mastery
**Goal**: Demonstrate complete Pandas mastery

**Project**: Multi-Stage ETL Pipeline
```python
# Complete real-world scenario: E-commerce data pipeline

# Source 1: Orders
orders = pd.DataFrame({...})  # Messy, missing values, wrong types

# Source 2: Customers
customers = pd.DataFrame({...})  # Duplicates, text standardization needed

# Source 3: Products
products = pd.DataFrame({...})  # Clean data source

# Tasks:
# 1. Clean orders (handle nulls, dates, amounts)
# 2. Deduplicate customers, standardize names
# 3. Merge all three sources
# 4. Calculate customer metrics (total spent, purchase frequency, etc.)
# 5. Identify high-value customers
# 6. Group by region and product
# 7. Detect anomalies
# 8. Create summary reports
# 9. Optimize for 10M+ rows
# 10. Validate output quality
# 11. Create audit logs
# 12. Export in multiple formats

print("✓ Week 20 Complete - YOU ARE A PANDAS EXPERT! 🎉")
```

**Final Skills You Have**:
- ✅ Data cleaning and preparation
- ✅ Complex transformations
- ✅ Time series analysis
- ✅ Performance optimization
- ✅ Data validation and quality assurance
- ✅ Real-world ETL pipelines
- ✅ Large dataset handling
- ✅ Professional data engineering

---

## Practice Projects Summary

### Level 1 (Weeks 1-5)
- Sales data analysis
- Student records filtering
- File I/O operations

### Level 2 (Weeks 6-10)
- Customer data cleaning
- Sales aggregation
- Department summaries

### Level 3 (Weeks 11-15)
- Stock price analysis
- Time series trends
- Complex ranking

### Level 4 (Weeks 16-20)
- Multi-source ETL pipeline
- Large dataset optimization
- Real-world data quality

---

## Resources

### Essential Files
- `PANDAS_COMPLETE_GUIDE.md` - Detailed reference (all 4 levels)
- `examples/` - Runnable code examples
- `README.md` - Quick start guide

### External Resources
- [Official Pandas Docs](https://pandas.pydata.org/docs/)
- [Stack Overflow Pandas Tag](https://stackoverflow.com/questions/tagged/pandas)
- [Real datasets for practice](https://www.kaggle.com/datasets)

---

## Milestones & Checkpoints

### Week 5 Milestone ✓
You can: Create DataFrames, load files, filter data, compute basic stats

### Week 10 Milestone ✓
You can: Clean data, merge sources, group and aggregate, handle null values

### Week 15 Milestone ✓
You can: Time series analysis, window functions, custom operations, complex indexes

### Week 20 Milestone ✓
You can: Build production ETL pipelines, optimize performance, ensure data quality

---

## Pro Tips

1. **Practice with real data** - Kaggle, government datasets, or your own
2. **Use `.head()` frequently** - Check your progress constantly
3. **Document transformations** - Add comments explaining each step
4. **Test edge cases** - What if data is empty? Has nulls? Wrong type?
5. **Measure performance** - Use `.memory_usage()` and `.time` for optimization
6. **Read error messages carefully** - They tell you exactly what's wrong
7. **Use `.copy()` to avoid warnings** - Prevents SettingWithCopyWarning
8. **Vectorize everything** - Loops are slow, use Pandas operations

---

## Common Mistakes to Avoid

❌ **Don't**: Chain operations without understanding each step
✅ **Do**: Break into steps, check `.head()` between each

❌ **Don't**: Ignore dtypes - they waste memory
✅ **Do**: Use `.astype()` and optimize early

❌ **Don't**: Use loops for transformations
✅ **Do**: Use vectorized operations

❌ **Don't**: Forget to handle missing values
✅ **Do**: Check for nulls first thing

---

## Progress Tracking

**Weeks 1-5: Foundation**
- [ ] Understand Series and DataFrames
- [ ] Can read/write multiple formats
- [ ] Can filter and select data
- [ ] Completed Week 5 project

**Weeks 6-10: Intermediate**
- [ ] Can clean real data
- [ ] Can merge multiple sources
- [ ] GroupBy aggregations comfortable
- [ ] Completed Week 10 project

**Weeks 11-15: Advanced**
- [ ] Time series operations smooth
- [ ] Window functions natural
- [ ] Custom functions applied easily
- [ ] Completed Week 15 project

**Weeks 16-20: Expert**
- [ ] Optimize large datasets instinctively
- [ ] Build ETL pipelines quickly
- [ ] Validate data quality automatically
- [ ] Completed Week 20 capstone

---

## Congratulations! 🎉

After 20 weeks, you'll be able to:
- Handle any data cleaning task
- Build production ETL pipelines
- Optimize for performance
- Work with millions of rows
- Ensure data quality
- Be a professional data engineer

**The learning doesn't stop here** - keep practicing, explore new datasets, and tackle real-world problems!

Good luck! 🚀
