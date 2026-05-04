# Pandas Learning Materials

Welcome to your **comprehensive Pandas learning journey** - from zero to expert level!

## 📚 What's Here

This folder contains everything you need to master Pandas:

### 1. **PANDAS_COMPLETE_GUIDE.md** (Reference)
The complete reference guide covering all 4 levels:
- **Level 1**: Fundamentals (Series, DataFrames, basic operations) - 1 week
- **Level 2**: Intermediate (Cleaning, GroupBy, Merging) - 2 weeks
- **Level 3**: Advanced (Time series, Window functions, Custom operations) - 2 weeks
- **Level 4**: Mastery (Optimization, Memory management, Production patterns) - 2 weeks

**Use this when**: You need to understand a specific concept, see examples, or need a reference while coding.

### 2. **PANDAS_LEARNING_PATH.md** (Structured Learning)
Your **20-week structured learning roadmap**:
- Week-by-week goals and topics
- Practice exercises for each week
- Real-world capstone projects
- Progress checkpoints and milestones
- Pro tips and common mistakes

**Use this when**: You want a guided learning schedule, or need to know what to focus on each week.

### 3. **examples/** (Runnable Code)
Practical code examples for each level:
- `level1_fundamentals.py` - Series, DataFrames, basic operations
- `level2_intermediate.py` - Data cleaning, groupby, merging
- `level3_advanced.py` - Time series, window functions
- `level4_mastery.py` - Performance optimization, production patterns

**Use this when**: You want to see working code and run it yourself.

---

## 🚀 Quick Start

### Option 1: Learning From Scratch (Recommended)
```bash
# Follow the 20-week path
cat PANDAS_LEARNING_PATH.md

# Week 1: Start with fundamentals
python examples/level1_fundamentals.py

# Reference the complete guide as needed
cat PANDAS_COMPLETE_GUIDE.md
```

### Option 2: Quick Reference
```bash
# Already familiar with Pandas? Jump to specific sections
grep -A 20 "Level 3: Advanced" PANDAS_COMPLETE_GUIDE.md
```

### Option 3: Project-Based Learning
```bash
# Jump to capstone projects in PANDAS_LEARNING_PATH.md
# Week 5, 10, 15, 20 have real-world projects

# Start with a project that interests you
python examples/level2_intermediate.py  # See data cleaning in action
```

---

## 📖 Materials Overview

| Material | Duration | Best For | Start With |
|----------|----------|----------|-----------|
| Complete Guide | Reference | Understanding concepts | Whenever you need to learn something |
| Learning Path | 20 weeks | Structured progression | Week 1 first |
| Level 1 Examples | 1 week | Hands-on practice | level1_fundamentals.py |
| Level 2 Examples | 2 weeks | Real data scenarios | level2_intermediate.py |
| Level 3 Examples | 2 weeks | Complex analysis | level3_advanced.py |
| Level 4 Examples | 2 weeks | Production skills | level4_mastery.py |

---

## 📋 Learning Paths by Goal

### "I want to learn Pandas from scratch"
1. Read: PANDAS_COMPLETE_GUIDE.md (Level 1)
2. Practice: level1_fundamentals.py
3. Follow: PANDAS_LEARNING_PATH.md (Week 1-5)
4. Build: Week 5 capstone project

### "I need to clean messy data"
1. Jump to: PANDAS_COMPLETE_GUIDE.md (Level 2: Data Cleaning)
2. Run: level2_intermediate.py (data cleaning examples)
3. Reference: PANDAS_LEARNING_PATH.md (Week 6-7)

### "I'm working with time series data"
1. Jump to: PANDAS_COMPLETE_GUIDE.md (Level 3: Time Series)
2. Run: level3_advanced.py (time series examples)
3. Follow: PANDAS_LEARNING_PATH.md (Week 11-12)

### "I need to optimize for large datasets"
1. Jump to: PANDAS_COMPLETE_GUIDE.md (Level 4: Performance)
2. Run: level4_mastery.py (optimization examples)
3. Study: PANDAS_LEARNING_PATH.md (Week 16)

---

## 🎯 Key Concepts at Each Level

### Level 1: Fundamentals
```python
# Series & DataFrames
s = pd.Series([1, 2, 3])
df = pd.DataFrame({'A': [1, 2, 3], 'B': [4, 5, 6]})

# Basic operations
df.head()      # View data
df['A'].mean() # Statistics
df[df['A'] > 1] # Filtering
```

### Level 2: Intermediate
```python
# Data cleaning
df.dropna()                    # Remove nulls
df.drop_duplicates()           # Remove duplicates
df['date'] = pd.to_datetime(df['date'])  # Convert types

# GroupBy
df.groupby('category')['sales'].sum()

# Merging
pd.merge(df1, df2, on='key')
```

### Level 3: Advanced
```python
# Time series
df.set_index('date').resample('M').sum()
df['value'].rolling(window=7).mean()

# Window functions
df['rank'] = df['value'].rank()
df['pct'] = df.groupby('group')['value'].transform(lambda x: x/x.sum())

# Custom operations
df['category'] = df['age'].apply(lambda x: 'Young' if x < 30 else 'Old')
```

### Level 4: Mastery
```python
# Optimization
df['category'] = df['category'].astype('category')  # Memory efficient
df.query('age > 25 and salary > 50000')  # Faster filtering

# Chunking large files
for chunk in pd.read_csv('huge.csv', chunksize=10000):
    process(chunk)

# Data validation
assert df['age'].min() >= 0, "Invalid age found"
```

---

## 📚 Real-World Scenarios Covered

1. **Sales Analysis** (Level 1)
   - Load sales data
   - Filter by criteria
   - Calculate statistics

2. **Customer Data Cleaning** (Level 2)
   - Handle missing values
   - Remove duplicates
   - Standardize formats
   - Merge customer sources

3. **Stock Price Analysis** (Level 3)
   - Time series operations
   - Rolling averages
   - Return calculations
   - Performance ranking

4. **ETL Pipeline** (Level 4)
   - Multi-source data integration
   - Complex transformations
   - Quality assurance
   - Large dataset optimization

---

## 💡 Usage Tips

### For Learning
```bash
# Read the guide section-by-section
cat PANDAS_COMPLETE_GUIDE.md | less

# Follow the structured path
cat PANDAS_LEARNING_PATH.md | grep "Week 1" -A 30

# Run examples and modify them
python examples/level1_fundamentals.py
# Then edit the file and try your own code
```

### For Reference
```bash
# Search for a concept
grep -n "GroupBy" PANDAS_COMPLETE_GUIDE.md

# Get quick syntax
grep -A 5 "Filtering Data" PANDAS_COMPLETE_GUIDE.md
```

### For Practice
```bash
# Run example level by level
python examples/level1_fundamentals.py
python examples/level2_intermediate.py
python examples/level3_advanced.py
python examples/level4_mastery.py

# Complete weekly exercises from PANDAS_LEARNING_PATH.md
# Build the capstone projects
```

---

## 🏆 Mastery Checklist

### Level 1 Complete ✓
- [ ] Create Series and DataFrames
- [ ] Read/write multiple file formats
- [ ] Access and filter data
- [ ] Compute basic statistics
- [ ] Completed Week 5 capstone

### Level 2 Complete ✓
- [ ] Clean missing data
- [ ] Remove duplicates
- [ ] Convert data types
- [ ] GroupBy aggregations
- [ ] Merge multiple sources
- [ ] Completed Week 10 capstone

### Level 3 Complete ✓
- [ ] Time series operations
- [ ] Rolling/expanding windows
- [ ] Apply custom functions
- [ ] Categorical data
- [ ] Multi-index structures
- [ ] Completed Week 15 capstone

### Level 4 Complete ✓
- [ ] Optimize memory usage
- [ ] Handle large datasets
- [ ] Data validation
- [ ] Build ETL pipelines
- [ ] Performance profiling
- [ ] Completed Week 20 capstone

---

## 🤔 Common Questions

**Q: Which file should I read first?**
A: If new to Pandas, start with PANDAS_LEARNING_PATH.md Week 1. Then reference PANDAS_COMPLETE_GUIDE.md as needed.

**Q: How long will it take to master Pandas?**
A: ~20 weeks with 5-7 hours/week of focused study and practice. Can be faster with prior Python experience.

**Q: Can I jump to advanced topics?**
A: If you already understand fundamentals, yes! Jump to Level 3 in PANDAS_COMPLETE_GUIDE.md. But Level 1-2 provide essential foundation.

**Q: Are the examples runnable?**
A: Yes! All examples in `examples/` folder are ready to run: `python examples/level1_fundamentals.py`

**Q: Where can I get datasets to practice?**
A: Kaggle (kaggle.com), UCI Machine Learning Repository, or create your own test data using the examples.

---

## 📞 Need Help?

### For Concept Explanations
→ Check PANDAS_COMPLETE_GUIDE.md (has analogies and detailed examples)

### For Structured Learning
→ Follow PANDAS_LEARNING_PATH.md week by week

### For Code Examples
→ Run examples in `examples/` folder and modify them

### For Real-World Scenarios
→ See capstone projects in PANDAS_LEARNING_PATH.md (Weeks 5, 10, 15, 20)

---

## 🎓 Next Steps

1. **This Week**: Start with Level 1 of PANDAS_LEARNING_PATH.md
2. **This Month**: Complete Levels 1-2
3. **Next 2 Months**: Complete Levels 3-4
4. **Apply Skills**: Use Pandas in your Spark/Kafka pipelines!

---

## 📁 File Structure

```
pandas/
├── README.md                    ← You are here!
├── PANDAS_COMPLETE_GUIDE.md     ← Complete reference (all topics)
├── PANDAS_LEARNING_PATH.md      ← 20-week structured roadmap
├── examples/
│   ├── level1_fundamentals.py   ← Basic Series/DataFrames
│   ├── level2_intermediate.py   ← Cleaning/groupby
│   ├── level3_advanced.py       ← Time series/windows
│   └── level4_mastery.py        ← Optimization/production
└── ... (more examples coming)
```

---

## 🚀 Ready to Start?

### For Beginners
```bash
# Week 1: Get started
cat PANDAS_LEARNING_PATH.md | grep "Week 1" -A 50
python examples/level1_fundamentals.py
```

### For Experienced Developers
```bash
# Jump to Level 3 or 4
grep -n "Level 3:" PANDAS_COMPLETE_GUIDE.md
python examples/level3_advanced.py
```

### For Your Project
```bash
# Use Pandas with Spark/Kafka
# In your spark/jobs/ - use Pandas for data processing
# In your kafka/consumers/ - use Pandas to organize streamed data
```

---

**Happy learning! You're on the path to becoming a Pandas expert. 🎉**

Questions? Check the relevant section in PANDAS_COMPLETE_GUIDE.md or review the examples!
