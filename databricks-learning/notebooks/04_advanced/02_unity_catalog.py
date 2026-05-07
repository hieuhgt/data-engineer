# Databricks notebook source
# MAGIC %md
# MAGIC # 02 — Unity Catalog: Governance and Access Control
# MAGIC
# MAGIC ## What this notebook teaches
# MAGIC Unity Catalog is Databricks' unified data governance layer. If you've used Hive metastore
# MAGIC (the default in older Databricks and standalone Spark), Unity Catalog is its replacement —
# MAGIC with enterprise features added: column-level security, data lineage, and cross-workspace sharing.
# MAGIC
# MAGIC **What you already know → What's new:**
# MAGIC | What you know | Unity Catalog equivalent |
# MAGIC |---|---|
# MAGIC | Hive metastore (`default.users`) | Unity Catalog (`catalog.schema.table`) — 3 levels instead of 2 |
# MAGIC | S3 bucket permissions (IAM) | Unity Catalog column-level GRANT/REVOKE |
# MAGIC | No lineage tracking | Automatic data lineage (who reads/writes what) |
# MAGIC | Per-workspace metastore | Unity Catalog spans entire account (all workspaces) |
# MAGIC | External Hive tables | External locations (storage credentials) |
# MAGIC
# MAGIC > **Community Edition note**: Unity Catalog is not available in Community Edition.
# MAGIC > This notebook shows the concepts and SQL commands — you'll use them in a real workspace.
# MAGIC > Some cells can run (SQL commands on temp views), others show patterns you'll use in production.
# MAGIC
# MAGIC **Topics:**
# MAGIC 1. The 3-level namespace: catalog.schema.table
# MAGIC 2. Creating catalogs, schemas, and tables
# MAGIC 3. Access control: GRANT/REVOKE at table and column level
# MAGIC 4. Row-level security with dynamic views
# MAGIC 5. Data lineage tracking
# MAGIC 6. External locations (connecting to S3/ADLS)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. The 3-Level Namespace
# MAGIC
# MAGIC **Before Unity Catalog (Hive metastore):**
# MAGIC ```
# MAGIC database.table        (2 levels)
# MAGIC bronze.users
# MAGIC silver.users
# MAGIC ```
# MAGIC
# MAGIC **With Unity Catalog:**
# MAGIC ```
# MAGIC catalog.schema.table  (3 levels)
# MAGIC my_company.bronze.users
# MAGIC my_company.silver.users
# MAGIC production.silver.users
# MAGIC development.silver.users
# MAGIC ```
# MAGIC
# MAGIC **Why 3 levels?**
# MAGIC - `catalog` = organization boundary (e.g., per business unit, per environment)
# MAGIC - `schema` = equivalent to old "database" (Bronze/Silver/Gold layers)
# MAGIC - `table` = the table
# MAGIC
# MAGIC **In practice**: Your medallion pipeline becomes:
# MAGIC ```
# MAGIC company_data.bronze.users
# MAGIC company_data.silver.users
# MAGIC company_data.gold.user_stats
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Creating Catalogs and Schemas

# COMMAND ----------

# MAGIC %md
# MAGIC ### In a real Unity Catalog workspace, you'd run:
# MAGIC
# MAGIC ```sql
# MAGIC -- Create a catalog (one per business domain or environment)
# MAGIC CREATE CATALOG IF NOT EXISTS company_data
# MAGIC COMMENT 'Production data catalog for company data assets';
# MAGIC
# MAGIC -- Switch to this catalog
# MAGIC USE CATALOG company_data;
# MAGIC
# MAGIC -- Create schemas (medallion layers)
# MAGIC CREATE SCHEMA IF NOT EXISTS bronze COMMENT 'Raw ingested data, immutable';
# MAGIC CREATE SCHEMA IF NOT EXISTS silver COMMENT 'Cleansed and conformed data';
# MAGIC CREATE SCHEMA IF NOT EXISTS gold   COMMENT 'Business-level aggregates for reporting';
# MAGIC
# MAGIC -- Create a managed Delta table in the silver schema
# MAGIC CREATE TABLE IF NOT EXISTS silver.users (
# MAGIC     id           INT      NOT NULL,
# MAGIC     full_name    STRING,
# MAGIC     email        STRING,
# MAGIC     age          INT,
# MAGIC     company_name STRING,
# MAGIC     department   STRING,
# MAGIC     _run_date    STRING
# MAGIC )
# MAGIC USING DELTA
# MAGIC COMMENT 'Cleansed user data from DummyJSON API'
# MAGIC TBLPROPERTIES ('delta.autoOptimize.optimizeWrite' = 'true');
# MAGIC ```

# COMMAND ----------

# We can demonstrate some Unity Catalog SQL patterns using the local session
# These work even in Community Edition (against temp views), but in a real workspace
# they'd target actual Unity Catalog tables

from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

# Create sample data
users_data = [
    (1, "Emily Johnson",   "emily@example.com",   28, "Engineering", "Acme Corp",     95000, "Senior",   "US"),
    (2, "Michael Williams","michael@example.com", 35, "Product",     "TechStart Inc", 130000, "Manager",  "US"),
    (3, "Sophia Martinez", "sophia@example.com",  31, "Data",        "Acme Corp",     110000, "Senior",   "MX"),
    (4, "James Brown",     "james@example.com",   42, "Engineering", "DataFlow LLC",  145000, "Principal","US"),
    (5, "Olivia Davis",    "olivia@example.com",  26, "Data",        "TechStart Inc",  82000, "Junior",   "CA"),
    (6, "Liam Wilson",     "liam@example.com",    29, "Analytics",   "DataFlow LLC",   95000, "Mid",      "UK"),
]

schema = "id INT, full_name STRING, email STRING, age INT, department STRING, company STRING, salary INT, level STRING, country STRING"
df = spark.createDataFrame(users_data, schema)

# Register as a global temp view to simulate a Unity Catalog table
df.createOrReplaceTempView("silver_users")
print("Created temp view: silver_users (simulates company_data.silver.users)")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Access Control — GRANT and REVOKE
# MAGIC
# MAGIC Unity Catalog has a GRANT/REVOKE model similar to PostgreSQL/Snowflake.
# MAGIC This replaces S3 bucket-level IAM policies with table/column-level permissions.
# MAGIC
# MAGIC **Permission hierarchy:**
# MAGIC ```
# MAGIC Account Admin
# MAGIC └── Catalog Admin (GRANT on catalog)
# MAGIC     └── Schema Admin (GRANT on schema)
# MAGIC         └── Table owner (GRANT on table)
# MAGIC             └── Column-level masking (GRANT on column)
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC ### Production GRANT patterns
# MAGIC
# MAGIC ```sql
# MAGIC -- Give the analyst team read access to silver schema
# MAGIC GRANT USE SCHEMA ON SCHEMA company_data.silver TO `analysts@company.com`;
# MAGIC GRANT SELECT ON SCHEMA company_data.silver TO `analysts@company.com`;
# MAGIC
# MAGIC -- Give data engineers full access to bronze
# MAGIC GRANT ALL PRIVILEGES ON SCHEMA company_data.bronze TO `data-engineers@company.com`;
# MAGIC
# MAGIC -- Give a specific service principal access to a table
# MAGIC GRANT SELECT, MODIFY ON TABLE company_data.silver.users TO `airflow-service-principal`;
# MAGIC
# MAGIC -- Revoke access
# MAGIC REVOKE SELECT ON TABLE company_data.silver.users FROM `contractor@partner.com`;
# MAGIC
# MAGIC -- Show what privileges exist on a table
# MAGIC SHOW GRANTS ON TABLE company_data.silver.users;
# MAGIC
# MAGIC -- Check what the current user can do
# MAGIC SHOW GRANTS TO `analyst@company.com`;
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Column-Level Security
# MAGIC
# MAGIC Unity Catalog lets you restrict access to specific columns — like PII fields.
# MAGIC This is something you can't do with S3 bucket policies.
# MAGIC
# MAGIC **Two approaches:**
# MAGIC 1. **Dynamic view with IF(is_account_group_member(...))** — filter/mask based on group membership
# MAGIC 2. **Column masks** (Unity Catalog feature) — apply a masking function to a column automatically

# COMMAND ----------

# Simulate column-level security with a dynamic view
# In production, this view would be in Unity Catalog and reference actual groups

spark.sql("""
    CREATE OR REPLACE TEMPORARY VIEW users_with_column_security AS
    SELECT
        id,
        full_name,
        -- Mask email: only data engineers see the real email
        -- is_account_group_member() is a Unity Catalog function (not available in CE)
        -- In CE we simulate with a variable
        CASE
            WHEN current_user() LIKE '%engineer%' THEN email
            ELSE CONCAT(SUBSTRING(email, 1, 2), '***@***.com')
        END AS email,

        age,
        department,
        company,

        -- Mask salary: only managers and above see real salary
        CASE
            WHEN level IN ('Manager', 'Principal', 'Director') THEN CAST(salary AS STRING)
            ELSE '***confidential***'
        END AS salary_display,

        level,
        country
    FROM silver_users
""")

print("Created column-masked view: users_with_column_security")
print("(In Unity Catalog, this would use is_account_group_member() instead of CASE/WHEN)")
print()

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Query the masked view
# MAGIC -- email and salary are masked based on the user's role
# MAGIC SELECT id, full_name, email, salary_display, level
# MAGIC FROM users_with_column_security
# MAGIC ORDER BY id

# COMMAND ----------

# MAGIC %md
# MAGIC ### Unity Catalog column masking (production syntax)
# MAGIC
# MAGIC ```sql
# MAGIC -- Create a masking function
# MAGIC CREATE FUNCTION company_data.security.mask_pii(pii_value STRING)
# MAGIC RETURNS STRING
# MAGIC RETURN CASE
# MAGIC   WHEN is_account_group_member('data-engineers') THEN pii_value
# MAGIC   ELSE CONCAT(SUBSTRING(pii_value, 1, 2), '***@***.com')
# MAGIC END;
# MAGIC
# MAGIC -- Apply the masking function to the email column
# MAGIC ALTER TABLE company_data.silver.users
# MAGIC ALTER COLUMN email
# MAGIC SET MASK company_data.security.mask_pii;
# MAGIC
# MAGIC -- Now ANY query on this table will automatically mask email for non-engineers
# MAGIC -- The masking is invisible to the user — they just see masked values
# MAGIC SELECT id, full_name, email FROM company_data.silver.users;
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Row-Level Security
# MAGIC
# MAGIC You can also restrict which ROWS a user can see — e.g., US analysts only see US data.

# COMMAND ----------

# Row-level security via a dynamic view
spark.sql("""
    CREATE OR REPLACE TEMPORARY VIEW users_row_filtered AS
    SELECT * FROM silver_users
    WHERE
        -- In Unity Catalog, use: is_account_group_member('us-analysts')
        -- Here we simulate: each user only sees their country's data
        -- In real setup, map current_user() to their allowed countries via a mapping table
        country IN ('US', 'CA')  -- simulating: this user is allowed US + CA data
        -- A real Unity Catalog row filter would look like:
        -- OR is_account_group_member('global-data-access')
""")

print("Row-filtered view (simulates: this user can only see US and CA data):")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT id, full_name, department, country
# MAGIC FROM users_row_filtered
# MAGIC ORDER BY country, id

# COMMAND ----------

# MAGIC %md
# MAGIC ### Unity Catalog row filters (production syntax)
# MAGIC
# MAGIC ```sql
# MAGIC -- Create a row filter function
# MAGIC CREATE FUNCTION company_data.security.user_country_filter(country STRING)
# MAGIC RETURNS BOOLEAN
# MAGIC RETURN
# MAGIC   is_account_group_member('global-access')  -- global team sees everything
# MAGIC   OR EXISTS (
# MAGIC     SELECT 1 FROM company_data.security.user_country_access
# MAGIC     WHERE user_email = current_user()
# MAGIC     AND allowed_country = country
# MAGIC   );
# MAGIC
# MAGIC -- Apply the row filter to the table
# MAGIC ALTER TABLE company_data.silver.users
# MAGIC SET ROW FILTER company_data.security.user_country_filter ON (country);
# MAGIC
# MAGIC -- Now queries automatically filter rows based on the user's allowed countries
# MAGIC SELECT * FROM company_data.silver.users;  -- each user sees only their allowed rows
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Data Lineage
# MAGIC
# MAGIC Unity Catalog automatically tracks:
# MAGIC - Which tables read from which tables (table-level lineage)
# MAGIC - Which columns are derived from which columns (column-level lineage)
# MAGIC
# MAGIC This is automatic — no code changes needed. Every read and write is tracked.
# MAGIC
# MAGIC **In the Databricks UI:**
# MAGIC 1. Go to **Catalog** → select your table
# MAGIC 2. Click the **Lineage** tab
# MAGIC 3. See the upstream and downstream tables as a graph
# MAGIC
# MAGIC For example, after running your medallion pipeline:
# MAGIC ```
# MAGIC DummyJSON API
# MAGIC     ↓
# MAGIC company_data.bronze.users (written by: notebook 01_batch_pipeline, job run_id=123)
# MAGIC     ↓
# MAGIC company_data.silver.users (derived from: bronze.users, transformation: job run_id=123)
# MAGIC     ↓
# MAGIC company_data.gold.user_stats (derived from: silver.users)
# MAGIC     ↓
# MAGIC Tableau Dashboard / Power BI (reads from: gold.user_stats)
# MAGIC ```

# COMMAND ----------

# Simulate what lineage tracking captures
lineage_entries = [
    {
        "entity": "bronze.users",
        "entity_type": "TABLE",
        "upstream": "DummyJSON API (external)",
        "downstream": "silver.users",
        "written_by": "job:daily_users_pipeline / task:ingest_bronze",
        "columns_derived": "all columns (raw copy)"
    },
    {
        "entity": "silver.users",
        "entity_type": "TABLE",
        "upstream": "bronze.users",
        "downstream": "gold.user_stats",
        "written_by": "job:daily_users_pipeline / task:transform_silver",
        "columns_derived": "full_name ← firstName + lastName\ncity ← address.city\ncompany_name ← company.name"
    },
    {
        "entity": "gold.user_stats",
        "entity_type": "TABLE",
        "upstream": "silver.users",
        "downstream": "Power BI dashboard (external)",
        "written_by": "job:daily_users_pipeline / task:aggregate_gold",
        "columns_derived": "headcount ← COUNT(id)\navg_age ← AVG(age)"
    },
]

print("Lineage graph for the users medallion pipeline:")
print("(Unity Catalog captures this automatically — no code needed)")
print()
for entry in lineage_entries:
    print(f"  [{entry['entity']}]")
    print(f"    upstream:       {entry['upstream']}")
    print(f"    downstream:     {entry['downstream']}")
    print(f"    written by:     {entry['written_by']}")
    print(f"    column lineage: {entry['columns_derived']}")
    print()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. External Locations — Connecting to S3/ADLS
# MAGIC
# MAGIC External locations replace mount points and allow Unity Catalog to govern access
# MAGIC to cloud storage. This is how you connect MinIO/S3-compatible storage.

# COMMAND ----------

# MAGIC %md
# MAGIC ### Setting up an External Location (admin task, done once)
# MAGIC
# MAGIC ```sql
# MAGIC -- Step 1: Create a storage credential (links to IAM role or service principal)
# MAGIC CREATE STORAGE CREDENTIAL my_s3_credential
# MAGIC WITH (
# MAGIC   TYPE = 'AWS_IAM_ROLE',
# MAGIC   ROLE_ARN = 'arn:aws:iam::123456789012:role/databricks-data-role'
# MAGIC );
# MAGIC
# MAGIC -- Step 2: Create an external location (S3 path + credential)
# MAGIC CREATE EXTERNAL LOCATION my_datalake
# MAGIC URL 's3://my-company-datalake/'
# MAGIC WITH (STORAGE CREDENTIAL my_s3_credential)
# MAGIC COMMENT 'Company data lake in us-east-1';
# MAGIC
# MAGIC -- Step 3: Grant teams access to the location
# MAGIC GRANT READ FILES ON EXTERNAL LOCATION my_datalake TO `data-engineers`;
# MAGIC GRANT WRITE FILES ON EXTERNAL LOCATION my_datalake TO `pipeline-service-account`;
# MAGIC
# MAGIC -- Step 4: Create an external table pointing to S3 (data stays in S3, metadata in UC)
# MAGIC CREATE EXTERNAL TABLE company_data.bronze.users
# MAGIC LOCATION 's3://my-company-datalake/bronze/users/'
# MAGIC USING DELTA;
# MAGIC ```
# MAGIC
# MAGIC **For MinIO (S3-compatible)**, replace `s3://` with your MinIO endpoint:
# MAGIC ```sql
# MAGIC CREATE EXTERNAL LOCATION minio_datalake
# MAGIC URL 's3://my-bucket/'
# MAGIC WITH (
# MAGIC   STORAGE CREDENTIAL minio_credential
# MAGIC   -- MinIO credentials configured in the storage credential
# MAGIC );
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC ## 8. Putting It All Together — Production Unity Catalog Setup
# MAGIC
# MAGIC Here's what a full Unity Catalog setup looks like for your medallion pipeline:

# COMMAND ----------

full_setup_sql = """
-- =========================================================
-- Unity Catalog Setup for Users Pipeline
-- Run by: account admin / catalog admin
-- =========================================================

-- 1. Create the catalog (one per environment or business unit)
CREATE CATALOG IF NOT EXISTS company_data
COMMENT 'Production data catalog';

USE CATALOG company_data;

-- 2. Create medallion schemas
CREATE SCHEMA IF NOT EXISTS bronze COMMENT 'Raw ingested data';
CREATE SCHEMA IF NOT EXISTS silver COMMENT 'Cleansed data';
CREATE SCHEMA IF NOT EXISTS gold   COMMENT 'Business aggregates';
CREATE SCHEMA IF NOT EXISTS security COMMENT 'Access control functions';

-- 3. Grant schema-level access
GRANT USE CATALOG ON CATALOG company_data TO `data-engineers@company.com`;
GRANT USE CATALOG ON CATALOG company_data TO `analysts@company.com`;

GRANT ALL PRIVILEGES ON SCHEMA bronze TO `data-engineers@company.com`;
GRANT ALL PRIVILEGES ON SCHEMA silver TO `data-engineers@company.com`;
GRANT SELECT ON SCHEMA silver TO `analysts@company.com`;
GRANT SELECT ON SCHEMA gold   TO `analysts@company.com`;

-- 4. Create the users table in silver
CREATE TABLE IF NOT EXISTS silver.users (
    id           INT     NOT NULL,
    full_name    STRING,
    email        STRING,   -- will be masked for non-engineers
    age          INT,
    department   STRING,
    company_name STRING,
    salary       INT,      -- will be masked based on role
    country      STRING,   -- will be row-filtered by country access
    _run_date    STRING,
    _updated_at  TIMESTAMP
)
USING DELTA
CLUSTER BY (company_name, department)
COMMENT 'Cleansed user data — column and row security applied';

-- 5. Create security functions
CREATE OR REPLACE FUNCTION security.mask_email(email STRING)
RETURNS STRING
RETURN CASE
  WHEN is_account_group_member('data-engineers') THEN email
  ELSE CONCAT(SUBSTRING(email, 1, 2), '***@***.com')
END;

CREATE OR REPLACE FUNCTION security.mask_salary(salary INT, user_level STRING)
RETURNS STRING
RETURN CASE
  WHEN is_account_group_member('hr-team') OR is_account_group_member('managers') THEN CAST(salary AS STRING)
  ELSE '***'
END;

CREATE OR REPLACE FUNCTION security.country_row_filter(country STRING)
RETURNS BOOLEAN
RETURN is_account_group_member('global-access')
    OR EXISTS (
        SELECT 1 FROM security.user_country_access
        WHERE user_email = current_user() AND allowed_country = country
    );

-- 6. Apply column masks
ALTER TABLE silver.users ALTER COLUMN email SET MASK security.mask_email;

-- 7. Apply row filter
ALTER TABLE silver.users SET ROW FILTER security.country_row_filter ON (country);

-- 8. Show what was created
SHOW SCHEMAS IN company_data;
SHOW TABLES IN company_data.silver;
DESCRIBE TABLE EXTENDED company_data.silver.users;
"""

print("Full Unity Catalog setup SQL:")
print("(This runs in a Unity Catalog-enabled workspace by an admin)")
print()
print(full_setup_sql)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 9. Unity Catalog vs Hive Metastore — Migration Path
# MAGIC
# MAGIC If you have existing Hive metastore tables (from your current Databricks setup),
# MAGIC you can migrate them to Unity Catalog:
# MAGIC
# MAGIC ```sql
# MAGIC -- Option 1: Upgrade in place (table stays in same location, metadata moves to UC)
# MAGIC -- Run from your workspace with Unity Catalog enabled
# MAGIC SYNC TABLE company_data.silver.users
# MAGIC FROM hive_metastore.silver.users;
# MAGIC
# MAGIC -- Option 2: CREATE TABLE AS SELECT (copies data to UC-managed location)
# MAGIC CREATE TABLE company_data.silver.users
# MAGIC AS SELECT * FROM hive_metastore.silver.users;
# MAGIC
# MAGIC -- Option 3: Databricks Upgrade tool (for large-scale migration)
# MAGIC -- Available in the Databricks UI: Catalog Explorer → "Upgrade" button
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC ## Summary
# MAGIC
# MAGIC | Concept | Key SQL | Notes |
# MAGIC |---|---|---|
# MAGIC | 3-level namespace | `catalog.schema.table` | replaces `database.table` |
# MAGIC | Create catalog | `CREATE CATALOG my_catalog` | admin task |
# MAGIC | Create schema | `CREATE SCHEMA my_catalog.bronze` | your Bronze/Silver/Gold layers |
# MAGIC | Grant access | `GRANT SELECT ON TABLE ... TO ...` | replaces S3 IAM policies |
# MAGIC | Column mask | `ALTER TABLE ... ALTER COLUMN ... SET MASK func` | PII, salary, etc. |
# MAGIC | Row filter | `ALTER TABLE ... SET ROW FILTER func ON (col)` | country/region filtering |
# MAGIC | Data lineage | Automatic — view in Catalog Explorer | no code needed |
# MAGIC | External location | `CREATE EXTERNAL LOCATION ... URL 's3://...'` | connect to S3/MinIO/ADLS |
# MAGIC
# MAGIC **Congratulations!** You've completed the Databricks learning path.
# MAGIC
# MAGIC You now know:
# MAGIC - Delta Lake (ACID, MERGE, time travel) — the storage layer
# MAGIC - Databricks Runtime, display(), dbutils — the platform APIs
# MAGIC - Batch medallion pipelines with Delta — same pattern, better tooling
# MAGIC - Auto Loader — file streaming without Kafka
# MAGIC - Workflows — Airflow replacement for Databricks-native pipelines
# MAGIC - OPTIMIZE, ZORDER, VACUUM, Liquid Clustering — performance tuning
# MAGIC - Unity Catalog — governance, security, and lineage

# COMMAND ----------

# MAGIC %md
# MAGIC ## What to Learn Next
# MAGIC
# MAGIC - **Delta Live Tables (DLT)**: Declarative ETL with built-in data quality expectations.
# MAGIC   Like dbt but for streaming + batch, fully managed by Databricks.
# MAGIC
# MAGIC - **MLflow on Databricks**: Track ML experiments, register models, deploy to serving endpoints.
# MAGIC   MLflow is open source but deeply integrated into Databricks Runtime.
# MAGIC
# MAGIC - **Databricks SQL**: A dedicated SQL warehouse (different from the compute cluster).
# MAGIC   Optimized for BI queries, connects to Tableau/Power BI natively.
# MAGIC
# MAGIC - **Terraform provider**: `databricks/databricks` Terraform provider to manage all of this
# MAGIC   as infrastructure-as-code (clusters, jobs, secrets, Unity Catalog objects).
