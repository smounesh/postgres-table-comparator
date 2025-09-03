# Postgres Table Comparator

A high-performance PostgreSQL table comparator that detects differences and generates SQL fix scripts using intelligent hierarchical partitioning for massive performance gains.

[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)
[![Go Version](https://img.shields.io/badge/Go-1.23+-blue.svg)](https://golang.org)
[![PostgreSQL](https://img.shields.io/badge/PostgreSQL-12+-blue.svg)](https://postgresql.org)

## Production Tested

**This tool is battle-tested in production environments.** It has successfully identified as few as 3-4 row differences in tables with over 100 million rows. 


## 🚀 Performance

This tool automatically selects the optimal comparison strategy based on table size:

| Table Size | Strategy | Performance Gain |
|------------|----------|------------------|
| < 500K rows | Traditional | Baseline |
| 500K - 10M | Two-level hierarchy | 2-5x faster |
| 10M - 100M | Three-level hierarchy | 5-15x faster |
| > 100M rows | Four-level hierarchy | 10-50x faster |


## Build Locally

```bash
# After cloning and entering the directory
git clone https://github.com/smounesh/postgres-table-comparator.git
cd postgres-table-comparator

# Create the go.mod go.sum
go mod init postgres-table-comparator

# Add required dependencies
go mod tidy

# Then build
go build -o postgres-table-comparator
```

## 🔧 Usage

### Basic Comparison

```bash
./postgres-table-comparator \
  -source "postgres://user:pass@source-host:5432/dbname" \
  -target "postgres://user:pass@target-host:5432/dbname" \
  -table users
```

### Advanced Options

```bash
./postgres-table-comparator \
  -source "postgres://user:pass@source-host:5432/dbname" \
  -target "postgres://user:pass@target-host:5432/dbname" \
  -table transactions \
  -schema public \
  -strategy auto \
  -workers 16 \
  -connections 50 \
  -batch 20000 \
  -output results.json \
  -sql-fix fixes.sql \
  -verbose
```

### Command Line Options

| Flag         | Description                                   | Default               |
|--------------|-----------------------------------------------|-----------------------|
| `-source`    | Source database URL (**required**)            | -                     |
| `-target`    | Target database URL (**required**)            | -                     |
| `-table`     | Table name to compare (**required**)          | -                     |
| `-schema`    | Schema name                                   | public                |
| `-strategy`  | Comparison strategy: auto, traditional, two-level, three-level, four-level | auto |
| `-workers`   | Number of parallel workers                    | CPU cores × 4         |
| `-connections`| Max connections per pool                     | 25                    |
| `-batch`     | Batch size for fetching rows                  | 10000                 |
| `-output`    | JSON output file path                         | -                     |
| `-sql-fix`   | SQL fix script file path                      | -                     |
| `-verbose`   | Enable detailed logging                       | false                 |
| `-no-color`  | Disable colored output                        | false                 |

---

### Advanced Hierarchical Options

| Flag                     | Description                         | Default |
|---------------------------|-------------------------------------|---------|
| `-coarse-partitions`      | Number of coarse partitions         | Auto    |
| `-fine-partition-size`    | Fine partition size                 | Auto    |
| `-micro-partition-size`   | Micro partition size                | Auto    |
| `-row-threshold`          | Row-level threshold                 | Auto    |
| `-diff-threshold`         | Difference threshold for adaptation | 0.25    |


## 📖 Examples

### Compare Small Configuration Table
```bash
./postgres-table-comparator \
  -source "postgres://user:pass@prod:5432/myapp" \
  -target "postgres://user:pass@staging:5432/myapp" \
  -table "employees"  \
  -strategy "traditional"  \
  -output "comparison_report_employees.json" \
  -sql-fix "sync_journey_employees.sql" \
  -verbose
```

### Force Specific Strategy for Testing
```bash
./postgres-table-comparator \
  -source "postgres://user:pass@db1:5432/test" \
  -target "postgres://user:pass@db2:5432/test" \
  -table large_table \
  -strategy three-level \
  -coarse-partitions 20 \
  -fine-partition-size 100000
```

### For Large Tables (>100M rows)
```bash
./postgres-table-comparator \
  -source "..." \
  -target "..." \
  -table huge_table \
  -strategy four-level \
  -workers 32 \
  -connections 100 \
  -batch 50000
```

## 🎯 How It Works

### Hierarchical Partitioning Algorithm

- Level 1 (Coarse): Divides table into large chunks and computes aggregate hashes
- Level 2 (Fine): Only processes coarse partitions with differences
- Level 3 (Micro): Further subdivides differing fine partitions
- Level 4 (Nano): Maximum granularity before row-level comparison
- Row-Level: Actual row-by-row comparison only for smallest differing partitions

This approach dramatically reduces the amount of data transferred and compared, especially for tables where differences are localized.

### Comparison Process

- Statistics Gathering: Analyzes table structure and estimates row counts
- Strategy Selection: Chooses optimal approach based on table size
- Partition Creation: Builds hierarchical partition tree
- Hash Comparison: Computes and compares MD5 hashes at each level
- Difference Detection: Identifies missing, extra, and modified rows
- Result Generation: Creates JSON report and SQL fix scripts

## 📊 Output Formats

### JSON Report Structure
```json
{
  "summary": {
    "source_db": "postgres://...",
    "target_db": "postgres://...",
    "table": "public.users",
    "strategy": "three-level",
    "total_source_rows": 5000000,
    "total_target_rows": 5000100,
    "rows_checked": 4523,
    "execution_time": "45.2s",
    "missing_count": 10,
    "extra_count": 110,
    "modified_count": 23,
    "match_percentage": 97.05,
    "partitions_skipped": 892,
    "partitions_processed": 47
  },
  "missing_rows": [...],
  "extra_rows": [...],
  "modified_rows": [...]
}
```

### SQL Fix Script Format
```sql
-- Database Table Comparison Fix Script
-- Generated: 2025-01-15 10:30:45
-- Table: public.users
-- Strategy: three-level
-- Total Fixes: 143

BEGIN;

-- Missing Rows: Insert 10 rows into target
INSERT INTO public.users (id, name, email, ...) VALUES (...);

-- Extra Rows: Delete 110 rows from target
DELETE FROM public.users WHERE id = 12345;

-- Modified Rows: Update 23 rows in target
UPDATE public.users SET name = 'John Doe', ... WHERE id = 67890;

COMMIT;
```

## 🏗️ Architecture
The tool uses several optimization techniques:

- Connection Pooling: Maintains persistent connections to reduce overhead
- Parallel Workers: Distributes partition processing across multiple goroutines
- Batch Processing: Fetches rows in configurable chunks
- Smart Skipping: Avoids processing identical partitions entirely
- Memory Streaming: Processes results without loading entire table into memory

## 📋 Requirements

Go 1.23 or higher
PostgreSQL 12 or higher
Tables must have a primary key (single column, integer type)
Network connectivity between source and target databases

## 🤝 Contributing
Contributions are welcome! Please feel free to submit a Pull Request.

## 📝 License
This project is licensed under the MIT License - see the LICENSE file for details.
🙏 Acknowledgments

Built with pgx - PostgreSQL driver and toolkit for Go
Inspired by the need for efficient database migration validation

## 📞 Support
For issues, questions, or suggestions, please open an issue.
