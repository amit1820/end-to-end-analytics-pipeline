# End-to-End Analytics Pipeline

A production-grade ETL (Extract, Transform, Load) pipeline demonstrating software engineering best practices for data analytics. Built with modular architecture, comprehensive logging, data quality checks, and automated testing.

## Overview

This project demonstrates how to build a maintainable, scalable analytics pipeline following industry best practices. Most portfolio projects show final outputs without the engineering behind them - this project focuses on the production-grade infrastructure required for reliable data processing.

## Architecture

```
analytics-pipeline/
├── src/                    # Source code modules
│   ├── ingestion.py       # Data loading and validation
│   ├── transformation.py  # Data cleaning and feature engineering
│   ├── validation.py      # Data quality checks
│   ├── aggregation.py     # Analytical aggregations
│   └── output.py          # Export and persistence
├── data/
│   ├── raw/               # Raw input data
│   └── processed/         # Pipeline outputs
├── logs/                  # Execution logs
├── tests/                 # Unit tests
├── config/                # Configuration files
├── run.py                 # Main execution script
└── requirements.txt       # Python dependencies
```

## Pipeline Stages

### Stage 1: Data Ingestion
- Loads raw data from CSV files
- Generates sample data if no input file exists
- Validates schema and structure
- Supports extensibility to databases

### Stage 2: Data Transformation
- Handles missing values based on business logic
- Fixes data types and formats
- Removes invalid values
- Creates derived columns and features
- Standardizes text fields

### Stage 3: Data Validation
- Completeness checks (missing values)
- Uniqueness checks (duplicates)
- Consistency checks (calculations match)
- Accuracy checks (realistic ranges)
- Generates quality reports

### Stage 4: Data Aggregation
- Daily revenue summaries
- Product performance metrics
- Customer behavior analysis
- Regional comparisons
- Hourly transaction patterns

### Stage 5: Data Output
- Saves processed data to CSV
- Exports aggregations
- Creates Excel reports with multiple sheets
- Generates validation reports
- Produces summary statistics

## Installation

### Prerequisites
- Python 3.8 or higher
- pip package manager

### Setup Steps

1. Clone or download this repository

2. Navigate to project directory:
```bash
cd analytics-pipeline
```

3. Create virtual environment (recommended):
```bash
python -m venv venv

# Windows
venv\Scripts\activate

# Mac/Linux
source venv/bin/activate
```

4. Install dependencies:
```bash
pip install -r requirements.txt
```

## Usage

### Basic Execution

Run the complete pipeline:
```bash
python run.py
```

The pipeline will:
1. Generate or load transaction data
2. Process through all 5 stages
3. Save outputs to `data/processed/`
4. Log execution to `logs/`

### Sample Output

```
================================================================================
ANALYTICS PIPELINE STARTED
================================================================================

STAGE 1/5: Data Ingestion
--------------------------------------------------------------------------------
Loading data from data/raw/transactions.csv
Loaded 10000 records

STAGE 2/5: Data Transformation
--------------------------------------------------------------------------------
Handling missing values
Dropped 200 rows with missing critical values
Transformation complete: 9800 clean records

STAGE 3/5: Data Validation
--------------------------------------------------------------------------------
All validation checks PASSED

STAGE 4/5: Data Aggregation
--------------------------------------------------------------------------------
Created 5 aggregation views:
  - daily_summary: 730 records
  - product_summary: 50 records
  - customer_summary: 1000 records
  - regional_summary: 4 records
  - hourly_patterns: 24 records

STAGE 5/5: Data Output
--------------------------------------------------------------------------------
Processed data saved
Saved 5 aggregation files
Validation report saved
Excel report saved

================================================================================
PIPELINE SUMMARY
================================================================================
Status: SUCCESS
Execution time: 2.45 seconds

Data Flow:
  Raw records:         10000
  Transformed records: 9800
  Data quality:        PASSED

Outputs Generated:
  - Processed data
  - 5 aggregation files
  - Validation report
  - Excel report
  - Summary statistics
================================================================================
```

## Output Files

After execution, find outputs in `data/processed/`:

- **processed_transactions.csv** - Clean, transformed data
- **daily_summary_TIMESTAMP.csv** - Daily metrics
- **product_summary_TIMESTAMP.csv** - Product performance
- **customer_summary_TIMESTAMP.csv** - Customer analysis
- **regional_summary_TIMESTAMP.csv** - Regional breakdown
- **hourly_patterns_TIMESTAMP.csv** - Time patterns
- **validation_report_TIMESTAMP.json** - Quality metrics
- **analytics_report_TIMESTAMP.xlsx** - Multi-sheet Excel report
- **summary_stats_TIMESTAMP.csv** - Statistical summary

Logs are saved to `logs/pipeline_TIMESTAMP.log`

## Customization

### Using Your Own Data

1. Place your CSV file in `data/raw/transactions.csv`

2. Ensure it has these columns (minimum):
```
transaction_id, timestamp, total_amount, customer_id, product_id, 
product_category, quantity, unit_price, region, payment_method, 
discount_applied
```

3. Run the pipeline: `python run.py`

### Modifying Transformations

Edit `src/transformation.py` to add custom logic:

```python
def _add_derived_columns(self, df):
    # Add your custom calculations
    df['custom_metric'] = df['revenue'] * df['multiplier']
    return df
```

### Adding Validation Rules

Edit `src/validation.py` to add checks:

```python
def _check_custom_rule(self, df):
    # Your validation logic
    passed = df['column'].between(0, 100).all()
    details = "Custom check details"
    return passed, details
```

### Creating New Aggregations

Edit `src/aggregation.py`:

```python
def _aggregate_by_custom(self, df):
    custom_agg = df.groupby('dimension').agg({
        'metric': ['sum', 'mean', 'count']
    })
    return custom_agg
```

## Design Principles

### Modularity
Each stage is a separate module with single responsibility. Easy to test, modify, and extend independently.

### Logging
Comprehensive logging at every stage. Track data flow, identify issues, and audit executions.

### Error Handling
Try-catch blocks with informative error messages. Pipeline fails gracefully and logs full context.

### Data Quality
Validation built into the pipeline, not as an afterthought. Catches issues before they propagate.

### Reproducibility
All transformations are deterministic. Same input always produces same output. Logs provide full audit trail.

### Configurability
Settings and business logic separated from code structure. Easy to adapt to different use cases.

## Engineering Practices Demonstrated

1. **Separation of Concerns**: Each module has one job
2. **DRY Principle**: Reusable functions, no code duplication
3. **Logging**: Production-grade logging infrastructure
4. **Error Handling**: Graceful failure with context
5. **Documentation**: Docstrings and comments throughout
6. **Code Organization**: Clear folder structure
7. **Dependency Management**: requirements.txt for reproducibility
8. **Testing Ready**: Structure supports unit testing

## Common Use Cases

**Data Engineering Teams**
- Template for production ETL pipelines
- Foundation for data quality monitoring
- Example of modular pipeline design

**Business Analysts**
- Automated data preparation
- Repeatable analysis workflows
- Quality-controlled reporting

**Data Scientists**
- Feature engineering pipeline
- Data preprocessing automation
- Model input preparation

## Extending the Pipeline

### Adding Database Connectivity

In `src/ingestion.py`:
```python
def load_from_postgres(self, connection_string, query):
    import psycopg2
    df = pd.read_sql(query, connection_string)
    return df
```

### Adding Data Quality Alerts

In `src/validation.py`:
```python
def send_alert(self, validation_report):
    if not validation_report['overall_status']:
        # Send email/Slack notification
        pass
```

### Scheduling Execution

Use cron (Linux/Mac) or Task Scheduler (Windows):
```bash
# Run daily at 2 AM
0 2 * * * /path/to/venv/bin/python /path/to/run.py
```

Or use Apache Airflow, Prefect, or similar orchestration tools.

## Troubleshooting

### Import Errors
Ensure you're in the project root and virtual environment is activated.

### File Not Found
Check that `data/raw/` and `logs/` directories exist. Pipeline creates them automatically.

### Memory Issues
For large datasets, process in chunks:
```python
chunks = pd.read_csv('file.csv', chunksize=10000)
for chunk in chunks:
    process(chunk)
```

## Dependencies

- **pandas**: Data manipulation and analysis
- **numpy**: Numerical computations
- **openpyxl**: Excel file creation

All specified in requirements.txt

## Testing

Create test files in `tests/` directory:

```python
# tests/test_transformation.py
import pytest
from src.transformation import DataTransformation

def test_handle_missing_values():
    # Your test logic
    pass
```

Run tests:
```bash
pytest tests/
```

## Future Enhancements

Planned improvements:
- PostgreSQL/MySQL database connectivity
- Incremental processing for large datasets
- Airflow DAG for orchestration
- Data profiling and anomaly detection
- Real-time streaming support
- Web dashboard for monitoring
- Email alerts for validation failures
- Automated testing suite

## License

MIT License - Free for commercial and personal use

## Author

Amit Kumar
- LinkedIn: linkedin.com/in/amit1820
- GitHub: github.com/amit1820

## Contributing

Contributions welcome. Guidelines:
- Follow existing code style
- Add docstrings to new functions
- Update README for new features
- Test thoroughly before submitting

---

Built to demonstrate production-grade data engineering practices
