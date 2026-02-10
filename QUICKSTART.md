# Quick Start Guide - Analytics Pipeline

## What This Pipeline Does

Processes transaction data through 5 automated stages:
1. Loads raw data
2. Cleans and transforms it
3. Validates quality
4. Creates business aggregations
5. Exports multiple output formats

## Installation (5 minutes)

### Step 1: Extract Files
Extract the zip file to your desired location.

### Step 2: Open Terminal
- **Windows**: Open Command Prompt or PowerShell in the project folder
- **Mac/Linux**: Open Terminal and navigate to the project folder

### Step 3: Create Virtual Environment
```bash
python -m venv venv
```

### Step 4: Activate Virtual Environment
**Windows:**
```bash
venv\Scripts\activate
```

**Mac/Linux:**
```bash
source venv/bin/activate
```

### Step 5: Install Dependencies
```bash
pip install -r requirements.txt
```

This installs pandas, numpy, and openpyxl.

## Running the Pipeline

### Execute
```bash
python run.py
```

### What Happens
1. Pipeline generates 10,000 sample transactions (if no data exists)
2. Processes data through all stages
3. Creates outputs in `data/processed/`
4. Logs everything to `logs/`

### Execution Time
About 2-5 seconds for 10,000 records.

## Outputs

After running, check `data/processed/` for:

**CSV Files:**
- `processed_transactions.csv` - Clean data with 25+ columns
- `daily_summary_*.csv` - Daily revenue metrics
- `product_summary_*.csv` - Product performance
- `customer_summary_*.csv` - Customer analysis
- `regional_summary_*.csv` - Geographic breakdown
- `hourly_patterns_*.csv` - Time-of-day patterns

**Excel Report:**
- `analytics_report_*.xlsx` - Multi-sheet workbook

**Quality Reports:**
- `validation_report_*.json` - Data quality checks
- `summary_stats_*.csv` - Statistical summary

**Logs:**
Check `logs/` for detailed execution logs.

## Using Your Own Data

### Step 1: Prepare Your CSV
Create a file named `transactions.csv` with these columns:
```
transaction_id, timestamp, customer_id, product_id, product_category,
quantity, unit_price, total_amount, region, payment_method, discount_applied
```

### Step 2: Place in Correct Location
Save it to: `data/raw/transactions.csv`

### Step 3: Run Pipeline
```bash
python run.py
```

The pipeline will process your data instead of generating samples.

## Customization Examples

### Change Sample Data Size

Edit `src/ingestion.py`, line 56:
```python
n_records = 10000  # Change to desired number
```

### Add Custom Calculations

Edit `src/transformation.py`, around line 120:
```python
def _add_derived_columns(self, df):
    # Add your custom calculations here
    df['profit_margin'] = (df['revenue'] - df['cost']) / df['revenue']
    return df
```

### Modify Aggregations

Edit `src/aggregation.py` to add new summary views.

## Understanding the Code Structure

```
analytics-pipeline/
│
├── src/                      # All pipeline logic
│   ├── ingestion.py         # Loads data (185 lines)
│   ├── transformation.py    # Cleans data (210 lines)
│   ├── validation.py        # Quality checks (180 lines)
│   ├── aggregation.py       # Creates summaries (170 lines)
│   ├── output.py            # Saves results (140 lines)
│   └── pipeline.py          # Orchestrates everything (150 lines)
│
├── data/                     # Data storage
│   ├── raw/                 # Input files
│   └── processed/           # Output files
│
├── logs/                     # Execution logs
├── tests/                    # Unit tests (future)
├── config/                   # Configuration files (future)
│
├── run.py                    # Main execution script
├── requirements.txt          # Python dependencies
└── README.md                 # Full documentation
```

## Common Issues

### "No module named 'src'"
**Solution:** Make sure you're running from the project root directory and virtual environment is activated.

### "Permission denied"
**Solution:** On Mac/Linux, you may need: `chmod +x run.py`

### "Memory error"
**Solution:** Reduce sample size in `src/ingestion.py` or process your data in smaller chunks.

## Next Steps

1. Run with sample data to see how it works
2. Review the generated outputs
3. Check the logs to understand the process
4. Try with your own data
5. Customize transformations and aggregations
6. Add tests in the `tests/` directory

## Learning Path

**Beginner:**
- Run the pipeline as-is
- Review outputs
- Read the logs

**Intermediate:**
- Modify transformations
- Add custom aggregations
- Change validation rules

**Advanced:**
- Add database connectivity
- Implement incremental processing
- Create automated tests
- Set up scheduling

## Support

- Check `README.md` for full documentation
- Review code comments for implementation details
- Examine logs for troubleshooting

## Performance Notes

**Sample Data (10,000 records):**
- Execution time: ~2 seconds
- Memory usage: ~50 MB
- Output files: ~8 files, ~10 MB total

**Scaling:**
- 100,000 records: ~10 seconds
- 1,000,000 records: ~60 seconds
- For larger datasets, implement chunking

---

Questions? Check the full README.md for comprehensive documentation.
