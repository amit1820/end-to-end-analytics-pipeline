# run.py
"""
Pipeline Runner
Execute this file to run the complete analytics pipeline
"""

from src.pipeline import run_pipeline

if __name__ == "__main__":
    print("\nStarting Analytics Pipeline...")
    print("This will process transaction data through 5 stages:\n")
    print("1. Data Ingestion")
    print("2. Data Transformation")
    print("3. Data Validation")
    print("4. Data Aggregation")
    print("5. Data Output\n")
    
    success = run_pipeline()
    
    if success:
        print("\nPipeline completed successfully!")
        print("Check the following directories for outputs:")
        print("  - data/processed/ - Processed data and aggregations")
        print("  - logs/ - Execution logs")
    else:
        print("\nPipeline failed. Check logs/ directory for details.")
