# src/pipeline.py
"""
End-to-End Analytics Pipeline
Main orchestrator that runs the complete ETL process
"""

import logging
import sys
from pathlib import Path
from datetime import datetime

from src.ingestion import DataIngestion
from src.transformation import DataTransformation
from src.validation import DataValidation
from src.aggregation import DataAggregation
from src.output import DataOutput

def setup_logging():
    """Configure logging for the pipeline"""
    log_dir = Path("logs")
    log_dir.mkdir(exist_ok=True)
    
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    log_file = log_dir / f"pipeline_{timestamp}.log"
    
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        handlers=[
            logging.FileHandler(log_file),
            logging.StreamHandler(sys.stdout)
        ]
    )
    return logging.getLogger(__name__)

def run_pipeline():
    """
    Execute the complete analytics pipeline
    
    Returns:
        bool: True if successful, False otherwise
    """
    logger = setup_logging()
    
    logger.info("=" * 80)
    logger.info("ANALYTICS PIPELINE STARTED")
    logger.info("=" * 80)
    
    start_time = datetime.now()
    
    try:
        # STAGE 1: Data Ingestion
        logger.info("")
        logger.info("STAGE 1/5: Data Ingestion")
        logger.info("-" * 80)
        
        ingestion = DataIngestion()
        raw_data = ingestion.load_raw_data()
        
        logger.info(f"Successfully loaded {len(raw_data)} raw records")
        logger.info(f"Columns: {list(raw_data.columns)}")
        
        # STAGE 2: Data Transformation
        logger.info("")
        logger.info("STAGE 2/5: Data Transformation")
        logger.info("-" * 80)
        
        transformation = DataTransformation()
        transformed_data = transformation.transform(raw_data)
        
        # Remove duplicates
        transformed_data = transformation.remove_duplicates(
            transformed_data,
            subset=['transaction_id']
        )
        
        logger.info(f"Transformation complete: {len(transformed_data)} clean records")
        logger.info(f"Final columns: {len(transformed_data.columns)}")
        
        # STAGE 3: Data Validation
        logger.info("")
        logger.info("STAGE 3/5: Data Validation")
        logger.info("-" * 80)
        
        validation = DataValidation()
        is_valid, validation_report = validation.validate(transformed_data)
        
        if not is_valid:
            logger.error("DATA VALIDATION FAILED")
            logger.error("Validation Report:")
            for check_name, check_result in validation_report['checks'].items():
                if not check_result['passed']:
                    logger.error(f"  {check_name}: {check_result['details']}")
            
            # Save validation report even if failed
            output = DataOutput()
            output.save_validation_report(validation_report)
            
            return False
        
        logger.info("All validation checks PASSED")
        
        # Generate quality report
        quality_report = validation.generate_quality_report(transformed_data)
        logger.info(f"Data quality metrics:")
        logger.info(f"  - Memory usage: {quality_report['memory_usage_mb']} MB")
        logger.info(f"  - Columns: {quality_report['column_count']}")
        
        # STAGE 4: Data Aggregation
        logger.info("")
        logger.info("STAGE 4/5: Data Aggregation")
        logger.info("-" * 80)
        
        aggregation = DataAggregation()
        aggregations = aggregation.aggregate(transformed_data)
        
        logger.info(f"Created {len(aggregations)} aggregation views:")
        for agg_name, agg_df in aggregations.items():
            logger.info(f"  - {agg_name}: {len(agg_df)} records")
        
        # STAGE 5: Data Output
        logger.info("")
        logger.info("STAGE 5/5: Data Output")
        logger.info("-" * 80)
        
        output = DataOutput()
        
        # Save processed data
        processed_file = output.save_processed_data(
            transformed_data,
            filename='processed_transactions.csv'
        )
        logger.info(f"Processed data saved: {processed_file}")
        
        # Save aggregations
        agg_files = output.save_aggregated_data(aggregations)
        logger.info(f"Saved {len(agg_files)} aggregation files")
        
        # Save validation report
        report_file = output.save_validation_report(validation_report)
        logger.info(f"Validation report saved: {report_file}")
        
        # Save summary statistics
        stats_file = output.export_summary_statistics(transformed_data)
        logger.info(f"Summary statistics saved: {stats_file}")
        
        # Create Excel report
        excel_data = {
            'Processed_Data': transformed_data.head(1000),
            'Daily_Summary': aggregations['daily_summary'],
            'Product_Summary': aggregations['product_summary'],
            'Regional_Summary': aggregations['regional_summary']
        }
        excel_file = output.save_to_excel(excel_data)
        logger.info(f"Excel report saved: {excel_file}")
        
        # Calculate execution time
        end_time = datetime.now()
        duration = (end_time - start_time).total_seconds()
        
        # Pipeline Summary
        logger.info("")
        logger.info("=" * 80)
        logger.info("PIPELINE SUMMARY")
        logger.info("=" * 80)
        logger.info(f"Status: SUCCESS")
        logger.info(f"Execution time: {duration:.2f} seconds")
        logger.info(f"")
        logger.info(f"Data Flow:")
        logger.info(f"  Raw records:         {len(raw_data):,}")
        logger.info(f"  Transformed records: {len(transformed_data):,}")
        logger.info(f"  Data quality:        PASSED")
        logger.info(f"")
        logger.info(f"Outputs Generated:")
        logger.info(f"  - Processed data:    {processed_file.name}")
        logger.info(f"  - Aggregations:      {len(agg_files)} files")
        logger.info(f"  - Validation report: {report_file.name}")
        logger.info(f"  - Excel report:      {excel_file.name}")
        logger.info(f"  - Summary stats:     {stats_file.name}")
        logger.info("=" * 80)
        
        return True
        
    except Exception as e:
        logger.error("=" * 80)
        logger.error("PIPELINE FAILED")
        logger.error("=" * 80)
        logger.error(f"Error: {str(e)}")
        logger.exception("Full traceback:")
        logger.error("=" * 80)
        return False

if __name__ == "__main__":
    success = run_pipeline()
    sys.exit(0 if success else 1)
