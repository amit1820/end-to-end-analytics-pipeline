# src/output.py
"""
Data Output Module
Saves processed data to various formats
"""

import pandas as pd
import logging
import json
from pathlib import Path
from datetime import datetime

logger = logging.getLogger(__name__)

class DataOutput:
    """Handles data export and persistence"""
    
    def __init__(self, output_dir='data/processed'):
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(parents=True, exist_ok=True)
        
    def save_processed_data(self, df, filename=None):
        """
        Save processed data to CSV
        
        Args:
            df: DataFrame to save
            filename: Optional custom filename
            
        Returns:
            Path: Path to saved file
        """
        if filename is None:
            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
            filename = f'processed_data_{timestamp}.csv'
        
        output_path = self.output_dir / filename
        
        try:
            logger.info(f"Saving processed data to {output_path}")
            df.to_csv(output_path, index=False)
            logger.info(f"Saved {len(df)} records")
            return output_path
            
        except Exception as e:
            logger.error(f"Error saving data: {str(e)}")
            raise
    
    def save_aggregated_data(self, aggregations_dict):
        """
        Save all aggregations to separate files
        
        Args:
            aggregations_dict: Dictionary of aggregated DataFrames
            
        Returns:
            list: Paths to saved files
        """
        saved_files = []
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        
        for agg_name, agg_df in aggregations_dict.items():
            filename = f'{agg_name}_{timestamp}.csv'
            output_path = self.output_dir / filename
            
            try:
                logger.info(f"Saving {agg_name} aggregation")
                agg_df.to_csv(output_path, index=False)
                saved_files.append(output_path)
                logger.info(f"Saved {len(agg_df)} records to {filename}")
                
            except Exception as e:
                logger.error(f"Error saving {agg_name}: {str(e)}")
        
        return saved_files
    
    def save_validation_report(self, report, filename=None):
        """
        Save validation report as JSON
        
        Args:
            report: Validation report dictionary
            filename: Optional custom filename
            
        Returns:
            Path: Path to saved file
        """
        if filename is None:
            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
            filename = f'validation_report_{timestamp}.json'
        
        output_path = self.output_dir / filename
        
        try:
            logger.info(f"Saving validation report to {output_path}")
            
            # Convert report to JSON-serializable format
            report_copy = self._make_json_serializable(report)
            
            with open(output_path, 'w') as f:
                json.dump(report_copy, f, indent=2)
            
            logger.info("Validation report saved")
            return output_path
            
        except Exception as e:
            logger.error(f"Error saving validation report: {str(e)}")
            raise
    
    def _make_json_serializable(self, obj):
        """
        Recursively convert object to JSON-serializable format
        
        Args:
            obj: Object to convert
            
        Returns:
            JSON-serializable object
        """
        if isinstance(obj, dict):
            return {key: self._make_json_serializable(value) for key, value in obj.items()}
        elif isinstance(obj, list):
            return [self._make_json_serializable(item) for item in obj]
        elif isinstance(obj, (bool, int, float, str, type(None))):
            return obj
        elif isinstance(obj, (pd.Timestamp, datetime)):
            return str(obj)
        elif isinstance(obj, (pd.Series, pd.DataFrame)):
            return obj.to_dict()
        else:
            # Convert any other type to string
            return str(obj)
    
    def save_to_excel(self, data_dict, filename=None):
        """
        Save multiple DataFrames to Excel with separate sheets
        
        Args:
            data_dict: Dictionary of DataFrames
            filename: Optional custom filename
            
        Returns:
            Path: Path to saved file
        """
        if filename is None:
            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
            filename = f'analytics_report_{timestamp}.xlsx'
        
        output_path = self.output_dir / filename
        
        try:
            logger.info(f"Saving Excel report to {output_path}")
            
            with pd.ExcelWriter(output_path, engine='openpyxl') as writer:
                for sheet_name, df in data_dict.items():
                    # Excel sheet names limited to 31 characters
                    safe_sheet_name = sheet_name[:31]
                    df.to_excel(writer, sheet_name=safe_sheet_name, index=False)
                    logger.info(f"Added sheet: {safe_sheet_name}")
            
            logger.info("Excel report saved")
            return output_path
            
        except Exception as e:
            logger.error(f"Error saving Excel file: {str(e)}")
            raise
    
    def export_summary_statistics(self, df, filename=None):
        """
        Export summary statistics
        
        Args:
            df: DataFrame to summarize
            filename: Optional custom filename
            
        Returns:
            Path: Path to saved file
        """
        if filename is None:
            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
            filename = f'summary_stats_{timestamp}.csv'
        
        output_path = self.output_dir / filename
        
        try:
            logger.info("Generating summary statistics")
            
            # Get numeric columns only
            numeric_cols = df.select_dtypes(include=['number']).columns
            
            if len(numeric_cols) == 0:
                logger.warning("No numeric columns found")
                return None
            
            # Calculate statistics
            stats = df[numeric_cols].describe()
            
            # Save to CSV
            stats.to_csv(output_path)
            logger.info(f"Summary statistics saved to {output_path}")
            return output_path
            
        except Exception as e:
            logger.error(f"Error exporting summary statistics: {str(e)}")
            raise
