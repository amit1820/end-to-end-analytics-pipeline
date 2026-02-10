# src/ingestion.py
"""
Data Ingestion Module
Loads raw data from various sources
"""

import pandas as pd
import logging
from pathlib import Path
from datetime import datetime

logger = logging.getLogger(__name__)

class DataIngestion:
    """Handles data loading from multiple sources"""
    
    def __init__(self, data_dir='data/raw'):
        self.data_dir = Path(data_dir)
        self.data_dir.mkdir(parents=True, exist_ok=True)
        
    def load_raw_data(self, filename='transactions.csv'):
        """
        Load raw data from CSV file
        
        Args:
            filename: Name of the CSV file to load
            
        Returns:
            pandas.DataFrame: Raw data
        """
        file_path = self.data_dir / filename
        
        try:
            logger.info(f"Loading data from {file_path}")
            
            # Check if file exists
            if not file_path.exists():
                logger.warning(f"File not found: {file_path}")
                logger.info("Generating sample data...")
                return self._generate_sample_data()
            
            # Load CSV
            df = pd.read_csv(file_path)
            logger.info(f"Loaded {len(df)} records from {filename}")
            
            # Basic validation
            if df.empty:
                logger.error("Loaded data is empty")
                raise ValueError("Empty dataset")
                
            return df
            
        except Exception as e:
            logger.error(f"Error loading data: {str(e)}")
            raise
    
    def _generate_sample_data(self):
        """Generate sample transaction data for testing"""
        import numpy as np
        
        logger.info("Generating 10,000 sample transactions")
        
        np.random.seed(42)
        n_records = 10000
        
        # Generate dates over 2 years
        start_date = pd.Timestamp('2023-01-01')
        dates = pd.date_range(start=start_date, periods=n_records, freq='H')
        
        data = {
            'transaction_id': [f'TXN-{i:06d}' for i in range(1, n_records + 1)],
            'timestamp': dates,
            'customer_id': [f'CUST-{np.random.randint(1, 1000):04d}' for _ in range(n_records)],
            'product_id': [f'PROD-{np.random.randint(1, 50):03d}' for _ in range(n_records)],
            'product_category': np.random.choice(['Electronics', 'Clothing', 'Food', 'Home'], n_records),
            'quantity': np.random.randint(1, 10, n_records),
            'unit_price': np.random.uniform(10, 500, n_records).round(2),
            'region': np.random.choice(['North', 'South', 'East', 'West'], n_records),
            'payment_method': np.random.choice(['Credit Card', 'Debit Card', 'Cash', 'Mobile'], n_records),
            'discount_applied': np.random.choice([0, 5, 10, 15, 20], n_records),
        }
        
        df = pd.DataFrame(data)
        
        # Calculate total amount
        df['total_amount'] = (df['quantity'] * df['unit_price'] * (1 - df['discount_applied']/100)).round(2)
        
        # Add some missing values to make it realistic
        missing_indices = np.random.choice(df.index, size=int(n_records * 0.02), replace=False)
        df.loc[missing_indices, 'customer_id'] = None
        
        # Add some invalid quantities
        invalid_indices = np.random.choice(df.index, size=int(n_records * 0.01), replace=False)
        df.loc[invalid_indices, 'quantity'] = -1
        
        # Save sample data
        output_path = self.data_dir / 'transactions.csv'
        df.to_csv(output_path, index=False)
        logger.info(f"Sample data saved to {output_path}")
        
        return df
    
    def load_from_database(self, connection_string, query):
        """
        Load data from database
        
        Args:
            connection_string: Database connection string
            query: SQL query to execute
            
        Returns:
            pandas.DataFrame: Query results
        """
        try:
            logger.info("Loading data from database")
            # Placeholder for database connection
            # df = pd.read_sql(query, connection_string)
            logger.info("Database loading not implemented in this version")
            raise NotImplementedError("Database connection not configured")
            
        except Exception as e:
            logger.error(f"Database error: {str(e)}")
            raise
    
    def validate_schema(self, df, required_columns):
        """
        Validate that dataframe has required columns
        
        Args:
            df: DataFrame to validate
            required_columns: List of required column names
            
        Returns:
            bool: True if valid, raises exception otherwise
        """
        missing_columns = set(required_columns) - set(df.columns)
        
        if missing_columns:
            error_msg = f"Missing required columns: {missing_columns}"
            logger.error(error_msg)
            raise ValueError(error_msg)
        
        logger.info("Schema validation passed")
        return True
