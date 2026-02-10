# src/transformation.py
"""
Data Transformation Module
Cleans and transforms raw data
"""

import pandas as pd
import numpy as np
import logging
from datetime import datetime

logger = logging.getLogger(__name__)

class DataTransformation:
    """Handles data cleaning and transformation"""
    
    def transform(self, df):
        """
        Apply all transformations to raw data
        
        Args:
            df: Raw DataFrame
            
        Returns:
            pandas.DataFrame: Transformed data
        """
        logger.info("Starting data transformation")
        
        # Create a copy to avoid modifying original
        df_clean = df.copy()
        
        # Apply transformations in sequence
        df_clean = self._handle_missing_values(df_clean)
        df_clean = self._fix_data_types(df_clean)
        df_clean = self._clean_invalid_values(df_clean)
        df_clean = self._add_derived_columns(df_clean)
        df_clean = self._standardize_text(df_clean)
        
        logger.info(f"Transformation complete: {len(df_clean)} records")
        return df_clean
    
    def _handle_missing_values(self, df):
        """Handle missing values based on business logic"""
        logger.info("Handling missing values")
        
        initial_rows = len(df)
        
        # Drop rows with missing critical fields
        critical_fields = ['transaction_id', 'timestamp', 'total_amount']
        df = df.dropna(subset=critical_fields)
        
        # Fill missing customer_id with 'UNKNOWN'
        if 'customer_id' in df.columns:
            df['customer_id'] = df['customer_id'].fillna('CUST-0000')
        
        # Fill missing product_id with 'UNKNOWN'
        if 'product_id' in df.columns:
            df['product_id'] = df['product_id'].fillna('PROD-000')
        
        dropped_rows = initial_rows - len(df)
        if dropped_rows > 0:
            logger.info(f"Dropped {dropped_rows} rows with missing critical values")
        
        return df
    
    def _fix_data_types(self, df):
        """Ensure correct data types"""
        logger.info("Fixing data types")
        
        # Convert timestamp to datetime
        if 'timestamp' in df.columns:
            df['timestamp'] = pd.to_datetime(df['timestamp'], errors='coerce')
        
        # Convert numeric columns
        numeric_columns = ['quantity', 'unit_price', 'total_amount', 'discount_applied']
        for col in numeric_columns:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors='coerce')
        
        # Convert categorical columns to string
        categorical_columns = ['product_category', 'region', 'payment_method']
        for col in categorical_columns:
            if col in df.columns:
                df[col] = df[col].astype(str)
        
        return df
    
    def _clean_invalid_values(self, df):
        """Remove or fix invalid values"""
        logger.info("Cleaning invalid values")
        
        initial_rows = len(df)
        
        # Remove negative quantities
        if 'quantity' in df.columns:
            df = df[df['quantity'] > 0]
        
        # Remove negative prices
        if 'unit_price' in df.columns:
            df = df[df['unit_price'] > 0]
        
        # Remove negative totals
        if 'total_amount' in df.columns:
            df = df[df['total_amount'] > 0]
        
        # Cap discount at 100%
        if 'discount_applied' in df.columns:
            df['discount_applied'] = df['discount_applied'].clip(0, 100)
        
        cleaned_rows = initial_rows - len(df)
        if cleaned_rows > 0:
            logger.info(f"Removed {cleaned_rows} rows with invalid values")
        
        return df
    
    def _add_derived_columns(self, df):
        """Add calculated and derived columns"""
        logger.info("Adding derived columns")
        
        # Extract date components
        if 'timestamp' in df.columns:
            df['date'] = df['timestamp'].dt.date
            df['year'] = df['timestamp'].dt.year
            df['month'] = df['timestamp'].dt.month
            df['day'] = df['timestamp'].dt.day
            df['hour'] = df['timestamp'].dt.hour
            df['day_of_week'] = df['timestamp'].dt.day_name()
            df['week_of_year'] = df['timestamp'].dt.isocalendar().week
            df['is_weekend'] = df['timestamp'].dt.dayofweek.isin([5, 6])
        
        # Calculate revenue metrics
        if 'quantity' in df.columns and 'unit_price' in df.columns:
            df['gross_revenue'] = df['quantity'] * df['unit_price']
            
            if 'discount_applied' in df.columns:
                df['discount_amount'] = df['gross_revenue'] * (df['discount_applied'] / 100)
                df['net_revenue'] = df['gross_revenue'] - df['discount_amount']
        
        # Add categorical flags
        if 'discount_applied' in df.columns:
            df['has_discount'] = df['discount_applied'] > 0
            df['discount_tier'] = pd.cut(
                df['discount_applied'],
                bins=[0, 5, 10, 20, 100],
                labels=['None', 'Low', 'Medium', 'High']
            )
        
        # Add price tier
        if 'unit_price' in df.columns:
            df['price_tier'] = pd.cut(
                df['unit_price'],
                bins=[0, 50, 100, 200, 1000],
                labels=['Budget', 'Standard', 'Premium', 'Luxury']
            )
        
        logger.info(f"Added {len(df.columns)} total columns")
        return df
    
    def _standardize_text(self, df):
        """Standardize text fields"""
        logger.info("Standardizing text fields")
        
        text_columns = ['product_category', 'region', 'payment_method', 'day_of_week']
        
        for col in text_columns:
            if col in df.columns:
                # Strip whitespace and standardize case
                df[col] = df[col].str.strip().str.title()
        
        return df
    
    def remove_duplicates(self, df, subset=None):
        """
        Remove duplicate records
        
        Args:
            df: DataFrame
            subset: Columns to consider for duplicates
            
        Returns:
            pandas.DataFrame: Deduplicated data
        """
        initial_rows = len(df)
        
        if subset:
            df = df.drop_duplicates(subset=subset, keep='first')
        else:
            df = df.drop_duplicates(keep='first')
        
        duplicates_removed = initial_rows - len(df)
        if duplicates_removed > 0:
            logger.info(f"Removed {duplicates_removed} duplicate records")
        
        return df
