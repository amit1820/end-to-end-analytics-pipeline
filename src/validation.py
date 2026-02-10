# src/validation.py
"""
Data Validation Module
Validates data quality and integrity
"""

import pandas as pd
import numpy as np
import logging

logger = logging.getLogger(__name__)

class DataValidation:
    """Performs data quality checks"""
    
    def __init__(self):
        self.validation_rules = {
            'completeness': self._check_completeness,
            'uniqueness': self._check_uniqueness,
            'consistency': self._check_consistency,
            'accuracy': self._check_accuracy
        }
    
    def validate(self, df):
        """
        Run all validation checks
        
        Args:
            df: DataFrame to validate
            
        Returns:
            tuple: (is_valid, validation_report)
        """
        logger.info("Starting data validation")
        
        report = {
            'timestamp': pd.Timestamp.now(),
            'total_records': len(df),
            'checks': {}
        }
        
        all_passed = True
        
        # Run each validation check
        for check_name, check_func in self.validation_rules.items():
            try:
                passed, details = check_func(df)
                report['checks'][check_name] = {
                    'passed': passed,
                    'details': details
                }
                
                if not passed:
                    all_passed = False
                    logger.warning(f"Validation check '{check_name}' FAILED: {details}")
                else:
                    logger.info(f"Validation check '{check_name}' passed")
                    
            except Exception as e:
                logger.error(f"Error in {check_name} check: {str(e)}")
                report['checks'][check_name] = {
                    'passed': False,
                    'details': f"Error: {str(e)}"
                }
                all_passed = False
        
        report['overall_status'] = 'PASSED' if all_passed else 'FAILED'
        
        logger.info(f"Validation complete: {report['overall_status']}")
        return all_passed, report
    
    def _check_completeness(self, df):
        """Check for missing values in critical columns"""
        critical_columns = ['transaction_id', 'timestamp', 'total_amount']
        
        missing_counts = {}
        has_issues = False
        
        for col in critical_columns:
            if col in df.columns:
                missing = df[col].isna().sum()
                missing_pct = (missing / len(df)) * 100
                
                missing_counts[col] = {
                    'count': int(missing),
                    'percentage': round(missing_pct, 2)
                }
                
                if missing > 0:
                    has_issues = True
        
        passed = not has_issues
        details = missing_counts if has_issues else "No missing values in critical columns"
        
        return passed, details
    
    def _check_uniqueness(self, df):
        """Check for duplicate transaction IDs"""
        if 'transaction_id' not in df.columns:
            return True, "Transaction ID column not found"
        
        duplicates = df['transaction_id'].duplicated().sum()
        duplicate_pct = (duplicates / len(df)) * 100
        
        passed = duplicates == 0
        details = {
            'duplicate_count': int(duplicates),
            'duplicate_percentage': round(duplicate_pct, 2)
        }
        
        return passed, details
    
    def _check_consistency(self, df):
        """Check data consistency"""
        issues = []
        
        # Check if calculated totals match
        if all(col in df.columns for col in ['quantity', 'unit_price', 'discount_applied', 'total_amount']):
            df['calculated_total'] = df['quantity'] * df['unit_price'] * (1 - df['discount_applied']/100)
            
            # Allow small floating point differences
            mismatch = abs(df['total_amount'] - df['calculated_total']) > 0.01
            mismatch_count = mismatch.sum()
            
            if mismatch_count > 0:
                issues.append(f"{mismatch_count} records with total amount mismatches")
        
        # Check date consistency
        if 'timestamp' in df.columns:
            future_dates = df['timestamp'] > pd.Timestamp.now()
            future_count = future_dates.sum()
            
            if future_count > 0:
                issues.append(f"{future_count} records with future timestamps")
        
        # Check for negative values where they shouldn't exist
        numeric_cols = ['quantity', 'unit_price', 'total_amount']
        for col in numeric_cols:
            if col in df.columns:
                negative_count = (df[col] < 0).sum()
                if negative_count > 0:
                    issues.append(f"{negative_count} negative values in {col}")
        
        passed = len(issues) == 0
        details = issues if issues else "All consistency checks passed"
        
        return passed, details
    
    def _check_accuracy(self, df):
        """Check data accuracy and reasonable ranges"""
        issues = []
        
        # Check for unrealistic quantities
        if 'quantity' in df.columns:
            high_qty = (df['quantity'] > 1000).sum()
            if high_qty > 0:
                issues.append(f"{high_qty} records with unusually high quantities (>1000)")
        
        # Check for unrealistic prices
        if 'unit_price' in df.columns:
            high_price = (df['unit_price'] > 10000).sum()
            if high_price > 0:
                issues.append(f"{high_price} records with very high prices (>$10,000)")
        
        # Check for unrealistic discounts
        if 'discount_applied' in df.columns:
            invalid_discount = ((df['discount_applied'] < 0) | (df['discount_applied'] > 100)).sum()
            if invalid_discount > 0:
                issues.append(f"{invalid_discount} records with invalid discount percentages")
        
        passed = len(issues) == 0
        details = issues if issues else "All accuracy checks passed"
        
        return passed, details
    
    def generate_quality_report(self, df):
        """
        Generate comprehensive data quality report
        
        Args:
            df: DataFrame to analyze
            
        Returns:
            dict: Quality metrics
        """
        report = {
            'record_count': len(df),
            'column_count': len(df.columns),
            'memory_usage_mb': round(df.memory_usage(deep=True).sum() / 1024**2, 2),
            'column_info': {}
        }
        
        for col in df.columns:
            report['column_info'][col] = {
                'dtype': str(df[col].dtype),
                'missing_count': int(df[col].isna().sum()),
                'missing_percentage': round((df[col].isna().sum() / len(df)) * 100, 2),
                'unique_count': int(df[col].nunique())
            }
            
            # Add statistics for numeric columns
            if pd.api.types.is_numeric_dtype(df[col]):
                report['column_info'][col]['min'] = float(df[col].min())
                report['column_info'][col]['max'] = float(df[col].max())
                report['column_info'][col]['mean'] = round(float(df[col].mean()), 2)
        
        return report
