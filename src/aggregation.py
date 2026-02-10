# src/aggregation.py
"""
Data Aggregation Module
Creates analytical aggregations and summaries
"""

import pandas as pd
import logging

logger = logging.getLogger(__name__)

class DataAggregation:
    """Creates various data aggregations"""
    
    def aggregate(self, df):
        """
        Create multiple aggregation views
        
        Args:
            df: Cleaned DataFrame
            
        Returns:
            dict: Dictionary of aggregated DataFrames
        """
        logger.info("Starting data aggregation")
        
        aggregations = {
            'daily_summary': self._aggregate_daily(df),
            'product_summary': self._aggregate_by_product(df),
            'customer_summary': self._aggregate_by_customer(df),
            'regional_summary': self._aggregate_by_region(df),
            'hourly_patterns': self._aggregate_by_hour(df)
        }
        
        logger.info(f"Created {len(aggregations)} aggregation views")
        return aggregations
    
    def _aggregate_daily(self, df):
        """Daily revenue and transaction summary"""
        logger.info("Creating daily aggregation")
        
        if 'date' not in df.columns:
            logger.warning("Date column not found")
            return pd.DataFrame()
        
        daily = df.groupby('date').agg({
            'transaction_id': 'count',
            'total_amount': ['sum', 'mean', 'median'],
            'quantity': 'sum',
            'discount_amount': 'sum',
            'customer_id': 'nunique'
        }).reset_index()
        
        # Flatten column names
        daily.columns = ['date', 'transaction_count', 'total_revenue', 
                        'avg_transaction', 'median_transaction', 
                        'total_quantity', 'total_discounts', 'unique_customers']
        
        # Add derived metrics
        daily['avg_items_per_transaction'] = daily['total_quantity'] / daily['transaction_count']
        daily['discount_rate'] = (daily['total_discounts'] / daily['total_revenue']) * 100
        
        logger.info(f"Daily aggregation: {len(daily)} days")
        return daily
    
    def _aggregate_by_product(self, df):
        """Product-level performance summary"""
        logger.info("Creating product aggregation")
        
        if 'product_id' not in df.columns:
            logger.warning("Product ID column not found")
            return pd.DataFrame()
        
        products = df.groupby(['product_id', 'product_category']).agg({
            'transaction_id': 'count',
            'quantity': 'sum',
            'total_amount': 'sum',
            'unit_price': 'mean',
            'discount_applied': 'mean',
            'customer_id': 'nunique'
        }).reset_index()
        
        products.columns = ['product_id', 'category', 'transaction_count', 
                           'total_quantity_sold', 'total_revenue', 
                           'avg_unit_price', 'avg_discount', 'unique_customers']
        
        # Calculate revenue share
        products['revenue_share'] = (products['total_revenue'] / products['total_revenue'].sum()) * 100
        
        # Sort by revenue
        products = products.sort_values('total_revenue', ascending=False)
        
        logger.info(f"Product aggregation: {len(products)} products")
        return products
    
    def _aggregate_by_customer(self, df):
        """Customer-level behavior summary"""
        logger.info("Creating customer aggregation")
        
        if 'customer_id' not in df.columns:
            logger.warning("Customer ID column not found")
            return pd.DataFrame()
        
        # Filter out unknown customers
        df_known = df[df['customer_id'] != 'CUST-0000'].copy()
        
        customers = df_known.groupby('customer_id').agg({
            'transaction_id': 'count',
            'total_amount': ['sum', 'mean'],
            'quantity': 'sum',
            'timestamp': ['min', 'max']
        }).reset_index()
        
        customers.columns = ['customer_id', 'transaction_count', 'total_spent', 
                            'avg_transaction_value', 'total_items_purchased',
                            'first_purchase', 'last_purchase']
        
        # Calculate customer lifetime
        customers['days_active'] = (customers['last_purchase'] - customers['first_purchase']).dt.days
        
        # Customer segmentation
        customers['customer_segment'] = pd.cut(
            customers['total_spent'],
            bins=[0, 1000, 5000, 10000, float('inf')],
            labels=['Bronze', 'Silver', 'Gold', 'Platinum']
        )
        
        # Sort by total spent
        customers = customers.sort_values('total_spent', ascending=False)
        
        logger.info(f"Customer aggregation: {len(customers)} customers")
        return customers
    
    def _aggregate_by_region(self, df):
        """Regional performance summary"""
        logger.info("Creating regional aggregation")
        
        if 'region' not in df.columns:
            logger.warning("Region column not found")
            return pd.DataFrame()
        
        regions = df.groupby('region').agg({
            'transaction_id': 'count',
            'total_amount': ['sum', 'mean'],
            'quantity': 'sum',
            'customer_id': 'nunique',
            'discount_applied': 'mean'
        }).reset_index()
        
        regions.columns = ['region', 'transaction_count', 'total_revenue', 
                          'avg_transaction_value', 'total_quantity', 
                          'unique_customers', 'avg_discount']
        
        # Calculate metrics
        regions['revenue_per_customer'] = regions['total_revenue'] / regions['unique_customers']
        regions['transactions_per_customer'] = regions['transaction_count'] / regions['unique_customers']
        
        # Calculate regional share
        regions['revenue_share'] = (regions['total_revenue'] / regions['total_revenue'].sum()) * 100
        
        # Sort by revenue
        regions = regions.sort_values('total_revenue', ascending=False)
        
        logger.info(f"Regional aggregation: {len(regions)} regions")
        return regions
    
    def _aggregate_by_hour(self, df):
        """Hourly transaction patterns"""
        logger.info("Creating hourly aggregation")
        
        if 'hour' not in df.columns:
            logger.warning("Hour column not found")
            return pd.DataFrame()
        
        hourly = df.groupby('hour').agg({
            'transaction_id': 'count',
            'total_amount': ['sum', 'mean'],
            'quantity': 'sum'
        }).reset_index()
        
        hourly.columns = ['hour', 'transaction_count', 'total_revenue', 
                         'avg_transaction_value', 'total_quantity']
        
        # Calculate percentage of daily volume
        hourly['transaction_share'] = (hourly['transaction_count'] / hourly['transaction_count'].sum()) * 100
        
        logger.info("Hourly aggregation complete")
        return hourly
    
    def create_pivot_table(self, df, index, columns, values, aggfunc='sum'):
        """
        Create custom pivot table
        
        Args:
            df: DataFrame
            index: Row index
            columns: Column headers
            values: Values to aggregate
            aggfunc: Aggregation function
            
        Returns:
            pandas.DataFrame: Pivot table
        """
        logger.info(f"Creating pivot: {index} x {columns}")
        
        try:
            pivot = pd.pivot_table(
                df,
                index=index,
                columns=columns,
                values=values,
                aggfunc=aggfunc,
                fill_value=0
            )
            
            return pivot
            
        except Exception as e:
            logger.error(f"Error creating pivot table: {str(e)}")
            raise
