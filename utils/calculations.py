"""
Utility Functions for IntelliStock Analytics
These functions are for VALIDATION/TESTING only.
SQL queries are the source of truth for all calculations.
"""

def calculate_avg_daily_usage(issued_list):
    """
    Calculate average daily usage from a list of issued quantities.
    
    Args:
        issued_list (list): List of daily issued quantities
        
    Returns:
        float: Average daily usage
    """
    if not issued_list or len(issued_list) == 0:
        return 0.0
    return sum(issued_list) / len(issued_list)


def calculate_days_left(closing_stock, avg_daily_usage):
    """
    Calculate days of stock remaining.
    
    Args:
        closing_stock (int): Current closing stock
        avg_daily_usage (float): Average daily usage
        
    Returns:
        float: Days of stock left (0 if no usage)
    """
    if avg_daily_usage == 0:
        return float('inf')  # Infinite stock if no usage
    return closing_stock / avg_daily_usage


def determine_risk_status(days_left, lead_time_days):
    """
    Determine risk status based on days left vs lead time.
    
    Args:
        days_left (float): Days of stock remaining
        lead_time_days (int): Supplier lead time in days
        
    Returns:
        str: 'HIGH' or 'NORMAL'
    """
    if days_left <= lead_time_days:
        return 'HIGH'
    return 'NORMAL'


def calculate_reorder_qty(lead_time_days, avg_daily_usage, closing_stock):
    """
    Calculate recommended reorder quantity.
    
    Args:
        lead_time_days (int): Supplier lead time in days
        avg_daily_usage (float): Average daily usage
        closing_stock (int): Current closing stock
        
    Returns:
        int: Reorder quantity (0 if no reorder needed)
    """
    required_stock = lead_time_days * avg_daily_usage
    reorder = required_stock - closing_stock
    return max(0, int(reorder))


def generate_explanation(row):
    """
    Generate deterministic, rule-based explanation for a stock-out alert.
    
    Args:
        row (dict): Row data containing item, organization, location, 
                   avg_daily_usage, lead_time_days, days_left
        
    Returns:
        str: Human-readable explanation
    """
    item = row.get('ITEM', row.get('item', 'Unknown Item'))
    org = row.get('ORGANIZATION', row.get('organization', 'Unknown Organization'))
    location = row.get('LOCATION', row.get('location', 'Unknown Location'))
    avg_usage = row.get('AVG_DAILY_USAGE', row.get('avg_daily_usage', 0))
    lead_time = row.get('LEAD_TIME_DAYS', row.get('lead_time_days', 0))
    days_left = row.get('DAYS_LEFT', row.get('days_left', 0))
    
    explanation = (
        f"{item} at {org} – {location} is at high risk of stock-out. "
        f"Average daily usage is {avg_usage:.1f} units with a supplier lead time of {lead_time} days. "
        f"Current stock will last approximately {days_left:.1f} days, "
        f"which is insufficient to cover the lead time period."
    )
    
    return explanation


def get_urgency_level(days_left, lead_time_days):
    """
    Determine urgency level for reorder recommendations.
    
    Args:
        days_left (float): Days of stock remaining
        lead_time_days (int): Supplier lead time in days
        
    Returns:
        str: 'CRITICAL', 'HIGH', or 'MEDIUM'
    """
    if days_left <= 0:
        return 'CRITICAL'
    elif days_left <= lead_time_days * 0.5:
        return 'CRITICAL'
    elif days_left <= lead_time_days:
        return 'HIGH'
    else:
        return 'MEDIUM'


# Validation function to compare Python vs SQL calculations
def validate_calculations(python_result, sql_result, tolerance=0.01):
    """
    Validate that Python calculations match SQL results.
    
    Args:
        python_result (float): Result from Python function
        sql_result (float): Result from SQL query
        tolerance (float): Acceptable difference
        
    Returns:
        bool: True if results match within tolerance
    """
    if python_result is None and sql_result is None:
        return True
    if python_result is None or sql_result is None:
        return False
    
    # Handle infinity
    if python_result == float('inf') and sql_result == float('inf'):
        return True
    if python_result == float('inf') or sql_result == float('inf'):
        return False
    
    return abs(python_result - sql_result) <= tolerance


if __name__ == "__main__":
    """
    Test the utility functions with sample data.
    """
    print("Testing IntelliStock Utility Functions\n")
    
    # Test data
    issued_list = [10, 15, 12, 8, 20]
    closing_stock = 50
    lead_time = 7
    
    # Calculate metrics
    avg_usage = calculate_avg_daily_usage(issued_list)
    days_left = calculate_days_left(closing_stock, avg_usage)
    risk = determine_risk_status(days_left, lead_time)
    reorder = calculate_reorder_qty(lead_time, avg_usage, closing_stock)
    urgency = get_urgency_level(days_left, lead_time)
    
    print(f"Average Daily Usage: {avg_usage:.2f} units")
    print(f"Days Left: {days_left:.2f} days")
    print(f"Risk Status: {risk}")
    print(f"Reorder Quantity: {reorder} units")
    print(f"Urgency Level: {urgency}")
    
    # Test explanation
    test_row = {
        'item': 'Paracetamol',
        'organization': 'City Hospital',
        'location': 'Main Warehouse',
        'avg_daily_usage': avg_usage,
        'lead_time_days': lead_time,
        'days_left': days_left
    }
    
    print(f"\nExplanation:\n{generate_explanation(test_row)}")
