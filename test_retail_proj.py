import pytest

from lib.Utils import get_spark_session
from lib.DataReader import read_customers,read_orders
from lib.DataManipulation import count_orders_state

def test_read_customers(spark):
    count=read_customers(spark,"LOCAL").count()
    assert count==12435
def test_read_orders(spark):
     count_order=read_orders(spark,"LOCAL").count()
     assert count_order==68884

def test_count_orders_state(spark,expected_results):
    cust_df=read_customers(spark,"LOCAL")
    result=count_orders_state(cust_df)
    assert result.collect()==expected_results.collect()

