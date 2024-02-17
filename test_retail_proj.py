import pytest

from lib.Utils import get_spark_session
from lib.DataReader import read_customers,read_orders

def test_read_customers(spark):
    count=read_customers(spark,"LOCAL").count()
    assert count==12435
def test_read_orders(spark):
     count_order=read_orders(spark,"LOCAL").count()
     assert count_order==68884