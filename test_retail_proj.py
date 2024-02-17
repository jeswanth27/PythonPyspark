import pytest

from lib.Utils import get_spark_session
from lib.DataReader import read_customers,read_orders

def test_read_customers():
    print("pytest running")
    spark=get_spark_session("LOCAL")
    count=read_customers(spark,"LOCAL").count()
    assert count==12435
