import pytest

from lib.Utils import get_spark_session
from lib.DataReader import read_customers,read_orders
from lib.DataManipulation import count_orders_state, filter_orders_generic

@pytest.mark.skip("work in progress")
def test_read_customers(spark):
    count=read_customers(spark,"LOCAL").count()
    assert count==12435

@pytest.mark.transformation
def test_read_orders(spark):
     count_order=read_orders(spark,"LOCAL").count()
     assert count_order==68884

@pytest.mark.transformation
def test_count_orders_state(spark,expected_results):
    cust_df=read_customers(spark,"LOCAL")
    result=count_orders_state(cust_df)
    assert result.collect()==expected_results.collect()

@pytest.mark.latest
def test_check_closed_count(spark):
    orders_df=read_orders(spark,"LOCAL")
    fil_count=filter_orders_generic(orders_df,"CLOSED").count()
    assert fil_count==7556

@pytest.mark.latest
def test_check_complete_count(spark):
    orders_df=read_orders(spark,"LOCAL")
    fil_count=filter_orders_generic(orders_df,"COMPLETE").count()
    assert fil_count==22900

@pytest.mark.latest
def test_check_pending_payment_count(spark):
    orders_df=read_orders(spark,"LOCAL")
    fil_count=filter_orders_generic(orders_df,"PENDING_PAYMENT").count()
    assert fil_count==15030

@pytest.mark.latest
def test_check_processing_count(spark):
    orders_df=read_orders(spark,"LOCAL")
    fil_count=filter_orders_generic(orders_df,"PROCESSING").count()
    assert fil_count==8275