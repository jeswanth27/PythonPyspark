import pytest
from lib.Utils import get_spark_session 
@pytest.fixture
def spark():
    "createsasparksession"
    spark_session= get_spark_session("LOCAL")
    yield spark_session
    spark_session.stop()

@pytest.fixture
def expected_results(spark):
    "gives the expected results"
    result_schema="state string,count int"
    return spark.read \
        .option("header",True) \
        .format("csv") \
        .schema(result_schema) \
        .load("data/state_aggregate2.csv")