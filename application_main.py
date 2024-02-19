import sys
from lib import DataManipulation, DataReader, Utils
from pyspark.sql.functions import *
if __name__ == '__main__':
    if len(sys.argv) < 1:
        print("Please specify the environment")
        sys.exit(-1)
    job_run_env = 'PROD'
    print("Creating Spark Session")
    spark = Utils.get_spark_session(job_run_env)
    print("Created Spark Session")
    orders_df = DataReader.read_orders(spark,job_run_env)
    count=DataManipulation.count_customer(orders_df)
    print(f"Number of customer {count}")
    orders_filtered = DataManipulation.filter_closed_orders(orders_df)
    customers_df = DataReader.read_customers(spark,job_run_env)

    # count customers for SC
    count1=DataManipulation.count_order_stateind(customers_df)
    print(f"count of state=SC orders {count1}")

    joined_df =DataManipulation.join_orders_customers(orders_filtered,customers_df)
    aggregated_results = DataManipulation.count_orders_state(joined_df)
    aggregated_results.show()
    print("end of main")