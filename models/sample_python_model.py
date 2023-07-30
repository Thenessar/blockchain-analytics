import pyspark.sql.functions as F

def model(dbt, session):
    dbt.config(
        submission_method="cluster",
        dataproc_cluster_name="dbt-python"
    )

    stg_customers_df = dbt.ref('sample_table')

    final_df = (
        stg_customers_df.alias('customers') \
            .select(F.col('customers.id').alias('customer_id')
            )
    )

    return final_df