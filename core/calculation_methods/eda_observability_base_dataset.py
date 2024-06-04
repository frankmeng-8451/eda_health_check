from pyspark.sql import functions as f 
### save the data under /Workspace/Shared/media_data_analyst/data_observability/eda/sample_data/sample_data.parquet
def eda_observability_base_dataset(df):

 return df.groupby('supplier_type','cda_channel_key','cda_campaign_key','event_date').agg(
    f.mean('date_diff').alias('avg_date_diff'),
    f.count(f.col('impression_id')).alias('Impressions'),
    f.sum(f.when(f.col('ehhn').isNotNull(),1).otherwise(0)).alias('Matched_Impression_count'),
    f.countDistinct(f.col('ehhn')).alias('Unique EHHN'),
    f.countDistinct(f.col('impression_id').isNull()).alias('UNIQUE_EHHN_NULL_CNT'), 
    f.count(f.when((f.col("user_identifier_type") == "TTD IDL") & f.col("user_identifier").isNotNull(), f.col("user_identifier"))).alias('IDL_IMPRESSION'),
    f.sum(f.col('media_cost_usd')).alias('MEDIA_COST'),
    f.sum(f.when(f.col('ehhn').isNull(),1).otherwise(0)).alias('NULL_Matched_Impression_CNT'),
    f.sum(f.when(f.col('salesforce_id').isNull(),1).otherwise(0)).alias('Null_Salesforce_id_CNT'),
    # f.count(f.col('KPM_DUPLICATED_ID').isNull()).alias('Null_KPM_DUPLICATED_ID_CNT'),
    f.sum(f.when(f.col('impression_id').isNull(),1).otherwise(0)).alias('Null_Impression_ID_CNT')
    )