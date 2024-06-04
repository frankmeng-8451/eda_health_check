def read_eda_sources(cda_channel_key, options=None, sample = False):
  """
  reading eda data
  options = ['agg_direct_connect','ttd_selfserve']
  output: spark.dataframe
  """

  if options == 'agg_direct_connect':
    df = spark.read.parquet("abfss://media@sa8451camprd.dfs.core.windows.net/certified/eda/v2/aggregates/selfserve_offsite/*/*")
  if options == 'ttd_selfserve':
    df = spark.read.parquet("abfss://media@sa8451camprd.dfs.core.windows.net/certified/eda/v2/impressions/supplier_type=ttd_selfserve/")
  if options == 'eda_agg':
    df = spark.read.parquet(f"abfss://media@sa8451camprd.dfs.core.windows.net/certified/eda/v2/aggregates/selfserve_offsite/")
  if cda_channel_key != '': ## this needs to be changed later for a cda channel key spec
    df = spark.read.parquet(f"abfss://media@sa8451camprd.dfs.core.windows.net/certified/eda/v2/aggregates/selfserve_offsite/cda_channel_key={cda_channel_key}")
  
  ### sample method:
  if sample == True:
    df = df.sample(withReplacement = False, fraction = 0.1, seed = 123)
  return df
