def total_matched_impression_count(calculation_type = 'overall'):
  if calculation_type == 'overall':
    return df.filter(f.col('ehhn').isNotNull()).count
  if calculation_type == 'by_day':
    return df.filter(f.col('ehhn').isNotNull()).groupby('event_date').count
