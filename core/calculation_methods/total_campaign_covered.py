def total_campaign_covered(calculation_type = 'overall'):
  if calculation_type == 'overall':
    return df.select('cda_campaign_key').distinct().count()
  