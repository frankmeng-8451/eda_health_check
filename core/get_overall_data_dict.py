def get_overall_data_dict(baseline_data_collection_data, baseline_data_collection_methods):

### create a temporary baseline update list to only update the ones that are 
update_collection_list = [key for key, value in baseline_data_collection_data.items() if value is None]

### update dictionary with existing data

for baseline_data_name in update_collection_list:
  baseline_data_collection_data[baseline_data_name] = baseline_data_collection_methods[baseline_data_name](calculation_type='overall')