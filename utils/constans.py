import os
# columns types

timestamp_fields = ['hour']

date_fields = ['date']

long_fields = ['click', 'time']


string_fields = ['id', 'C1', 'banner_pos', 'site_id', 'site_domain', 'site_category', 'app_id',
                 'app_domain', 'app_category', 'device_id', 'device_ip', 'device_model', 'device_type',
                 'device_conn_type', 'C14', 'C15', 'C16', 'C17', 'C18', 'C19', 'C20', 'C21']

# data paths
static_data = os.path.join('data', 'split_by_date')
streaming_data_path = os.path.join('data', 'input_data')
