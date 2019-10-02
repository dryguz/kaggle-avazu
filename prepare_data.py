import pandas as pd
import random
import string
import os
import pyarrow
import argparse


def random_string(string_length=10):
    """Generate a random string of fixed length """
    letters = string.ascii_lowercase
    return ''.join(random.choice(letters) for i in range(string_length))


def split_date_file(file):
    file_path = os.path.join('data', file)
    reader = pd.read_csv(file_path, chunksize=30000)
    for chunk in reader:
        chunk.hour = pd.to_datetime(chunk.hour, format='%y%m%d%H')
        grouped_sample = chunk.groupby('hour')
        for name, sample in grouped_sample:
            date = 'date={}'.format(name.date())
            hour = 'time={:02d}'.format(name.hour)
            path = os.path.join('data', 'split_by_date', date, hour)
            if not os.path.isdir(path):
                os.makedirs(path)
            name = random_string(10)
            file_path = os.path.join(path, name+'.parquet')
            uint65_to_str = {i: 'str' for i in sample.dtypes[sample.dtypes == 'uint64'].index}
            sample = sample.astype(uint65_to_str)
            sample.to_parquet(fname=file_path, engine='pyarrow', compression='snappy', index=False)


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Get file name in data folder')
    parser.add_argument('--file', type=str, help='provide a name of file in data folder to split by date')
    args = parser.parse_args()

    file = args.file
    split_date_file(file)
    print('Finished.')
