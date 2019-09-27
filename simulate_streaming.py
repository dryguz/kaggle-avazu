import os
import time
import datetime
import argparse


root_folder = 'data/split_by_date'
days = os.listdir(root_folder)
days_and_hours = []

for day in days:
    day_path = os.path.join(root_folder, day)
    hours = os.listdir(day_path)
    days_and_hours += [os.path.join(day_path, hour) for hour in hours]

days_and_hours.sort()

ADD_EVERY_X_TIME = 1  # how many hours of data to add every x minutes to folder


def copy_another_hour_of_data():
    pass


input_data_folder = os.path.join('data', 'input_data')

if not os.path.isdir(input_data_folder):
    os.makedirs(input_data_folder)

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Get number of seconds for copying next hour of input data')
    parser.add_argument('--file', type=str, help='provide a time window in seconds for copying hour of data')
    args = parser.parse_args()

    time_window = args.file

    t0 = datetime.datetime.now()
    while True:
        t1 = datetime.datetime.now()
        if (t1 - t0).seconds // time_window > 0:
            copy_another_hour_of_data()
            t0 = datetime.datetime.now()
        else:
            time.sleep(600)



