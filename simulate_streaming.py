import os
import time
import datetime
import argparse


def list_days_and_hours(folder):

    if not os.path.isdir(folder):
        os.makedirs(folder)

    days = os.listdir(folder)
    days_and_hours = []

    for day in days:
        day_path = os.path.join(folder, day)
        hours = os.listdir(day_path)
        days_and_hours += [os.path.join(day_path, hour) for hour in hours]
    days_and_hours.sort()

    return days_and_hours


def copy_another_hour_of_data(root_folder, input_folder):
    root_days_and_hours = list_days_and_hours(root_folder)
    input_days_and_hours = list_days_and_hours(input_folder)
    to_copy_list = list(set(root_days_and_hours) - set(input_days_and_hours))
    to_copy_list.sort()
    return to_copy_list[0]


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Get number of seconds for copying next hour of input data')
    parser.add_argument('--file', type=str, help='provide a time window in seconds for copying hour of data')
    args = parser.parse_args()

    time_window = args.file

    t0 = datetime.datetime.now()
    while True:
        t1 = datetime.datetime.now()
        if (t1 - t0).seconds // time_window > 0:
            copy_another_hour_of_data('data/split_by_date', 'data/input_data')
            t0 = datetime.datetime.now()
        else:
            time.sleep(600)



