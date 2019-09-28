import os
import time
import datetime
import argparse
import shutil


def list_days_and_hours(folder):

    if not os.path.isdir(folder):
        os.makedirs(folder)

    days = os.listdir(folder)
    days_and_hours = []

    for day in days:
        hours = os.listdir(os.path.join(folder, day))
        days_and_hours += [os.path.join(day, hour) for hour in hours]
    days_and_hours.sort()

    return days_and_hours


def copy_another_hour_of_data(root_folder, input_folder):
    root_days_and_hours = list_days_and_hours(root_folder)
    input_days_and_hours = list_days_and_hours(input_folder)
    to_copy_list = list(set(root_days_and_hours) - set(input_days_and_hours))
    to_copy_list.sort()
    src = os.path.join(root_folder, to_copy_list[0])
    dst = os.path.join(input_folder, to_copy_list[0])
    shutil.copytree(src=src, dst=dst)
    return to_copy_list[0]


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Get number of seconds for copying next hour of input data')
    parser.add_argument('--tw', type=int, help='provide a time window in seconds for copying hour of data')
    parser.add_argument('--sleep', type=int, help='how log to wait before check again if time window past')
    args = parser.parse_args()

    time_window = args.tw
    sleep = args.sleep

    t0 = datetime.datetime.now()
    while True:
        t1 = datetime.datetime.now()
        if (t1 - t0).seconds // time_window > 0:
            copied = copy_another_hour_of_data('data/split_by_date', 'data/input_data')
            print('Copied folder {}'.format(copied))
            t0 = datetime.datetime.now()
        else:
            print('Sleeping for {} minutes, then checking again'.format(sleep/60))
            time.sleep(sleep)



