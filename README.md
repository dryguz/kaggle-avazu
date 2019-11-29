# kaggle-avazu

Exercise project for testing

## Data preparation
Data are provided in one big csv file. 
[File description under this link](https://www.kaggle.com/c/avazu-ctr-prediction/data)

### prepare_data.py
Script splits the data in file "/data/file.csv" by day and by hour.
The results are folders with parquet files.
Source file needs to be in data folder, which is included in .gitignore

Script output folder's schema is:
"data=xxxx-xx-xx"/"hour=xx"/"parqets_files_with_random_string_name".

To run the script make sure train.csv is in /data folder, then use command:
```shell script
python 01_prepare_data.py --file train.csv
```
Wait until script finish with a print "Finished."
If you want to break earlier enter "Ctrl-C".

### simulate_streaming.py 
Script simulate streaming by doing in a while loop, every x seconds:
- checking content of source_folder = 'data/split_by_date/'
- checking content of destiny_folder = 'data/input_data/'
- comparing two lists and copying from source to destiny folder the earliest day and hour not yet present there

Running script with example parameters: will copy new hour of date every minute by checking every 0.25 of minute:
```shell script
python 02_simulate_streaming.py --tw 60 --sleep 30
```
Script will end when there is no more data to copy in dst folder. 
If you want to break earlier enter "Ctrl-C".

### batch_approach.ipnyb
Jupyter notebook with ETL and initial ML modeling


