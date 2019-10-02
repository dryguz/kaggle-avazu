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
python prepare_data.py --file train.csv
```
Wait until script finish with a print "Finished."
If you want to break earlier enter "Ctrl-C".

### simulate_streaming.py 
Script simulate streaming by doing in a while loop, every x seconds:
- checking content of root_folder = 'data/split_by_date/'
- checking content of input_folder = 'data/input_data/'
- comparing two lists and taking the earliest day and hour from not copied yet
- copies one hour of data

Running script with example parameters: will copy new hour of date every minute by checking every 0.25 of minute:
```shell script
python simulate_streaming.py --tw 60 --sleep 30
```
Script will end when there is no more data to copy in dst folder. 
If you want to break earlier enter "Ctrl-C".

### read_data.py
Python code for 
- reading parquets 
- setting schema
- setting socket for stream_reading
