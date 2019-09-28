# kaggle-avazu

Exercise project for testing

##Data preparation
Data are provided in one big csv file. 
[File description under this link](https://www.kaggle.com/c/avazu-ctr-prediction/data)
###prepare_data.py
Script splits the data by day and by hour.
The results are folders with parquet files.

Folder's schema is:
"data=xxxx-xx-xx"/"hour=xx"/"parqets_files_with_random_string_name".

To run the script use command:
```
python prepare_data.py --file train.csv
```

###simulate_streamin.py 
Script simulate streaming by doing in a while loop, every x seconds:
- checking content of root_folder = 'data/split_by_date/'
- checking content of input_folder = 'data/input_data/'
- comparing two lists and taking the earliest day and hour from not copied yet
- copies one hour of data