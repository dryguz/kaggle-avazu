# kaggle-avazu

Exercise project for testing

Data are provided in one big csv file. 
"prepare_data.py" splits the data by day and by hour.
The results are folders with parquet files.

Folder's schema is:
"data=xxxx-xx-xx"/"hour=xx"/"parqets_files_with_random_string_name".

To run the script use command:
```
python prepare_data.py --file train.csv
```
