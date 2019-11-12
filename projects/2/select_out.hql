insert overwrite directory 'KrasnovEA_hiveout' row format delimited fields terminated by ' ' stored as textfile select * from hw2_pred;
