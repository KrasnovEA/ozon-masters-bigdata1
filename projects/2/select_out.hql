insert overwrite directory 'KrasnovEA_hiveout' row format delimited fields terminated by '\t' as textfile select * from hw2_pred;
