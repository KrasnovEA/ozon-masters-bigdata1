add archive projects/2/predict.py;
insert into hw2_pred select * from(
    select transform(*) using 'predict.py' as (id, pred) from(
        select * from hw2_test
        where nvl(if1, 0) > 20 and nvl(if1, 0) < 40) temp_name) temp_name) temp_name;