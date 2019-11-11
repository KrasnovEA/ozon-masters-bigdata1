add archive projects/2/predict.py;
add archive projects/2/model.py;
add archive 2.joblib;



insert into hw2_pred select * from(
    select transform(*) using 'predict.py' as (id, pred) from(
        select nvl(*, '') from hw2_test
        where nvl(if1, 0) > 20 and nvl(if1, 0) < 40) temp_name) temp_name;