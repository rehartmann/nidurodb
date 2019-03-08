create_env('dbenv');
create_db('D');
current_db := 'D';
begin tx;
var t1 real rel {n int, s string, f float, b boolean, bn binary} key{n};
var t2 real rel {n int, s string, f float, b boolean, bn binary} key{n};
var t3 real rel {m int, k int, s string} key{m};
commit;