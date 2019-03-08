create_env('dbenv');
create_db('D');
current_db := 'D';
begin tx;
var t1 real rel {n int, s string, f float, b boolean, bn binary} key{n};
var t2 real rel {n int, m int} key{n};
var t3 real rel {n int, s string} key{n};
var t4 real rel {n int, s string} key{n};
commit;
