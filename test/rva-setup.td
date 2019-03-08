create_env('dbenv');
create_db('D');
current_db := 'D';
begin tx;
var t1 real rel {n int, r rel {a int, b string} } key{n};
var t2 real rel {n int, a int, b string};
commit;