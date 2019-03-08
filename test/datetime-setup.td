create_env('dbenv');
create_db('D');
current_db := 'D';
begin tx;
var td real rel {n int, d datetime} key{n};
commit;
