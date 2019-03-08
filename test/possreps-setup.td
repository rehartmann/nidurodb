create_env('dbenv');
create_db('D');
current_db := 'D';
begin tx;
type point possrep (x int, y int) init point(0, 0);
implement type point; end implement;
var td real rel {n int, p point} key{n};
insert td tup {n 1, p point(1, 5)};
commit;
