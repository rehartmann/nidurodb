# To run the test, add the bin directory to PATH
# and the DuroDBMS lib directory to LD_LIBRARY_PATH

import osproc
import duro
import unittest
import os

suite "possreps":

  setup:
    let errC = execCmd("echo \"" &
                      "create_env('dbenv');" &
                      "create_db('D');" &
                      "current_db := 'D';" &
                      "begin tx;" &
                      "type point possrep (x int, y int) init point(0, 0);" &
                      "implement type point; end implement;" &
                      "var td real rel {n int, p point} key{n};" &
                      "insert td tup {n 1, p point(1, 5)};" &
                      "commit;\"" &
                      "| durodt")
    require(errC == 0)

  teardown:
    removeDir("dbenv")

  test "read":
    let dc = createContext("dbenv", 0)
    require(dc != nil)
    let
      tx = dc.getDatabase("D").begin

    var
      t: tuple[n: int, p: tuple[x: int, y: int]]
    
    toTuple(t, tupleFrom(@@td), tx)
    check (t.p.x == 1)
    check (t.p.y == 5)

    tx.commit
    dc.closeContext
