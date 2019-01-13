# To run the test, add the bin directory to PATH
# and the DuroDBMS lib directory to LD_LIBRARY_PATH

import osproc
import duro
import unittest
import os
import algorithm

suite "tuple-valued attributes":

  setup:
    let errC = execCmd("echo \"" &
                      "create_env('dbenv');" &
                      "create_db('D');" &
                      "current_db := 'D';" &
                      "begin tx;" &
                      "var t1 real rel {n int, r rel {a int, b string} } key{n};" &
                      "commit;\"" &
                      "| durodt")
    require(errC == 0)

  teardown:
    removeDir("dbenv")

  test "insert and read":
    let dc = createContext("dbenv", 0)
    require(dc != nil)
    let
      tx = dc.getDatabase("D").begin

    duro.insert(t1, (n: 1, r: @[(a: 5, b: "s"), (a: 12, b: "SS")]), tx)

    var
      t: tuple[n: int, r: seq[tuple[a: int, b: string]]]
    
    toTuple(t, tupleFrom(@@t1), tx)

    check(t.n == 1)
    t.r.sort do (t1, t2: tuple[a: int, b: string]) -> int:
      result = cmp(t1.a, t2.a)

    check(t.r[0].a == 5)
    check(t.r[0].b == "s")
    check(t.r[1].a == 12)
    check(t.r[1].b == "SS")

    tx.commit
    dc.closeContext

    