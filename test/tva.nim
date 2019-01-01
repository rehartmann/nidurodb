# To run the test, add the bin directory to PATH
# and the DuroDBMS lib directory to LD_LIBRARY_PATH

import osproc
import duro
import unittest
import os

suite "tuple-valued attributes":

  setup:
    let errC = execCmd("echo \"" &
                      "create_env('dbenv');" &
                      "create_db('D');" &
                      "current_db := 'D';" &
                      "begin tx;" &
                      "var t1 real rel {n int, tp tuple {a int, b string} } key{n};" &
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

    duro.insert(t1, (n: 1, tp: (a: 5, b: "s")), tx)
    
    var
      t: tuple[n: int, tp: tuple[a: int, b: string]]
    
    toTuple(t, tupleFrom(@@t1), tx)

    check(t.n == 1)
    check(t.tp.a == 5)
    check(t.tp.b == "s")

    var
      s: seq[tuple[n: int, tp: tuple[a: int, b: string]]]

    load(s, @@t1, tx)

    check(s.len == 1)
    check(s[0].n == 1)
    check(s[0].tp.a == 5)
    check(s[0].tp.b == "s")

    tx.commit
    dc.closeContext
