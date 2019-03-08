# To run the test, add the bin directory to PATH
# and the DuroDBMS lib directory to LD_LIBRARY_PATH

import osproc
import duro
import unittest
import os

suite "possreps":

  setup:
    let errC = execCmd("durodt possreps-setup.td")
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
