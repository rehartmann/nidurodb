# To run the test, add the bin directory to PATH
# and the DuroDBMS lib directory to LD_LIBRARY_PATH

import osproc
import duro
import unittest
import os

suite "tuple-valued attributes":

  setup:
    let errC = execCmd("durodt tva-setup.td")
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

  test "wrap":
    let dc = createContext("dbenv", 0)
    require(dc != nil)
    let
      tx = dc.getDatabase("D").begin

    duro.insert(t2, (n: 1, m: 5, s: "foo", f: 5.0), tx)

    var
      t: tuple[n: int, tp: tuple[m: int, s: string, f: float]]
    
    toTuple(t, tupleFrom(@@t2.wrap({m, s, f} as tp)), tx)

    check(t.n == 1)
    check(t.tp.m == 5)
    check(t.tp.s == "foo")
    check(t.tp.f == 5.0)

    tx.commit
    dc.closeContext

  test "unwrap":
    let dc = createContext("dbenv", 0)
    require(dc != nil)
    let
      tx = dc.getDatabase("D").begin
    require(tx != nil)

    duro.insert(t1, (n: 5, tp: (a: 76, b: "bee")), tx)

    var
      t: tuple[n: int, a: int, b: string]
    
    let exp = tupleFrom(@@t1.unwrap(tp));
    check(exp != nil)

    toTuple(t, exp, tx)

    check(t.n == 5)
    check(t.a == 76)
    check(t.b == "bee")

    tx.commit
    dc.closeContext
