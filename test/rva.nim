# To run the test, add the bin directory to PATH
# and the DuroDBMS lib directory to LD_LIBRARY_PATH

import osproc
import duro
import unittest
import os
import algorithm

suite "tuple-valued attributes":

  setup:
    let errC = execCmd("durodt rva-setup.td")
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

  test "group":
    let dc = createContext("dbenv", 0)
    require(dc != nil)
    let
      tx = dc.getDatabase("D").begin

    duro.insert(t2, (n: 10, a: 2, b: "b"), tx)
    duro.insert(t2, (n: 10, a: 3, b: "bee"), tx)

    var
      s: seq[tuple[n: int, r: seq[tuple[a: int, b: string]]]]
    
    load(s, @@t2.group({a, b} as r), tx)
    check(len(s) == 1)
    check(s[0].n == 10)

    s[0].r.sort do (t1, t2: tuple[a: int, b: string]) -> int:
      result = cmp(t1.a, t2.a)

    check(len(s[0].r) == 2)
    check(s[0].r[0].a == 2)
    check(s[0].r[0].b == "b")
    check(s[0].r[1].a == 3)
    check(s[0].r[1].b == "bee")

    tx.commit
    dc.closeContext

  test "ungroup":
    let dc = createContext("dbenv", 0)
    require(dc != nil)
    let
      tx = dc.getDatabase("D").begin

    duro.insert(t1, (n: 1, r: @[(a: 7, b: "x"), (a: 17, b: "yz")]), tx)

    var
      s: seq[tuple[n: int, a: int, b: string]]
    load(s, @@t1.ungroup(r), tx, SeqItem(attr: "a", dir: asc))

    check(len(s) == 2)
    check(s[0].a == 7)
    check(s[0].b == "x")
    check(s[1].a == 17)
    check(s[1].b == "yz")

    tx.commit
    dc.closeContext
