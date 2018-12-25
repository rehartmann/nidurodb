# To run the test, add the bin directory to PATH
# and the DuroDBMS lib directory to LD_LIBRARY_PATH

import osproc
import duro
import unittest
import os

suite "relational algrebra":

  setup:
    let errC = execCmd("echo \"" &
                      "create_env('dbenv');" &
                      "create_db('D');" &
                      "current_db := 'D';" &
                      "begin tx;" &
                      "var t1 real rel {n int, s string, f float, b boolean, bn binary} key{n};" &
                      "var t2 real rel {n int, s string, f float, b boolean, bn binary} key{n};" &
                      "var t3 real rel {m int, k int, s string} key{m};" &
                      "commit;\"" &
                      "| durodt")
    require(errC == 0)

  teardown:
    removeDir("dbenv")

  test "whereProject":
    let dc = createContext("dbenv", 0)
    require(dc != nil)
    let
      tx = dc.getDatabase("D").begin

    var
      intup: tuple[n: int, s: string, f: float, b: bool, bn: seq[byte]] = (n: 1, s: "Ui", f: 1.5, b: true, bn: @[byte(255)])
    duro.insert(t1, intup, tx)
    intup = (n: 2, s: "Ohh", f: 3.0, b: false, bn: @[byte(0), byte(1)])
    duro.insert(t1, intup, tx)

    var
      s: seq[tuple[n: int, s: string]]
    load(s, @@t1{ n, s }, tx, SeqItem(attr: "n", dir: asc))
    check(s[0].n == 1)
    check(s[0].s == "Ui")
    check(s[1].n == 2)
    check(s[1].s == "Ohh")

    var
      outtup: tuple[n: int, s: string, f: float, b: bool, bn: seq[byte]]
    toTuple(outtup, tupleFrom(@@t1.where(@@s $= "Ohh")), tx)
    check(outtup.n == 2)
    check(outtup.s == "Ohh")
    check(outtup.b == false)
    check(outtup.bn == @[byte(0), byte(1)])

    tx.commit
    dc.closeContext

  test "unionMinusIntersect":
    let dc = createContext("dbenv", 0)
    require(dc != nil)
    let
      tx = dc.getDatabase("D").begin

    duro.insert(t1, (n: 1, s: "Ui", f: 1.5, b: true, bn: @[byte(255)]), tx)
    duro.insert(t1, (n: 2, s: "ohh", f: 2.0, b: false, bn: @[byte(1), byte(2)]), tx)
    duro.insert(t2, (n: 1, s: "Ui", f: 1.5, b: true, bn: @[byte(255)]), tx)

    var
      s: seq[tuple[n: int, s: string, f: float, b: bool, bn: seq[byte]]]

    load(s, @@t1.union(@@t2), tx, SeqItem(attr: "n", dir: asc))
    check(s.len == 2)
    check(s[0].n == 1)
    check(s[0].s == "Ui")
    check(s[0].f == 1.5)
    check(s[0].b == true)
    check(s[0].bn == @[byte(255)])
    check(s[1].n == 2)
    check(s[1].s == "ohh")
    check(s[1].f == 2.0)
    check(s[1].b == false)
    check(s[1].bn == @[byte(1), byte(2)])

    load(s, @@t1.minus(@@t2), tx, SeqItem(attr: "n", dir: asc))
    check(s.len == 1)
    check(s[0].n == 2)
    check(s[0].s == "ohh")

    load(s, @@t1.intersect(@@t2), tx, SeqItem(attr: "n", dir: asc))
    check(s.len == 1)
    check(s[0].n == 1)
    check(s[0].s == "Ui")

    expect DuroError:
      load(s, @@t1.dUnion(@@t2), tx, SeqItem(attr: "n", dir: asc))

    load(s, @@t1.where(@@n $= 2).dUnion(@@t2), tx, SeqItem(attr: "n", dir: asc))
    check(s.len == 2)
    check(s[0].n == 1)
    check(s[0].s == "Ui")
    check(s[1].n == 2)
    check(s[1].s == "ohh")

    tx.commit
    dc.closeContext

  test "renameJoin":
    let dc = createContext("dbenv", 0)
    require(dc != nil)
    let
      tx = dc.getDatabase("D").begin

    duro.insert(t1, (n: 1, s: "Ui", f: 1.5, b: true, bn: @[byte(255)]), tx)
    duro.insert(t3, (m: 1, k: 1, s: "Foo!"), tx)
    duro.insert(t3, (m: 2, k: 1, s: "Bar?"), tx)

    var
      s: seq[tuple[n: int, s: string, f: float, b: bool, bn: seq[byte], m: int, s3: string]]

    load(s, @@t1.join(@@t3.rename(k as n, s as s3)), tx, SeqItem(attr: "n", dir: asc))
    check(s.len == 2)
    check(s[0].n == 1)
    check(s[0].s == "Ui")
    check(s[0].f == 1.5)
    check(s[0].b == true)
    check(s[0].bn == @[byte(255)])
    check(s[0].m == 1)
    check(s[0].s3 == "Foo!")
    check(s[1].n == 1)
    check(s[1].s == "Ui")
    check(s[1].f == 1.5)
    check(s[1].b == true)
    check(s[1].bn == @[byte(255)])
    check(s[1].m == 2)
    check(s[1].s3 == "Bar?")

    tx.commit
    dc.closeContext

  test "extendSummarize":
    let dc = createContext("dbenv", 0)
    require(dc != nil)
    let tx = dc.getDatabase("D").begin

    duro.insert(t1, (n: 1, s: "Ui", f: 1.5, b: true, bn: @[byte(255)]), tx)
    
    var
      s: seq[tuple[n: int, s: string, f: float, b: bool, bn: seq[byte], nx: int, sx: string]]

    load(s, @@t1.extend(nx := @@n * 2, sx := @@s || "x"), tx)
    check(s[0].n == 1)
    check(s[0].s == "Ui")
    check(s[0].f == 1.5)
    check(s[0].b == true)
    check(s[0].bn == @[byte(255)])
    check(s[0].nx == 2)
    check(s[0].sx == "Uix")

    duro.insert(t3, (m: 1, k: 1, s: ""), tx)
    duro.insert(t3, (m: 2, k: 1, s: ""), tx)
    duro.insert(t3, (m: 3, k: 2, s: ""), tx)

    var
      s2: seq[tuple[k: int, mc: int, ms: int]]
    
    load(s2, @@t3.summarize(@@t3{ k }, mc := count(@@m), ms := sum(@@m)), tx)
    check(s2.len == 2)
    check(s2[0].k == 1)
    check(s2[0].mc == 2)
    check(s2[0].ms == 3)
    check(s2[1].k == 2)
    check(s2[1].mc == 1)
    check(s2[1].ms == 3)

    tx.commit
    dc.closeContext

  test "evaluate":
    let dc = createContext("dbenv", 0)
    require(dc != nil)
    let tx = dc.getDatabase("D").begin

    duro.insert(t1, (n: 1, s: "Ui", f: 1.5, b: true, bn: @[byte(255)]), tx)

    check(toInt(count(@@t1), tx) == 1)
    check(toFloat(toExpr(1.0), tx) == 1.0)
    check(toString(toExpr("x"), tx) == "x")

    tx.commit
    dc.closeContext
