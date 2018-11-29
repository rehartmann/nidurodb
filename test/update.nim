# To run the test, add the bin directory to PATH
# and the DuroDBMS lib directory to LD_LIBRARY_PATH

import osproc
import duro
import unittest
import os

suite "table insert, update, delete":

  setup:
    let errC = execCmd("echo \"" &
                      "create_env('dbenv');" &
                      "create_db('D');" &
                      "current_db := 'D';" &
                      "begin tx;" &
                      "var t1 real rel {n int, s string, f float, b boolean, bn binary} key{n};" &
                      "commit;\"" &
                      "| durodt")
    require(errC == 0)

  teardown:
    removeDir("dbenv")

  test "insertUpdate":
    let dc = createContext("dbenv", 0)
    require(dc != nil)

    let
      tx = dc.getDatabase("D").begin
      intup: tuple[n: int, s: string, f: float, b: bool,
                   bn: seq[byte]] = (n: 1, s: "Ui", f: 1.5, b: true, bn: @[byte(255)])
    duro.insert(t1, intup, tx)

    var
      outtup: tuple[n: int, s: string, f: float, b: bool, bn: seq[byte]]
    toTuple(outtup, V(t1), tx)
    check(outtup.n == 1)
    check(outtup.s == "Ui")
    check(outtup.f == 1.5)
    check(outtup.b == true)
    check(outtup.bn == @[byte(255)])

    duro.update(t1, V(n) $= 1, tx, s := toExpr("ohh"), f := toExpr(1.0), b := toExpr(false),
                 bn := toExpr(@[byte(1), byte(20)]))

    toTuple(outtup, V(t1), tx)
    check(outtup.n == 1)
    check(outtup.s == "ohh")
    check(outtup.f == 1.0)
    check(outtup.b == false)
    check(outtup.bn == @[byte(1), byte(20)])

    tx.commit
    dc.closeContext

  test "delete":
    let dc = createContext("dbenv", 0)
    require(dc != nil)
  
    let
      tx = dc.getDatabase("D").begin
      intup: tuple[n: int, s: string, f: float, b: bool, bn: seq[byte]] = (n: 1, s: "Ui", f: 1.5, b: true, bn: @[byte(255)])
    duro.insert(t1, intup, tx)

    check(duro.delete(t1, V(n) $= 1, tx) == 1)
    check(toInt(opInv("count", V(t1)), tx) == 0)
    tx.commit
    dc.closeContext

  test "multiple assignment":
    let dc = createContext("dbenv", 0)
    require(dc != nil)
  
    let
      tx = dc.getDatabase("D").begin
    check(assign(duro.insert(t1, (n: 1, s: "Ui", f: 1.5, b: true, bn: @[byte(255)])), tx) == 1)

    var
      outtup: tuple[n: int, s: string, f: float, b: bool, bn: seq[byte]]
    toTuple(outtup, V(t1), tx)
    check(outtup.n == 1)
    check(outtup.s == "Ui")
    check(outtup.f == 1.5)
    check(outtup.b == true)
    check(outtup.bn == @[byte(255)])

    tx.commit
    dc.closeContext
