# To run the test, add the bin directory to PATH
# and the DuroDBMS lib directory to LD_LIBRARY_PATH

import osproc
import duro
import unittest
import os
import times

suite "datetime":

  setup:
    let errC = execCmd("echo \"" &
                      "create_env('dbenv');" &
                      "create_db('D');" &
                      "current_db := 'D';" &
                      "begin tx;" &
                      "var td real rel {n int, d datetime} key{n};" &
                      "commit;\"" &
                      "| durodt")
    require(errC == 0)

  teardown:    
    removeDir("dbenv")

  test "insert and read":
    const
      testYear = 2018
      testMonth = mDec
      testDay = 22
      testHour = 11
      testMinute = 25
      testSecond = 30

    let dc = createContext("dbenv", 0)
    require(dc != nil)
    let
      tx = dc.getDatabase("D").begin

    let dt = DateTime(year: testYear, month: testMonth, monthday: testDay,
                      hour: testHour, minute: testMinute, second: testSecond)
    duro.insert(td, (n: 1, d: dt), tx)

    var
      tup = (n: 0, d: DateTime(year: 2018, month: mJan, monthday: 1))
    toTuple(tup, tupleFrom(@@td), tx)
    check(tup.d.year == testYear)
    check(tup.d.month == testMonth)
    check(tup.d.monthday == testDay)
    check(tup.d.hour == testHour)
    check(tup.d.minute == testMinute)
    check(tup.d.second == testSecond)

    toTuple(tup, tupleFrom(@@td.where(@@d$.year $= testYear)), tx)
    check(tup.d.year == testYear)
    check(tup.d.month == testMonth)
    check(tup.d.monthday == testDay)
    check(tup.d.hour == testHour)
    check(tup.d.minute == testMinute)
    check(tup.d.second == testSecond)

    var
      s: seq[tuple[n: int, d: DateTime]]
    load(s, @@td.where(@@d$.year $= 1900), tx)
    check(s.len == 0)

    tx.commit
    dc.closeContext
