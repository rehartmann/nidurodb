import libduro
import macros
import times

type
   Expression* = ref object of RootObj

   VarExpression = ref object of Expression
     name: string

   BoolExpression = ref object of Expression
     value: bool

   StringExpression = ref object of Expression
     value: string

   IntExpression = ref object of Expression
     value: int

   FloatExpression = ref object of Expression
     value: float

   ByteSeqExpression = ref object of Expression
     value: seq[byte]

   StringSeqExpression = ref object of Expression
     value: seq[string]

   OpExpression = ref object of Expression
     name: string
     args: seq[Expression]

   PropertyExpression = ref object of Expression
     obj: Expression
     propName: string

   DuroError* = object of Exception

   DContext* = ref object
     pEnv: pointer
     execContext: RDB_exec_contextObj

   Database* = ref object
     pDb: pointer
     context: DContext

   Transaction* = ref object
     database: Database
     tx: RDB_transactionObj

   AssignmentKind = enum
     akCopy, akInsert, akUpdate, akDelete, akVDelete

   AttrUpdate* = object
     name: string
     exp: Expression

   Assignment* = ref object
     case kind: AssignmentKind
     of akCopy:
       copyDest: string
       copySource: RDB_object
     of akInsert:
       insertDest: string
       insertSource: RDB_object
       insertFlags: int
     of akUpdate:
       updateDest: string
       updateCond: Expression
       attrUpdates: seq[AttrUpdate]
     of akDelete:
       deleteDest: string
       deleteCond: Expression
     of akVDelete:
       vDeleteDest: string
       vDeleteSource: RDB_object
       vDeleteFlags: int

var
  execContext: RDB_exec_contextObj

RDB_init_exec_context(addr(execContext))

proc raiseDuroError(pExecContext: RDB_exec_context, tx: Transaction = nil) {.noReturn.} =
  let err = RDB_get_err(pExecContext)
  let errtyp = RDB_obj_type(err)
  if errtyp != nil:
    var errmsg = $RDB_type_name(errtyp)
    
    if tx != nil:
      var propval: RDB_object
      if RDB_obj_property(err, cstring("msg"), addr(propval), nil, pExecContext,
                          addr(tx.tx)) == 0:
        if RDB_obj_type(addr(propval)) == addr(RDB_STRING):
          errmsg = errmsg & ": " & $RDB_obj_string(addr(propval))
    
    raise newException(DuroError, errmsg)
  raise newException(DuroError, "unkown")

proc raiseDuroError(tx: Transaction) {.noReturn.} =
  raiseDuroError(addr(tx.database.context.execContext), tx)

proc createContext*(path: string, flags: int): DContext =
  ## Creates a DContext from a path and flags.
  var
    dc: DContext
  new(dc)
  RDB_init_exec_context(addr(dc.execContext))
  dc.pEnv = RDB_open_env(cstring(path), cint(flags), addr(dc.execContext))
  if dc.pEnv == nil:
    return nil
  return dc

proc closeContext*(dc: DContext) =
   ## Closes a DContext
   let res = RDB_close_env(dc.pEnv, addr(dc.execContext))
   if res != 0:
     raiseDuroError(addr(dc.execContext))
   RDB_destroy_exec_context(addr(dc.execContext))

proc getDatabase*(dc: DContext, name: string): Database =
   ## Returns a Database object for the database 'name'.
   let db = RDB_get_db_from_env(cstring(name), dc.pEnv, addr(dc.execContext), nil);
   if db == nil:
      return nil
   return Database(pDb: db, context: dc)

proc begin*(db: Database): Transaction =
   ## Starts a transaction.
   var tx: Transaction
   new(tx)
   if RDB_begin_tx(addr(db.context.execContext), addr(tx.tx), db.pDb, nil) != 0:
      raiseDuroError(addr(db.context.execContext))
   tx.database = db
   return tx

proc commit*(tx: Transaction) =
  ## Commits the transacton 'tx' refers to.
  if RDB_commit(addr(tx.database.context.execContext), addr(tx.tx)) != 0:
    raiseDuroError(addr(tx.database.context.execContext))

proc rollback*(tx: Transaction) =
  ## Aborts the transaction 'tx' refers to.
  if RDB_rollback(addr(tx.database.context.execContext), addr(tx.tx)) != 0:
    raiseDuroError(addr(tx.database.context.execContext))

proc toVar*(varname: string): VarExpression =
  result = VarExpression(name: varname)

macro `@@`*(varname: untyped): Expression =
  ## Converts an identifier to a variable name.
  result = newCall("toVar", @[toStrLit(varname)])

macro V*(varname: untyped): Expression =
  ## Converts an identifier to a variable name.
  result = newCall("toVar", @[toStrLit(varname)])

proc opInv*(opname: string, opargs: varargs[Expression]): Expression =
  result = OpExpression(name: opname, args: @opargs)

proc `$=`*(exp1: Expression, exp2: Expression): Expression =
  ## Creates a = expression
  result = OpExpression(name: "=", args: @[exp1, exp2])

proc `$=`*(exp1: Expression, s: string): Expression =
  ## Creates a = expression
  result = OpExpression(name: "=", args: @[exp1, StringExpression(value: s)])

proc `$=`*(exp1: Expression, val: int): Expression =
  ## Creates a = expression
  result = OpExpression(name: "=", args: @[exp1, IntExpression(value: val)])

proc `$=`*(exp1: Expression, val: float): Expression =
  ## Creates a = expression
  result = OpExpression(name: "=", args: @[exp1, FloatExpression(value: val)])

proc `$!=`*(exp1: Expression, exp2: Expression): Expression =
  ## Creates a <> expression
  result = OpExpression(name: "<>", args: @[exp1, exp2])

proc `$!=`*(exp1: Expression, s: string): Expression =
  ## Creates a <> expression
  result = OpExpression(name: "<>", args: @[exp1, StringExpression(value: s)])

proc `$!=`*(exp1: Expression, val: int): Expression =
  ## Creates a <> expression
  result = OpExpression(name: "<>", args: @[exp1, IntExpression(value: val)])

proc `$!=`*(exp1: Expression, val: float): Expression =
  result = OpExpression(name: "<>", args: @[exp1, FloatExpression(value: val)])
  ## Creates a <> expression

proc `$<`*(exp1: Expression, exp2: Expression): Expression =
  ## Creates a < expression
  result = OpExpression(name: "<", args: @[exp1, exp2])

proc `$<`*(exp1: Expression, s: string): Expression =
  ## Creates a < expression
  result = OpExpression(name: "<", args: @[exp1, StringExpression(value: s)])

proc `$<`*(exp1: Expression, val: int): Expression =
  ## Creates a < expression
  result = OpExpression(name: "<", args: @[exp1, IntExpression(value: val)])

proc `$<`*(exp1: Expression, val: float): Expression =
  ## Creates a < expression
  result = OpExpression(name: "<", args: @[exp1, FloatExpression(value: val)])

proc `$>`*(exp1: Expression, exp2: Expression): Expression =
  ## Creates a > expression
  result = OpExpression(name: ">", args: @[exp1, exp2])

proc `$>`*(exp1: Expression, s: string): Expression =
  ## Creates a > expression
  result = OpExpression(name: ">", args: @[exp1, StringExpression(value: s)])

proc `$>`*(exp1: Expression, val: int): Expression =
  ## Creates a > expression
  result = OpExpression(name: ">", args: @[exp1, IntExpression(value: val)])

proc `$>`*(exp1: Expression, val: float): Expression =
  ## Creates a > expression
  result = OpExpression(name: ">", args: @[exp1, FloatExpression(value: val)])

proc `$<=`*(exp1: Expression, exp2: Expression): Expression =
   result = OpExpression(name: "<=", args: @[exp1, exp2])

proc `$<=`*(exp1: Expression, s: string): Expression =
  result = OpExpression(name: "<=", args: @[exp1, StringExpression(value: s)])

proc `$<=`*(exp1: Expression, val: int): Expression =
  result = OpExpression(name: "<=", args: @[exp1, IntExpression(value: val)])

proc `$<=`*(exp1: Expression, val: float): Expression =
  result = OpExpression(name: "<=", args: @[exp1, FloatExpression(value: val)])

proc `$>=`*(exp1: Expression, exp2: Expression): Expression =
   result = OpExpression(name: ">=", args: @[exp1, exp2])

proc `$>=`*(exp1: Expression, s: string): Expression =
  result = OpExpression(name: ">=", args: @[exp1, StringExpression(value: s)])

proc `$>=`*(exp1: Expression, val: int): Expression =
  result = OpExpression(name: ">=", args: @[exp1, IntExpression(value: val)])

proc `$>=`*(exp1: Expression, val: float): Expression =
  result = OpExpression(name: ">=", args: @[exp1, FloatExpression(value: val)])

proc `+`*(exp1: Expression, exp2: Expression): Expression =
  result = OpExpression(name: "+", args: @[exp1, exp2])

proc `and`*(exp1: Expression, exp2: Expression): Expression =
  result = OpExpression(name: "and", args: @[exp1, exp2])

proc `or`*(exp1: Expression, exp2: Expression): Expression =
  result = OpExpression(name: "or", args: @[exp1, exp2])

proc `xor`*(exp1: Expression, exp2: Expression): Expression =
  result = OpExpression(name: "xor", args: @[exp1, exp2])

proc `not`*(exp1: Expression): Expression =
  result = OpExpression(name: "not", args: @[exp1])

proc `+`*(exp1: Expression, val: int): Expression =
  result = OpExpression(name: "+", args: @[exp1, IntExpression(value: val)])

proc `+`*(exp1: Expression, val: float): Expression =
  result = OpExpression(name: "+", args: @[exp1, FloatExpression(value: val)])

proc `-`*(exp1: Expression, val: int): Expression =
  result = OpExpression(name: "-", args: @[exp1, IntExpression(value: val)])

proc `-`*(exp1: Expression, val: float): Expression =
  result = OpExpression(name: "-", args: @[exp1, FloatExpression(value: val)])

proc `-`*(exp1: Expression, exp2: Expression): Expression =
  result = OpExpression(name: "-", args: @[exp1, exp2])

proc `-`*(exp1: Expression): Expression =
  result = OpExpression(name: "-", args: @[exp1])

proc `*`*(exp1: Expression, exp2: Expression): Expression =
  result = OpExpression(name: "*", args: @[exp1, exp2])

proc `*`*(exp1: Expression, val: int): Expression =
  result = OpExpression(name: "*", args: @[exp1, IntExpression(value: val)])

proc `*`*(exp1: Expression, val: float): Expression =
  result = OpExpression(name: "*", args: @[exp1, FloatExpression(value: val)])

proc `/`*(exp1: Expression, exp2: Expression): Expression =
  result = OpExpression(name: "/", args: @[exp1, exp2])

proc `/`*(exp1: Expression, val: int): Expression =
  result = OpExpression(name: "/", args: @[exp1, IntExpression(value: val)])

proc `/`*(exp1: Expression, val: float): Expression =
  result = OpExpression(name: "/", args: @[exp1, FloatExpression(value: val)])

proc `%`*(exp1: Expression, exp2: Expression): Expression =
  result = OpExpression(name: "%", args: @[exp1, exp2])

proc `%`*(exp1: Expression, val: int): Expression =
  result = OpExpression(name: "%", args: @[exp1, IntExpression(value: val)])

proc castAsString*(exp: Expression): Expression =
  ## Creates a cast_as_string expression
  result = OpExpression(name: "cast_as_string", args: @[exp])

proc castAsInt*(exp: Expression): Expression =
  result = OpExpression(name: "cast_as_integer", args: @[exp])

proc castAsFloat*(exp: Expression): Expression =
  result = OpExpression(name: "cast_as_float", args: @[exp])

proc `||`*(exp1: Expression, exp2: Expression): Expression =
  result = OpExpression(name: "||", args: @[exp1, exp2])

proc `||`*(exp1: Expression, s: string): Expression =
  result = OpExpression(name: "||", args: @[exp1, StringExpression(value: s)])

proc `like`*(exp1: Expression, s: string): Expression =
  result = OpExpression(name: "like", args: @[exp1, StringExpression(value: s)])

proc `like`*(exp1: Expression, exp2: Expression): Expression =
  result = OpExpression(name: "like", args: @[exp1, exp2])

proc `regexLike`*(exp1: Expression, s: string): Expression =
  result = OpExpression(name: "regex_like", args: @[exp1, StringExpression(value: s)])

proc `regexLike`*(exp1: Expression, exp2: Expression): Expression =
  result = OpExpression(name: "regex_like", args: @[exp1, exp2])

proc count*(tbexp: Expression): Expression =
  result = OpExpression(name: "count", args: @[tbexp])

proc sum*(exp: Expression): Expression =
  result = OpExpression(name: "sum", args: @[exp])

proc sum*(tbexp: Expression, exp: Expression): Expression =
  result = OpExpression(name: "sum", args: @[tbexp, exp])

proc avg*(tbexp: Expression, exp: Expression): Expression =
  result = OpExpression(name: "avg", args: @[tbexp, exp])

proc project*(exp: Expression, attrs: varargs[string]): Expression =
  var
    opargs = newSeq[Expression](len(attrs) + 1);
  opargs[0] = exp
  for i in 0..<len(attrs):
    opargs[i + 1] = StringExpression(value: attrs[i])
  result = OpExpression(name: "project", args: opargs)

macro `{}`*(exp: Expression, attrs: varargs[untyped]): untyped =
  ## Creates a projection expression.
  ## Example: V(t){ attr1, attr2 }
  var args: seq[NimNode] = newSeq[NimNode](len(attrs) + 1);
  args[0] = exp
  for i in 0..<len(attrs):
    args[i + 1] = toStrLit(attrs[i])
  result = newCall("project", args)

proc renameExpr*(exp: Expression, renamings: varargs[string]): Expression =
  var
    opargs = newSeq[Expression](len(renamings) + 1);
  opargs[0] = exp
  for i in 0..<len(renamings):
    opargs[i + 1] = StringExpression(value: renamings[i])
  result = OpExpression(name: "rename", args: opargs)

macro rename*(exp: Expression, renamings: varargs[untyped]): untyped =
  ## Creates a rename expression.
  ## Example: V(t).rename(attr1 as new_attr1, attr2 as new_attr2)
  var opargs: seq[NimNode] = newSeq[NimNode](2 * len(renamings) + 1);
  opargs[0] = exp
  for i in 0..<len(renamings):
    if len(renamings[i]) != 3:
      raise newException(ValueError, "invalid renaming")
    if toStrLit(renamings[i][0]).strVal != "as":
      raise newException(ValueError, "invalid renaming")
    opargs[i * 2 + 1] = toStrLit(renamings[i][1])
    opargs[i * 2 + 2] = toStrLit(renamings[i][2])
  result = newCall("renameExpr", opargs)

proc wrapExpr*(exp: Expression, wrappings: varargs[tuple[fromAttrs: seq[string], toAttr: string]]): Expression =
  var
    opargs = @[exp]
  for wrapping in wrappings:
    opargs.add(StringSeqExpression(value: wrapping.fromAttrs))
    opargs.add(StringExpression(value: wrapping.toAttr))
  result = OpExpression(name: "wrap", args: opargs)

macro wrap*(exp: Expression, wrappings: varargs[untyped]): untyped =
  ## Creates a WRAP expression.
  ## Example: V(t).wrap({a, b} as tp)
  var opargs: seq[NimNode] = @[exp];
  for i in 0..<len(wrappings):
    if len(wrappings[i]) != 3:
      raise newException(ValueError, "invalid wrapping")
    if toStrLit(wrappings[i][0]).strVal != "as":
      raise newException(ValueError, "invalid wrapping")
    var
      wrapFromAttrs: seq[string] = @[]
    for wrapFromAttr in wrappings[i][1]:
      wrapFromAttrs.add(toStrLit(wrapFromAttr).strVal)
    opargs.add(newLit((fromAttrs: wrapFromAttrs, toAttr: toStrLit(wrappings[i][2]).strVal)))
  result = newCall("wrapExpr", opargs)

proc unwrapExpr*(exp: Expression, attrs: varargs[string]): Expression =
  var
    opargs = newSeq[Expression](len(attrs) + 1);
  opargs[0] = exp
  for i in 0..<len(attrs):
    opargs[i + 1] = StringExpression(value: attrs[i])
  result = OpExpression(name: "unwrap", args: opargs)

macro unwrap*(exp: Expression, attrs: varargs[untyped]): untyped =
  ## Creates an UNWRAP expression.
  var args: seq[NimNode] = newSeq[NimNode](len(attrs) + 1);
  args[0] = exp
  for i in 0..<len(attrs):
    args[i + 1] = toStrLit(attrs[i])
  result = newCall("unwrapExpr", args)

proc groupExpr*(exp: Expression, grouping: varargs[string]): Expression =
  var
    opargs = @[exp]
  for attr in grouping:
    opargs.add(StringExpression(value: attr))
  result = OpExpression(name: "group", args: opargs)

macro group*(exp: Expression, grouping: untyped): untyped =
  ## Creates a GROUP expression.
  ## Example: V(t).group({a, b} as r)
  var opargs: seq[NimNode] = @[exp];
  if len(grouping) != 3:
    raise newException(ValueError, "invalid grouping")
  if toStrLit(grouping[0]).strVal != "as":
    raise newException(ValueError, "invalid grouping")
  for groupFromAttr in grouping[1]:
    opargs.add(toStrLit(groupFromAttr))
  opargs.add(toStrLit(grouping[2]))
  result = newCall("groupExpr", opargs)

proc ungroupExpr*(exp: Expression, attr: string): Expression =
  result = OpExpression(name: "ungroup", args: @[exp, StringExpression(value: attr)])

macro ungroup*(exp: Expression, attr: untyped): untyped =
  ## Creates an UNGROUP expression.
  ## Example: V(t).ungroup(a)
  var opargs: seq[NimNode] = @[exp, toStrLit(attr)];
  result = newCall("ungroupExpr", opargs)

proc propExpr*(exp: Expression, prop: string): Expression =
  result = PropertyExpression(obj: exp, propName: prop)

macro `$.`*(exp: Expression, prop: untyped): Expression =
  ## Creates an expression which represents accessing a property
  result = newCall("propExpr", exp, toStrLit(prop))

proc toExpr*(b: bool): Expression =
  ## Converts a boolean value to an expression.
  return BoolExpression(value: b)

proc toExpr*(i: int): Expression =
  ## Converts an integer value to an expression.
  return IntExpression(value: i)

proc toExpr*(f: float): Expression =
  ## Converts a float value to an expression.
  return FloatExpression(value: f)

proc toExpr*(s: string): Expression =
  ## Converts a string value to an expression.
  return StringExpression(value: s)

proc toExpr*(bytes: seq[byte]): Expression =
  ## Converts a sequence of bytes to an expression.
  return ByteSeqExpression(value: bytes)

proc extendExpr*(exp: Expression, assigns: varargs[Expression]): Expression =
  var
    opargs = newSeq[Expression](len(assigns) + 1);
  opargs[0] = exp
  for i in 0..<len(assigns):
    opargs[i + 1] = assigns[i]
  result = OpExpression(name: "extend", args: opargs)

macro extend*(exp: Expression, assigns: varargs[untyped]): untyped =
  ## Creates an extend expression.
  ## Example: V(t).extend(new_attr1 := <expression>, new_attr2 := <expression>)
  var opargs: seq[NimNode] = newSeq[NimNode](2 * len(assigns) + 1);
  opargs[0] = exp
  for i in 0..<len(assigns):
    if len(assigns[i]) != 3:
      raise newException(ValueError, "invalid assignment")
    if toStrLit(assigns[i][0]).strVal != ":=":
      raise newException(ValueError, "invalid assignment")
    if assigns[i][2].kind == nnkStrLit:
      opargs[i * 2 + 1] = newCall("toExpr", toStrLit(assigns[i][2]))
    else:
      opargs[i * 2 + 1] = assigns[i][2]
    opargs[i * 2 + 2] = newCall("toExpr", toStrLit(assigns[i][1]))
  result = newCall("extendExpr", opargs)

proc summarizeExpr*(exp1: Expression, exp2: Expression, assigns: varargs[Expression]): Expression =
  var
    opargs = newSeq[Expression](len(assigns) + 2);
  opargs[0] = exp1
  opargs[1] = exp2
  for i in 0..<len(assigns):
    opargs[i + 2] = assigns[i]
  result = OpExpression(name: "summarize", args: opargs)

macro summarize*(exp1: Expression, exp2: Expression, assigns: varargs[untyped]): untyped =
  ## Creates a summarize expression.
  ## Example: V(t).summarize(V(t){ attr1 }, new_attr := count(V(attr2)))
  var opargs: seq[NimNode] = newSeq[NimNode](2 * len(assigns) + 2);
  opargs[0] = exp1
  opargs[1] = exp2
  for i in 0..<len(assigns):
    if len(assigns[i]) != 3:
      raise newException(ValueError, "invalid assignment")
    if toStrLit(assigns[i][0]).strVal != ":=":
      raise newException(ValueError, "invalid assignment")
    opargs[i * 2 + 2] = assigns[i][2]
    opargs[i * 2 + 3] = newCall("toExpr", toStrLit(assigns[i][1]))
  result = newCall("summarizeExpr", opargs)

proc where*(table: Expression, cond: Expression): Expression =
  ## Creates a where expression.
  ## Example: V(t).where(V(s) $= <value>)
  result = OpExpression(name: "where", args: @[table, cond])

proc union*(exp1: Expression, exp2: Expression): Expression =
  result = OpExpression(name: "union", args: @[exp1, exp2])

proc dUnion*(exp1: Expression, exp2: Expression): Expression =
  result = OpExpression(name: "d_union", args: @[exp1, exp2])

proc minus*(exp1: Expression, exp2: Expression): Expression =
  result = OpExpression(name: "minus", args: @[exp1, exp2])

proc intersect*(exp1: Expression, exp2: Expression): Expression =
  result = OpExpression(name: "intersect", args: @[exp1, exp2])
   
proc join*(exp1: Expression, exp2: Expression): Expression =
  result = OpExpression(name: "join", args: @[exp1, exp2])

proc `|><|`*(exp1: Expression, exp2: Expression): Expression =
  result = join(exp1, exp2)

proc semijoin*(exp1: Expression, exp2: Expression): Expression =
  ## Creates a semijoin expression.
  result = OpExpression(name: "semijoin", args: @[exp1, exp2])

proc `|><`*(exp1: Expression, exp2: Expression): Expression =
  ## Creates a semijoin expression.
  result = semijoin(exp1, exp2)

proc `><|`*(exp1: Expression, exp2: Expression): Expression =
  ## Creates a semijoin expression.
  result = semijoin(exp2, exp1)

proc matching*(exp1: Expression, exp2: Expression): Expression =
  ## Creates a semijoin expression.
  result = semijoin(exp1, exp2)

proc semiminus*(exp1: Expression, exp2: Expression): Expression =
  result = OpExpression(name: "semiminus", args: @[exp1, exp2])

proc notMatching*(exp1: Expression, exp2: Expression): Expression =
  result = OpExpression(name: "semiminus", args: @[exp1, exp2])

proc tupleFrom*(exp: Expression): Expression =
  ## Creates an expression that extracts a single tuple from a table.
  result = OpExpression(name: "to_tuple", args: @[exp])

proc dateTimeFromDuro(duroval: ptr RDB_object,
                      tx: Transaction): DateTime =
  var
    yearObj: RDB_object
    monthObj: RDB_object 
    dayObj: RDB_object
    hourObj: RDB_object
    minuteObj: RDB_object
    secondObj: RDB_object
  RDB_init_obj(addr(yearObj))
  RDB_init_obj(addr(monthObj))
  RDB_init_obj(addr(dayObj))
  RDB_init_obj(addr(hourObj))
  RDB_init_obj(addr(minuteObj))
  RDB_init_obj(addr(secondObj))

  try:
    if RDB_obj_property(duroval, cstring("year"), addr(yearObj), nil,
                      addr(tx.database.context.execContext), addr(tx.tx)) != RDB_OK:
      raiseDuroError(tx)
    if RDB_obj_property(duroval, cstring("month"), addr(monthObj), nil,
                      addr(tx.database.context.execContext), addr(tx.tx)) != RDB_OK:
      raiseDuroError(tx)
    if RDB_obj_property(duroval, cstring("day"), addr(dayObj), nil,
                      addr(tx.database.context.execContext), addr(tx.tx)) != RDB_OK:
      raiseDuroError(tx)
    if RDB_obj_property(duroval, cstring("hour"), addr(hourObj), nil,
                      addr(tx.database.context.execContext), addr(tx.tx)) != RDB_OK:
      raiseDuroError(tx)
    if RDB_obj_property(duroval, cstring("minute"), addr(minuteObj), nil,
                      addr(tx.database.context.execContext), addr(tx.tx)) != RDB_OK:
      raiseDuroError(tx)
    if RDB_obj_property(duroval, cstring("second"), addr(secondObj), nil,
                      addr(tx.database.context.execContext), addr(tx.tx)) != RDB_OK:
      raiseDuroError(tx)
    result = DateTime(year: int(RDB_obj_int(addr(yearObj))),
                      month: Month(RDB_obj_int(addr(monthObj))),
                      monthday: MonthdayRange(RDB_obj_int(addr(dayObj))),
                      hour: HourRange(RDB_obj_int(addr(hourObj))),
                      minute: MinuteRange(RDB_obj_int(addr(minuteObj))),
                      second: SecondRange(RDB_obj_int(addr(secondObj))))
  finally:
    RDB_destroy_obj(addr(yearObj), addr(execContext))
    RDB_destroy_obj(addr(monthObj), addr(execContext))
    RDB_destroy_obj(addr(dayObj), addr(execContext))
    RDB_destroy_obj(addr(hourObj), addr(execContext))
    RDB_destroy_obj(addr(minuteObj), addr(execContext))
    RDB_destroy_obj(addr(secondObj), addr(execContext))

proc fromDuro[T](value: var T, duroObj: ptr RDB_object, tx: Transaction)

proc tupleFromDuroPossreps[T](t: var T, duroObj: ptr RDB_object, tx: Transaction) =
  for name, value in fieldPairs(t):
    var
      prop: RDB_object
    RDB_init_obj(addr(prop))
    if RDB_obj_property(duroObj, cstring(name), addr(prop), nil,
                        addr(tx.database.context.execContext), addr(tx.tx)) != RDB_OK:
      RDB_destroy_obj(addr(prop), addr(tx.database.context.execContext))
      raise newException(ValueError, "property " & name & " not found")
    fromDuro(value, addr(prop), tx)
    RDB_destroy_obj(addr(prop), addr(tx.database.context.execContext))

proc tupleFromDuro[T](t: var T, durotup: ptr RDB_object, tx: Transaction) =
  for name, value in fieldPairs(t):
    let pDuroval = RDB_tuple_get(durotup, cstring(name))
    if pDuroval == nil:
       raise newException(KeyError, "attribute " & name & " not found")
    fromDuro(value, pDuroval, tx)

type
  Direction* = enum asc, desc
  SeqItem* = object
    attr*: string
    dir*: Direction

proc toSeq[T](s: var seq[T], tb: ptr RDB_object, tx: Transaction, order: varargs[SeqItem]) =
  var orderseq = newSeq[RDB_seq_item](order.len);
  for i in 0..<order.len:
    orderseq[i].attrname = cstring(order[i].attr)
    orderseq[i].asc = cchar(if order[i].dir == asc: 1 else: 0)
  var arr: RDB_object
  RDB_init_obj(addr(arr))
  try:
    if RDB_table_to_array(addr(arr), tb, cint(order.len),
                          if order.len > 0: addr(orderseq[0]) else: nil,
                          cint(0),
                          addr(tx.database.context.execContext),
                          addr(tx.tx)) != 0:
      raiseDuroError(tx)
    s = @[]
    var t: T
    for i in 0..<RDB_array_length(addr(arr), addr(tx.database.context.execContext)):
      tupleFromDuro(t, RDB_array_get(addr(arr), cint(i), addr(tx.database.context.execContext)),
                    tx)
      s.add(t)
  finally:
    RDB_destroy_obj(addr(arr), addr(tx.database.context.execContext))

proc fromDuro[T](value: var T, duroObj: ptr RDB_object, tx: Transaction) =
  let typ = RDB_obj_type(duroObj)
  when value is bool:
    if typ != addr(RDB_BOOLEAN):
      raise newException(ValueError, "not a boolean")
    value = bool(RDB_obj_bool(duroObj))
  elif value is string:
    if typ != addr(RDB_STRING):
      raise newException(ValueError, "not a string")
    value = $RDB_obj_string(duroObj)
  elif value is int:
    if typ != addr(RDB_INTEGER):
      raise newException(ValueError, "not an integer")
    value = int(RDB_obj_int(duroObj))
  elif value is float:
    if typ != addr(RDB_FLOAT):
      raise newException(ValueError, "not a float")
    value = float(RDB_obj_float(duroObj))
  elif value is seq[byte]:
    if typ != addr(RDB_BINARY):
      raise newException(ValueError, "not a binary")
    var bp: ptr byte
    let len = RDB_binary_length(duroObj)
    if RDB_binary_get(duroObj, 0, len, addr(tx.database.context.execContext),
                      addr(bp), nil) != RDB_OK:
      raiseDuroError(tx)
    newSeq(value, len)
    copyMem(addr(value[0]), bp, len)
  elif value is tuple:
    if RDB_is_tuple(duroObj) == cchar(0):
      tupleFromDuroPossreps(value, duroObj, tx)
    else:
      tupleFromDuro(value, duroObj, tx)
  elif value is seq:
    if (typ == nil) or (RDB_type_is_relation(typ) == cchar(0)):
      raise newException(ValueError, "not a table")
    toSeq(value, duroObj, tx)
  elif value is DateTime:
    value = dateTimeFromDuro(duroObj, tx)
  else:
    raise newException(ValueError, "value not supported")

method toDuroExpression(exp: Expression, pExecContext: RDB_exec_context): RDB_expression {.base.} = nil

method toDuroExpression(exp: VarExpression, pExecContext: RDB_exec_context): RDB_expression =
  result = RDB_var_ref(cstring(exp.name), pExecContext)
  if result == nil:
    raiseDuroError(pExecContext)

method toDuroExpression(exp: BoolExpression, pExecContext: RDB_exec_context): RDB_expression =
  result = RDB_bool_to_expr(cchar(exp.value), pExecContext)
  if result == nil:
    raiseDuroError(pExecContext)

method toDuroExpression(exp: StringExpression, pExecContext: RDB_exec_context): RDB_expression =
  result = RDB_string_to_expr(cstring(exp.value), pExecContext)
  if result == nil:
    raiseDuroError(pExecContext)

method toDuroExpression(exp: IntExpression, pExecContext: RDB_exec_context): RDB_expression =
  result = RDB_int_to_expr(cint(exp.value), pExecContext)
  if result == nil:
    raiseDuroError(pExecContext)

method toDuroExpression(exp: FloatExpression, pExecContext: RDB_exec_context): RDB_expression =
  result = RDB_float_to_expr(cdouble(exp.value), pExecContext)
  if result == nil:
    raiseDuroError(pExecContext)

method toDuroExpression(exp: StringSeqExpression, pExecContext: RDB_exec_context): RDB_expression =
  var
    dArray: RDB_object
  RDB_init_obj(addr(dArray))
  try:
    if RDB_set_array_length(addr(dArray), cint(exp.value.len), pExecContext) != RDB_OK:
      raiseDuroError(pExecContext)
    for i in 0..<exp.value.len:
      if RDB_string_to_obj(RDB_array_get(addr(dArray), cint(i), pExecContext),
                           cstring(exp.value[i]),
                           pExecContext) != RDB_OK:
        raiseDuroError(pExecContext)
    result = RDB_obj_to_expr(addr(dArray), pExecContext)
    
    # Set expression type
    let expType = RDB_new_array_type(addr(RDB_STRING), pExecContext)
    if expType == nil:
      raiseDuroError(pExecContext)
    RDB_set_expr_type(result, expType)
    
  finally:
    RDB_destroy_obj(addr(dArray), pExecContext)

method toDuroExpression(exp: OpExpression, pExecContext: RDB_exec_context): RDB_expression =
  result = RDB_ro_op(cstring(exp.name), pExecContext)
  if result == nil:
    raiseDuroError(pExecContext)
  for i in 0..<len(exp.args):
    RDB_add_arg(result, toDuroExpression(exp.args[i], pExecContext))

method toDuroExpression(exp: ByteSeqExpression, pExecContext: RDB_exec_context): RDB_expression =
  var
    obj: RDB_object
  RDB_init_obj(addr(obj))
  result = RDB_obj_to_expr(addr(obj), pExecContext)
  RDB_destroy_obj(addr(obj), pExecContext)
  if result == nil:
    raiseDuroError(pExecContext)    
  if RDB_binary_set(RDB_expr_obj(result), csize(0), addr(exp.value[0]),
                    csize(exp.value.len), pExecContext) != RDB_OK:
    raiseDuroError(pExecContext)

method toDuroExpression(exp: PropertyExpression, pExecContext: RDB_exec_context): RDB_expression =
  result = RDB_expr_property(toDuroExpression(exp.obj, pExecContext), cstring(exp.propName),
                             pExecContext);
  if result == nil:
    raiseDuroError(pExecContext)

proc toBool*(exp: Expression, tx: Transaction): bool =
  ## Evaluates an expression to int.
  var dexp = toDuroExpression(exp, addr(tx.database.context.execContext))
  var dobj: RDB_object
  RDB_init_obj(addr(dobj))
  try:
    if RDB_evaluate(dexp, pointer(nil), pointer(nil), tx.database.context.pEnv,
                    addr(tx.database.context.execContext), addr(tx.tx),
                    addr(dobj)) != 0:
      raiseDuroError(tx)
    if RDB_obj_type(addr(dobj)) != addr(RDB_INTEGER):
      raise newException(ValueError, "not an integer")
    result = bool(RDB_obj_bool(addr(dobj)))
  finally:
    discard RDB_destroy_obj(addr(dobj), addr(tx.database.context.execContext))
    RDB_del_expr(dexp, addr(tx.database.context.execContext))

proc toInt*(exp: Expression, tx: Transaction): int =
  ## Evaluates an expression to int.
  var dexp = toDuroExpression(exp, addr(tx.database.context.execContext))
  var dobj: RDB_object
  RDB_init_obj(addr(dobj))
  try:
    if RDB_evaluate(dexp, pointer(nil), pointer(nil), tx.database.context.pEnv,
                    addr(tx.database.context.execContext), addr(tx.tx),
                    addr(dobj)) != 0:
      raiseDuroError(tx)
    if RDB_obj_type(addr(dobj)) != addr(RDB_INTEGER):
      raise newException(ValueError, "not an integer")
    result = int(RDB_obj_int(addr(dobj)))
  finally:
    discard RDB_destroy_obj(addr(dobj), addr(tx.database.context.execContext))
    RDB_del_expr(dexp, addr(tx.database.context.execContext))

proc toFloat*(exp: Expression, tx: Transaction): float =
  ## Evaluates an expression to float.
  var dexp = toDuroExpression(exp, addr(tx.database.context.execContext))
  var dobj: RDB_object
  RDB_init_obj(addr(dobj))
  try:
    if RDB_evaluate(dexp, pointer(nil), pointer(nil), tx.database.context.pEnv,
                    addr(tx.database.context.execContext), addr(tx.tx),
                    addr(dobj)) != 0:
      raiseDuroError(tx)
    if RDB_obj_type(addr(dobj)) != addr(RDB_FLOAT):
      raise newException(ValueError, "not a float")
    result = float(RDB_obj_float(addr(dobj)))
  finally:
    RDB_destroy_obj(addr(dobj), addr(tx.database.context.execContext))
    RDB_del_expr(dexp, addr(tx.database.context.execContext))

proc toString*(exp: Expression, tx: Transaction): string =
  ## Evaluates an expression to string.
  var dexp = toDuroExpression(exp, addr(tx.database.context.execContext))
  var dobj: RDB_object
  RDB_init_obj(addr(dobj))
  try:
    if RDB_evaluate(dexp, pointer(nil), pointer(nil), tx.database.context.pEnv,
                    addr(tx.database.context.execContext), addr(tx.tx),
                    addr(dobj)) != 0:
      raiseDuroError(tx)
    if RDB_obj_type(addr(dobj)) != addr(RDB_STRING):
      raise newException(ValueError, "not a float")
    let csresult = RDB_obj_string(addr(dobj))
    result = newString(len(csresult))
    for i in 0..<len(csresult):
      result[i] = csresult[i]
  finally:
    RDB_destroy_obj(addr(dobj), addr(tx.database.context.execContext))
    RDB_del_expr(dexp, addr(tx.database.context.execContext))

proc toTuple*[T](t: var T, exp: Expression, tx: Transaction) =
  ## Converts an expression to a tuple.
  var dexp = toDuroExpression(exp, addr(tx.database.context.execContext))
  var obj: RDB_object
  RDB_init_obj(addr(obj))
  try:
    if RDB_evaluate(dexp, pointer(nil), pointer(nil), tx.database.context.pEnv,
                    addr(tx.database.context.execContext), addr(tx.tx),
                    addr(obj)) != RDB_OK:
      raiseDuroError(tx)
    if RDB_is_tuple(addr(obj)) == cchar(0):
      raise newException(ValueError, "not a tuple")
    tupleFromDuro(t, addr(obj), tx)
  finally:
    discard RDB_destroy_obj(addr(obj), addr(tx.database.context.execContext))
    RDB_del_expr(dexp, addr(tx.database.context.execContext))

proc load*[T](s: var seq[T], exp: Expression, tx: Transaction, order: varargs[SeqItem]) =
  ## Copies a relational expression to a sequence.
  if exp of VarExpression:
    let tb = RDB_get_table(cstring(VarExpression(exp).name),
                           addr(tx.database.context.execContext),
                           addr(tx.tx))
    if tb == nil:
       raise newException(KeyError, "table " & VarExpression(exp).name & " not found")
    toSeq(s, tb, tx, order)
  else:
    var dexp = toDuroExpression(exp, addr(tx.database.context.execContext))
    var tbobj: RDB_object
    RDB_init_obj(addr(tbobj))
    try:
      if RDB_evaluate(dexp, nil, nil, tx.database.context.pEnv,
                    addr(tx.database.context.execContext), addr(tx.tx),
                    addr(tbobj)) != 0:
        RDB_destroy_obj(addr(tbobj), addr(tx.database.context.execContext))
        raiseDuroError(tx)
      toSeq(s, addr(tbobj), tx, order)
    finally:
      RDB_destroy_obj(addr(tbobj), addr(tx.database.context.execContext))
      RDB_del_expr(dexp, addr(tx.database.context.execContext))

proc toDuroObj(dest: ptr RDB_object, dt: DateTime) =
  var
    yearObj: RDB_object
    monthObj: RDB_object 
    dayObj: RDB_object
    hourObj: RDB_object
    minuteObj: RDB_object
    secondObj: RDB_object
  RDB_init_obj(addr(yearObj))
  RDB_init_obj(addr(monthObj))
  RDB_init_obj(addr(dayObj))
  RDB_init_obj(addr(hourObj))
  RDB_init_obj(addr(minuteObj))
  RDB_init_obj(addr(secondObj))
  RDB_int_to_obj(addr(yearObj), cint(dt.year))
  RDB_int_to_obj(addr(monthObj), cint(dt.month))
  RDB_int_to_obj(addr(dayObj), cint(dt.monthday))
  RDB_int_to_obj(addr(hourObj), cint(dt.hour))
  RDB_int_to_obj(addr(minuteObj), cint(dt.minute))
  RDB_int_to_obj(addr(secondObj), cint(dt.second))
  let selectorArgs = @[addr(yearObj), addr(monthObj), addr(dayObj),
                       addr(hourObj), addr(minuteObj), addr(secondObj)]  
  try:
    if RDB_call_ro_op_by_name(cstring("datetime"),
                              cint(selectorArgs.len), unsafeAddr(selectorArgs[0]),
                              addr(execContext), nil, dest) != 0:
      raiseDuroError(addr(execContext))
  finally:
    RDB_destroy_obj(addr(yearObj), addr(execContext))
    RDB_destroy_obj(addr(monthObj), addr(execContext))
    RDB_destroy_obj(addr(dayObj), addr(execContext))
    RDB_destroy_obj(addr(hourObj), addr(execContext))
    RDB_destroy_obj(addr(minuteObj), addr(execContext))
    RDB_destroy_obj(addr(secondObj), addr(execContext))

proc toDuroObj[T: tuple](dest: ptr RDB_object, source: seq[T])

proc toDuroObj[T: tuple](dest: ptr RDB_object, source: T) =
  for name, value in fieldPairs(source):
    when value is bool:
      if RDB_tuple_set_bool(dest, cstring(name), cchar(value),
                           addr(execContext)) != 0:
        raiseDuroError(addr(execContext))
    elif value is string:
      if RDB_tuple_set_string(dest, cstring(name), cstring(value), 
                           addr(execContext)) != 0:
        raiseDuroError(addr(execContext))
    elif value is int:
      if RDB_tuple_set_int(dest, cstring(name), cint(value), 
                           addr(execContext)) != 0:
        raiseDuroError(addr(execContext))
    elif value is float:
      if RDB_tuple_set_float(dest, cstring(name), cdouble(value), 
                           addr(execContext)) != 0:
        raiseDuroError(addr(execContext))
    elif value is seq[byte]:
      var binobj: RDB_object
      RDB_init_obj(addr(binobj))
      try:
        if RDB_binary_set(addr(binobj), 0, unsafeAddr(value[0]), len(value),
                          addr(execContext)) != 0:
          raiseDuroError(addr(execContext))
        if RDB_tuple_set(dest, cstring(name), addr(binobj),
                        addr(execContext)) != 0:
          raiseDuroError(addr(execContext))
      finally:
        RDB_destroy_obj(addr(binobj), addr(execContext))
    elif value is DateTime:
      var dtobj: RDB_object
      RDB_init_obj(addr(dtobj))
      try:
        toDuroObj(addr(dtobj), value)
        if RDB_tuple_set(dest, cstring(name), addr(dtobj),
                        addr(execContext)) != 0:
          raiseDuroError(addr(execContext))
      finally:
        RDB_destroy_obj(addr(dtobj), addr(execContext))
    elif value is tuple:
      var tpobj: RDB_object
      RDB_init_obj(addr(tpobj))
      try:
        toDuroObj(addr(tpobj), value)
        if RDB_tuple_set(dest, cstring(name), addr(tpobj),
                        addr(execContext)) != 0:
          raiseDuroError(addr(execContext))
      finally:
        RDB_destroy_obj(addr(tpobj), addr(execContext))
    elif value is seq:
      var tbobj: RDB_object
      RDB_init_obj(addr(tbobj))
      try:
        toDuroObj(addr(tbobj), value)
        if RDB_tuple_set(dest, cstring(name), addr(tbobj),
                        addr(execContext)) != 0:
          raiseDuroError(addr(execContext))
      finally:
        RDB_destroy_obj(addr(tbobj), addr(execContext))
    else:
      raise newException(ValueError, "invalid value")

proc toDuroObj[T: tuple](dest: ptr RDB_object, source: seq[T]) =
  var
    attr: RDB_attr
    attrs: seq[RDB_attr] = @[]
    t: T
  for tname, value in fieldPairs(t):
    attr.name = cstring(tname)
    when value is bool:
      attr.typ = addr(RDB_BOOLEAN)
    elif value is string:
      attr.typ = addr(RDB_STRING)
    elif value is int:
      attr.typ = addr(RDB_INTEGER)
    elif value is float:
      attr.typ = addr(RDB_FLOAT)
    elif value is seq[byte]:
      attr.typ = addr(RDB_BINARY)
    else:
      raise newException(ValueError, "invalid value")
    attrs.add(attr)

  if RDB_init_table(dest, nil, cint(attrs.len), if attrs.len > 0: addr(attrs[0]) else: nil,
                    0, nil, addr(execContext)) != RDB_OK:
    raiseDuroError(addr(execContext))

  for tup in source:
    var tupobj: RDB_object
    RDB_init_obj(addr(tupobj))
    try:
      toDuroObj(addr(tupobj), tup)
    except DuroError:
      RDB_destroy_obj(addr(tupobj), addr(execContext))
      raise getCurrentException()
    let res = RDB_insert(dest, addr(tupobj), addr(execContext), nil);
    RDB_destroy_obj(addr(tupobj), addr(execContext))
    if res != 0:
      raiseDuroError(addr(execContext))

proc insertS*[T](tbName: string, t: T, tx: Transaction) =
  let tb = RDB_get_table(cstring(tbName),
                         addr(tx.database.context.execContext),
                         addr(tx.tx))
  if tb == nil:
    raise newException(KeyError, "table " & tbName & " not found")
  var obj: RDB_object
  RDB_init_obj(addr(obj))
  toDuroObj(addr(obj), t)
  try:
    if RDB_insert(tb, addr(obj), addr(tx.database.context.execContext),
                addr(tx.tx)) != 0:
      raiseDuroError(tx)
  finally:
    RDB_destroy_obj(addr(obj), addr(tx.database.context.execContext))

macro insert*(dest: untyped, src: untyped, tx: Transaction): typed =
  ## Inserts a Nim tuple into the table given by 'dest'.
  result = newCall("insertS", toStrLit(dest), src, tx)

proc deleteS*(tbName: string, cond: Expression, tx: Transaction): int {.discardable.} =
  ## Deletes the tuples for which 'cond' evaluates to true from the table
  ## given by 'v'.
  let tb = RDB_get_table(cstring(tbname),
                         addr(tx.database.context.execContext),
                         addr(tx.tx))
  if tb == nil:
    raise newException(KeyError, "table " & tbName & " not found")
  let dcond = toDuroExpression(cond, addr(tx.database.context.execContext));
  result = RDB_delete(tb, dcond, addr(tx.database.context.execContext), addr(tx.tx));
  RDB_del_expr(dcond, addr(tx.database.context.execContext))
  if result < 0:
    raiseDuroError(tx)

macro delete*(dest: untyped, cond:Expression, tx: Transaction): int {.discardable.} =
  result = newCall("deleteS", toStrLit(dest), cond, tx)

proc updateS*(tbName: string, cond: Expression, tx: Transaction,
                 updexprs: varargs[Expression]): int {.discardable.} =
  let tb = RDB_get_table(cstring(tbName),
                         addr(tx.database.context.execContext),
                         addr(tx.tx))
  if tb == nil:
    raise newException(KeyError, "table " & tbName & " not found")
  var updates: seq[RDB_attr_update]
  newSeq(updates, updexprs.len div 2);
  for i in 0..<updates.len:
    updates[i].exp = nil
  var dcond: RDB_expression = nil
  try:
    for i in 0..<updates.len:
      updates[i].name = cstring(StringExpression(updexprs[i * 2 + 1]).value)
      updates[i].exp = toDuroExpression(updexprs[i * 2], addr(tx.database.context.execContext))
      if updates[i].exp == nil:
        raiseDuroError(tx)
    dcond = toDuroExpression(cond, addr(tx.database.context.execContext))
    if dcond == nil:
      raiseDuroError(tx)
    result = RDB_update(tb, dcond, cint(updates.len), addr(updates[0]),
                        addr(tx.database.context.execContext), addr(tx.tx));
    if result < 0:
      raiseDuroError(tx)
  finally:
    if dcond != nil:
      RDB_del_expr(dcond, addr(tx.database.context.execContext))
    for i in 0..<updates.len:
      if updates[i].exp != nil:
        RDB_del_expr(updates[i].exp, addr(tx.database.context.execContext))

macro update*(dest: untyped, cond: Expression, tx: Transaction,
              assigns: varargs[untyped]): int {.discardable.} =
  ## Updates the table given by 'v'.
  ## Example: duro.update(t1, V(n) $= 1, tx, s := toExpr("NewValue"))
  var opargs: seq[NimNode] = newSeq[NimNode](2 * len(assigns) + 3);
  opargs[0] = toStrLit(dest)
  opargs[1] = cond
  opargs[2] = tx
  for i in 0..<len(assigns):
    if len(assigns[i]) != 3:
      raise newException(ValueError, "invalid assignment")
    if toStrLit(assigns[i][0]).strVal != ":=":
      raise newException(ValueError, "invalid assignment")
    if assigns[i][2].kind == nnkStrLit:
      opargs[i * 2 + 3] = newCall("toExpr", toStrLit(assigns[i][2]))
    else:
      opargs[i * 2 + 3] = assigns[i][2]
    opargs[i * 2 + 4] = newCall("toExpr", toStrLit(assigns[i][1]))
  result = newCall("updateS", opargs)

proc copyAssignment*[T](dest: string, src: T): Assignment =
  result = new Assignment
  result.kind = akCopy
  result.copyDest = dest
  RDB_init_obj(addr(result.copySource))
  try:
    toDuroObj(addr(result.copySource), src)
  except DuroError:
    RDB_destroy_obj(addr(result.insertSource), addr(execContext))
    raise getCurrentException()

macro `:=`*[T](dest: untyped, src: T): Assignment =
  result = newCall("copyAssignment", toStrLit(dest), src)

proc attributeAssignment*(dest: string, src: Expression): AttrUpdate =
  result.name = dest
  result.exp = src

macro `:=`*(dest: untyped, src: Expression): AttrUpdate =
  result = newCall("attributeAssignment", toStrLit(dest), src)

proc insertAssignment*[T](dest: string, src: T): Assignment =
  result = Assignment(kind: akInsert, insertDest: dest, insertFlags: 0)
  RDB_init_obj(addr(result.insertSource))
  try:
    toDuroObj(addr(result.insertSource), src)
  except DuroError:
    RDB_destroy_obj(addr(result.insertSource), addr(execContext))
    raise getCurrentException()

macro insert*[T](insDest: untyped, insSrc: T): Assignment =
  result = newCall("insertAssignment", toStrLit(insDest), insSrc)

proc deleteAssignment*(dest: string, cond: Expression): Assignment =
  result = Assignment(kind: akDelete, deleteDest: dest, deleteCond: cond)

macro delete*(delDest: untyped, delCond: Expression): Assignment =
  result = newCall("deleteAssignment", toStrLit(delDest), delCond)

proc updateAssignment*(tbName: string, cond: Expression,
                 attrUpdates: varargs[AttrUpdate]): Assignment =
  result = Assignment(kind: akUpdate, updateDest: tbName, updateCond: cond)

  result.attrUpdates = newSeq[AttrUpdate](attrUpdates.len)
  for i in 0..<result.attrUpdates.len:
    result.attrUpdates[i] = attrUpdates[i]

macro update*(dest: untyped, cond: Expression,
              attrUpdates: varargs[AttrUpdate]): Assignment =
  var updateAssignmentArgs: seq[NimNode] = @[toStrLit(dest), cond]
  for attrUpdate in attrUpdates:
    updateAssignmentArgs.add(attrUpdate)
  result = newCall("updateAssignment", updateAssignmentArgs)

proc vDeleteAssignment*[T](dest: string, src: T): Assignment =
  result = Assignment(kind: akVDelete, vDeleteDest: dest, vDeleteFlags: 0)
  RDB_init_obj(addr(result.vDeleteSource))
  try:
    toDuroObj(addr(result.vDeleteSource), src)
  except DuroError:
    RDB_destroy_obj(addr(result.vDeleteSource), addr(execContext))
    raise getCurrentException()

macro delete*[T](delDest: untyped, delSrc: T): Assignment =
  result = newCall("vDeleteAssignment", toStrLit(delDest), delSrc)

proc assign*(assigns: varargs[Assignment], tx: Transaction): int =
  ## Multiple assignment.
  ## Supports copying, insert, update, and delete in a single call.
  ## Example:
  ## ::
  ## assign(duro.insert(t1, (n: 1, s: "Ooey")),
  ##        duro.update(t2, V(n) $= 1, s := toExpr("Bar")),
  ##        duro.delete(t3, V(n) $= 1),
  ##        duro.delete(t4, (n: 1, s: "Foo")),
  ##        t5 := @[(n: 1, s: "foo"),
  ##                (n: 2, s: "bar")],
  ##        tx)
  var copySeq: seq[RDB_ma_copy] = @[]
  var insertSeq: seq[RDB_ma_insert] = @[]
  var updateSeq: seq[RDB_ma_update] = @[]
  var deleteSeq: seq[RDB_ma_delete] = @[]
  var vDeleteSeq: seq[RDB_ma_vdelete] = @[]
  for i in 0..<assigns.len:
    case assigns[i].kind
      of akCopy:
        var maCopy: RDB_ma_copy
        maCopy.tbp = RDB_get_table(cstring(assigns[i].copyDest),
                         addr(tx.database.context.execContext),
                         addr(tx.tx))
        if maCopy.tbp == nil:
          raise newException(KeyError, "table " & assigns[i].copyDest & " not found")
        maCopy.objp = addr(assigns[i].copySource)
        copySeq.add(maCopy)
      of akInsert:
        var maInsert: RDB_ma_insert
        maInsert.tbp = RDB_get_table(cstring(assigns[i].insertDest),
                         addr(tx.database.context.execContext),
                         addr(tx.tx))
        if maInsert.tbp == nil:
          raise newException(KeyError, "table " & assigns[i].insertDest & " not found")
        maInsert.objp = addr(assigns[i].insertSource)
        insertSeq.add(maInsert)
      of akUpdate:
        var maUpdate: RDB_ma_update
        maUpdate.tbp = RDB_get_table(cstring(assigns[i].updateDest),
                         addr(tx.database.context.execContext),
                         addr(tx.tx))
        if maUpdate.tbp == nil:
          raise newException(KeyError, "table " & assigns[i].updateDest & " not found")
        var attrUpdates = newSeq[RDB_attr_update](assigns[i].attrUpdates.len)
        for j in 0..<assigns[i].attrUpdates.len:
          attrUpdates[j].name = cstring(assigns[i].attrUpdates[j].name)
          attrUpdates[j].exp = toDuroExpression(assigns[i].attrUpdates[j].exp,
                                                addr(tx.database.context.execContext))
        if assigns[i].updateCond != nil:
          maUpdate.condp = toDuroExpression(assigns[i].updateCond, addr(tx.database.context.execContext))
        else:
          maUpdate.condp = nil
        maUpdate.updc = cint(assigns[i].attrUpdates.len)
        if maUpdate.updc > 0:
          maUpdate.updv = addr(attrUpdates[0])
        updateSeq.add(maUpdate)
      of akDelete:
        var maDelete: RDB_ma_delete
        maDelete.tbp = RDB_get_table(cstring(assigns[i].deleteDest),
                         addr(tx.database.context.execContext),
                         addr(tx.tx))
        if maDelete.tbp == nil:
          raise newException(KeyError, "table " & assigns[i].deleteDest & " not found")
        maDelete.condp = toDuroExpression(assigns[i].deleteCond, addr(tx.database.context.execContext))
        deleteSeq.add(maDelete)
      of akVDelete:
        var maVDelete: RDB_ma_vdelete
        maVDelete.tbp = RDB_get_table(cstring(assigns[i].vDeleteDest),
                         addr(tx.database.context.execContext),
                         addr(tx.tx))
        if maVDelete.tbp == nil:
          raise newException(KeyError, "table " & assigns[i].vDeleteDest & " not found")
        maVDelete.objp = addr(assigns[i].vDeleteSource)
        vDeleteSeq.add(maVDelete)
  result = int(RDB_multi_assign(cint(insertSeq.len), if insertSeq.len > 0: addr(insertSeq[0]) else: nil,
                                cint(updateSeq.len), if updateSeq.len > 0: addr(updateSeq[0]) else: nil,
                                cint(deleteSeq.len), if deleteSeq.len > 0: addr(deleteSeq[0]) else: nil,
                                cint(vDeleteSeq.len), if vDeleteSeq.len > 0: addr(vDeleteSeq[0]) else: nil,
                                cint(copySeq.len), if copySeq.len > 0: addr(copySeq[0]) else: nil,
                                nil, nil,
                                addr(tx.database.context.execContext), addr(tx.tx)))
  for i in 0..<insertSeq.len:
    RDB_destroy_obj(insertSeq[i].objp, addr(tx.database.context.execContext))
  for i in 0..<copySeq.len:
    RDB_destroy_obj(copySeq[i].objp, addr(tx.database.context.execContext))
  for i in 0..<updateSeq.len:
    if updateSeq[i].condp != nil:
      RDB_del_expr(updateSeq[i].condp, addr(tx.database.context.execContext))
    for j in 0..<updateSeq[i].updc:
      let upd = cast[ptr RDB_attr_update]
                    (cast[uint](updateSeq[i].updv) + cast[uint](j * sizeof(ptr RDB_attr_update)))
      RDB_del_expr(upd.exp, addr(tx.database.context.execContext))
  for i in 0..<deleteSeq.len:
    RDB_del_expr(deleteSeq[i].condp, addr(tx.database.context.execContext))
  for i in 0..<vDeleteSeq.len:
    RDB_destroy_obj(vDeleteSeq[i].objp, addr(tx.database.context.execContext))
