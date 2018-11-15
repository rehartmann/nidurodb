import libduro
import macros
import system

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

   OpExpression = ref object of Expression
     name: string
     args: seq[Expression]

   DuroError* = object of Exception

   DContext* = ref object
      pEnv: pointer
      execContext: array[60, char]

   Database* = ref object
      pDb: pointer
      context: DContext

   Transaction* = ref object
      database: Database
      tx: array[7, pointer]

proc raiseDuroError(ecp: pointer) {.noReturn.} =
  let err = RDB_get_err(ecp)
  let errtyp = RDB_obj_type(err)
  if errtyp != nil:
    var errmsg = newString(len(RDB_type_name(errtyp)))
    for i in countup(0, len(errmsg) - 1):
      errmsg[i] = RDB_type_name(errtyp)[i]
    raise newException(DuroError, errmsg)
  raise newException(DuroError, "unkown")

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

macro V*(varname: untyped): Expression =
  ## Converts a identifier to a variable name.
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
  # Creates a projection expression.
  # Example: V(t){ attr1, attr2 }
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
  # Creates a rename expression.
  # Example: V(t).rename(attr1 as new_attr1, attr2 as new_attr2)
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

proc toExpr*(b: bool): Expression =
  return BoolExpression(value: b)

proc toExpr*(i: int): Expression =
  return IntExpression(value: i)

proc toExpr*(f: float): Expression =
  return FloatExpression(value: f)

proc toExpr*(s: string): Expression =
  return StringExpression(value: s)

proc extendExpr*(exp: Expression, assigns: varargs[Expression]): Expression =
  var
    opargs = newSeq[Expression](len(assigns) + 1);
  opargs[0] = exp
  for i in 0..<len(assigns):
    opargs[i + 1] = assigns[i]
  result = OpExpression(name: "extend", args: opargs)

macro extend*(exp: Expression, assigns: varargs[untyped]): untyped =
  # Creates an extend expression.
  # Example: V(t).extend(new_attr1 := <expression>, new_attr2 := <expression>)
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
  # Creates a summarize expression.
  # Example: V(t).summarize(V(t){ attr1 }, new_attr := count(V(attr2)))
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
  # Creates a summarize expression.
  # Example: V(t).where(V(s) $= <value>)
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
  # Creates a semijoin expression.
  result = OpExpression(name: "semijoin", args: @[exp1, exp2])

proc `|><`*(exp1: Expression, exp2: Expression): Expression =
  # Creates a semijoin expression.
  result = semijoin(exp1, exp2)

proc `><|`*(exp1: Expression, exp2: Expression): Expression =
  # Creates a semijoin expression.
  result = semijoin(exp2, exp1)

proc matching*(exp1: Expression, exp2: Expression): Expression =
  # Creates a semijoin expression.
  result = semijoin(exp1, exp2)

proc semiminus*(exp1: Expression, exp2: Expression): Expression =
  result = OpExpression(name: "semiminus", args: @[exp1, exp2])

proc notMatching*(exp1: Expression, exp2: Expression): Expression =
  result = OpExpression(name: "semiminus", args: @[exp1, exp2])

proc tupleFromDuro[T](t: var T, durotup: pointer, ecp: pointer) =
  for name, value in fieldPairs(t):
    let duroval = RDB_tuple_get(durotup, cstring(name))
    if duroval == nil:
       raise newException(KeyError, "attribute " & name & " not found")
    let typ = RDB_obj_type(duroval)
    when value is bool:
      if typ != addr(RDB_BOOLEAN):
        raise newException(ValueError, "not a boolean")
      value = bool(RDB_obj_bool(duroval))
    elif value is string:
      if typ != addr(RDB_STRING):
        raise newException(ValueError, "not a string")
      var cstr = RDB_obj_string(duroval);
      value = newString(len(cstr))
      for i in countup(0, len(value) - 1):
        value[i] = cstr[i]
    elif value is int:
      if typ != addr(RDB_INTEGER):
         raise newException(ValueError, "not an integer")
      value = int(RDB_obj_int(duroval))
    elif value is float:
      if typ != addr(RDB_FLOAT):
        raise newException(ValueError, "not a float")
      value = float(RDB_obj_float(duroval))
    elif value is seq[byte]:
      if typ != addr(RDB_BINARY):
        raise newException(ValueError, "not a binary")
      var bp: ptr byte
      let len = RDB_binary_length(duroval)
      if RDB_binary_get(duroval, 0, len, ecp, addr(bp), nil) != RDB_OK:
        raiseDuroError(ecp)
      newSeq(value, len)
      copyMem(addr(value[0]), bp, len)
    elif value is tuple:
      tupleFromDuro(value, duroval, ecp)
    else:
      raise newException(ValueError, "value not supported")

method toDuroExpression(exp: Expression, ecp: pointer): RDB_expression {.base.} = nil

method toDuroExpression(exp: VarExpression, ecp: pointer): RDB_expression =
  result = RDB_var_ref(cstring(exp.name), ecp)
  if result == nil:
    raiseDuroError(ecp)

method toDuroExpression(exp: BoolExpression, ecp: pointer): RDB_expression =
  result = RDB_bool_to_expr(cchar(exp.value), ecp)
  if result == nil:
    raiseDuroError(ecp)

method toDuroExpression(exp: StringExpression, ecp: pointer): RDB_expression =
  result = RDB_string_to_expr(cstring(exp.value), ecp)
  if result == nil:
    raiseDuroError(ecp)

method toDuroExpression(exp: IntExpression, ecp: pointer): RDB_expression =
  result = RDB_int_to_expr(cint(exp.value), ecp)
  if result == nil:
    raiseDuroError(ecp)

method toDuroExpression(exp: FloatExpression, ecp: pointer): RDB_expression =
  result = RDB_float_to_expr(cdouble(exp.value), ecp)
  if result == nil:
    raiseDuroError(ecp)

method toDuroExpression(exp: OpExpression, ecp: pointer): RDB_expression =
  let dexp = RDB_ro_op(cstring(exp.name), ecp)
  if dexp == nil:
    raiseDuroError(ecp)
  for i in countup(0, len(exp.args)-1):
    RDB_add_arg(dexp, toDuroExpression(exp.args[i], ecp))
  return dexp

proc extractTuple[T](t: var T, tb: pointer, tx: Transaction) =
  var obj: RDB_object
  RDB_init_obj(addr(obj))
  let res = RDB_extract_tuple(tb, addr(tx.database.context.execContext),
                           addr(tx.tx), addr(obj))
  if res != 0:
    discard RDB_destroy_obj(addr(obj), addr(tx.database.context.execContext))
    raiseDuroError(addr(tx.database.context.execContext))
  tupleFromDuro(t, addr(obj), addr(tx.database.context.execContext))
  discard RDB_destroy_obj(addr(obj), addr(tx.database.context.execContext))

proc toBool*(exp: Expression, tx: Transaction): bool =
  # Evaluates an expression to int.
  var dexp = toDuroExpression(exp, addr(tx.database.context.execContext))
  var dobj: RDB_object
  RDB_init_obj(addr(dobj))
  try:
    if RDB_evaluate(dexp, pointer(nil), pointer(nil), tx.database.context.pEnv,
                    addr(tx.database.context.execContext), addr(tx.tx),
                    addr(dobj)) != 0:
      raiseDuroError(addr(tx.database.context.execContext))
    if RDB_obj_type(addr(dobj)) != addr(RDB_INTEGER):
      raise newException(ValueError, "not an integer")
    result = bool(RDB_obj_bool(addr(dobj)))
  finally:
    discard RDB_destroy_obj(addr(dobj), addr(tx.database.context.execContext))
    RDB_del_expr(dexp, addr(tx.database.context.execContext))

proc toInt*(exp: Expression, tx: Transaction): int =
  # Evaluates an expression to int.
  var dexp = toDuroExpression(exp, addr(tx.database.context.execContext))
  var dobj: RDB_object
  RDB_init_obj(addr(dobj))
  try:
    if RDB_evaluate(dexp, pointer(nil), pointer(nil), tx.database.context.pEnv,
                    addr(tx.database.context.execContext), addr(tx.tx),
                    addr(dobj)) != 0:
      raiseDuroError(addr(tx.database.context.execContext))
    if RDB_obj_type(addr(dobj)) != addr(RDB_INTEGER):
      raise newException(ValueError, "not an integer")
    result = int(RDB_obj_int(addr(dobj)))
  finally:
    discard RDB_destroy_obj(addr(dobj), addr(tx.database.context.execContext))
    RDB_del_expr(dexp, addr(tx.database.context.execContext))

proc toFloat*(exp: Expression, tx: Transaction): float =
  # Evaluates an expression to float.
  var dexp = toDuroExpression(exp, addr(tx.database.context.execContext))
  var dobj: RDB_object
  RDB_init_obj(addr(dobj))
  try:
    if RDB_evaluate(dexp, pointer(nil), pointer(nil), tx.database.context.pEnv,
                    addr(tx.database.context.execContext), addr(tx.tx),
                    addr(dobj)) != 0:
      raiseDuroError(addr(tx.database.context.execContext))
    if RDB_obj_type(addr(dobj)) != addr(RDB_FLOAT):
      raise newException(ValueError, "not a float")
    result = float(RDB_obj_float(addr(dobj)))
  finally:
    RDB_destroy_obj(addr(dobj), addr(tx.database.context.execContext))
    RDB_del_expr(dexp, addr(tx.database.context.execContext))

proc toString*(exp: Expression, tx: Transaction): string =
  # Evaluates an expression to string.
  var dexp = toDuroExpression(exp, addr(tx.database.context.execContext))
  var dobj: RDB_object
  RDB_init_obj(addr(dobj))
  try:
    if RDB_evaluate(dexp, pointer(nil), pointer(nil), tx.database.context.pEnv,
                    addr(tx.database.context.execContext), addr(tx.tx),
                    addr(dobj)) != 0:
      raiseDuroError(addr(tx.database.context.execContext))
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
  # Extracts a single tuple from a table.
  # Raises an error if the table is empty or if it contains more than one tuple.
  if exp of VarExpression:
    let tb = RDB_get_table(cstring(VarExpression(exp).name),
                           addr(tx.database.context.execContext),
                           addr(tx.tx))
    if tb == nil:
      raise newException(KeyError, "table " & VarExpression(exp).name & " not found")
    extractTuple(t, tb, tx)
  else:
    var dexp = toDuroExpression(exp, addr(tx.database.context.execContext))
    var tbobj: RDB_object
    RDB_init_obj(addr(tbobj))
    try:
      if RDB_evaluate(dexp, pointer(nil), pointer(nil), tx.database.context.pEnv,
                      addr(tx.database.context.execContext), addr(tx.tx),
                      addr(tbobj)) != 0:
        raiseDuroError(addr(tx.database.context.execContext))
      extractTuple(t, addr(tbobj), tx)
    finally:
      discard RDB_destroy_obj(addr(tbobj), addr(tx.database.context.execContext))
      RDB_del_expr(dexp, addr(tx.database.context.execContext))

type
  Direction* = enum asc, desc
  SeqItem* = object
    attr*: string
    dir*: Direction

proc toSeq[T](s: var seq[T], tb: pointer, tx: Transaction, order: varargs[SeqItem]) =
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
      raiseDuroError(addr(tx.database.context.execContext))
    s = @[]
    var t: T
    for i in 0..<RDB_array_length(addr(arr), addr(tx.database.context.execContext)):
      tupleFromDuro(t, RDB_array_get(addr(arr), i, addr(tx.database.context.execContext)),
                    addr(tx.database.context.execContext))
      s.add(t)
  finally:
    RDB_destroy_obj(addr(arr), addr(tx.database.context.execContext))

proc load*[T](s: var seq[T], exp: Expression, tx: Transaction, order: varargs[SeqItem]) =
  # Copies a relational expression to a sequence.
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
        raiseDuroError(addr(tx.database.context.execContext))
      toSeq(s, addr(tbobj), tx, order)
    finally:
      RDB_destroy_obj(addr(tbobj), addr(tx.database.context.execContext))
      RDB_del_expr(dexp, addr(tx.database.context.execContext))

proc insert*[T](v: Expression, t: T, tx: Transaction) =
  # Inserts a Nim tuple into the table given by 'v'.
  let tb = RDB_get_table(cstring(VarExpression(v).name),
                         addr(tx.database.context.execContext),
                         addr(tx.tx))
  if tb == nil:
    raise newException(KeyError, "table " & VarExpression(v).name & " not found")
  var obj: RDB_object
  RDB_init_obj(addr(obj))
  try:
    for name, value in fieldPairs(t):
      when value is bool:
        if RDB_tuple_set_bool(addr(obj), cstring(name), cchar(value),
                           addr(tx.database.context.execContext)) != 0:
          raiseDuroError(addr(tx.database.context.execContext))
      elif value is string:
        if RDB_tuple_set_string(addr(obj), cstring(name), cstring(value), 
                           addr(tx.database.context.execContext)) != 0:
          raiseDuroError(addr(tx.database.context.execContext))
      elif value is int:
        if RDB_tuple_set_int(addr(obj), cstring(name), cint(value), 
                           addr(tx.database.context.execContext)) != 0:
          raiseDuroError(addr(tx.database.context.execContext))
      elif value is float:
        if RDB_tuple_set_float(addr(obj), cstring(name), cdouble(value), 
                           addr(tx.database.context.execContext)) != 0:
          raiseDuroError(addr(tx.database.context.execContext))
      elif value is seq[byte]:
        var binobj: RDB_object
        RDB_init_obj(addr(binobj))
        try:
          if RDB_binary_set(addr(binobj), 0, unsafeAddr(value[0]), len(value),
                          addr(tx.database.context.execContext)) != 0:
            raiseDuroError(addr(tx.database.context.execContext))
          if RDB_tuple_set(addr(obj), cstring(name), addr(binobj),
                        addr(tx.database.context.execContext)) != 0:
            raiseDuroError(addr(tx.database.context.execContext))
        finally:
          RDB_destroy_obj(addr(binobj), addr(tx.database.context.execContext))
      else:
        raise newException(ValueError, "invalid value")
    if RDB_insert(tb, addr(obj), addr(tx.database.context.execContext),
                addr(tx.tx)) != 0:
      raiseDuroError(addr(tx.database.context.execContext))
  finally:
    RDB_destroy_obj(addr(obj), addr(tx.database.context.execContext))

proc delete*(v: Expression, cond: Expression, tx: Transaction): int {.discardable.} =
  # Deletes the tuples for which 'cond' evaluates to true from the table
  # given by 'v'.
  let tb = RDB_get_table(cstring(VarExpression(v).name),
                         addr(tx.database.context.execContext),
                         addr(tx.tx))
  if tb == nil:
    raise newException(KeyError, "table " & VarExpression(v).name & " not found")
  let dcond = toDuroExpression(cond, addr(tx.database.context.execContext));
  result = RDB_delete(tb, dcond, addr(tx.database.context.execContext), addr(tx.tx));
  RDB_del_expr(dcond, addr(tx.database.context.execContext))
  if result < 0:
    raiseDuroError(addr(tx.database.context.execContext))

proc updateExpr*(v: Expression, cond: Expression, tx: Transaction,
                 updexprs: varargs[Expression]): int {.discardable.} =
  let tb = RDB_get_table(cstring(VarExpression(v).name),
                         addr(tx.database.context.execContext),
                         addr(tx.tx))
  if tb == nil:
    raise newException(KeyError, "table " & VarExpression(v).name & " not found")
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
        raiseDuroError(addr(tx.database.context.execContext))
    dcond = toDuroExpression(cond, addr(tx.database.context.execContext))
    if dcond == nil:
      raiseDuroError(addr(tx.database.context.execContext))
    result = RDB_update(tb, dcond, cint(updates.len), addr(updates[0]),
                        addr(tx.database.context.execContext), addr(tx.tx));
    if result < 0:
      raiseDuroError(addr(tx.database.context.execContext))
  finally:
    if dcond != nil:
      RDB_del_expr(dcond, addr(tx.database.context.execContext))
    for i in 0..<updates.len:
      if updates[i].exp != nil:
        RDB_del_expr(updates[i].exp, addr(tx.database.context.execContext))

macro update*(v: Expression, cond: Expression, tx: Transaction,
              assigns: varargs[untyped]): int {.discardable.} =
  # Updates the table given by 'v'.
  # Example: 
  var opargs: seq[NimNode] = newSeq[NimNode](2 * len(assigns) + 3);
  opargs[0] = v
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
  result = newCall("updateExpr", opargs)
