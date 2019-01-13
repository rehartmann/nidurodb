#
# libduro C interface
#

const
  RDB_OK* = 0

  libduro = "libduro.so"

var
  RDB_BOOLEAN* {.dynlib: libduro, importc.}: pointer
  RDB_INTEGER* {.dynlib: libduro, importc.}: pointer
  RDB_FLOAT* {.dynlib: libduro, importc.}: pointer
  RDB_STRING* {.dynlib: libduro, importc.}: pointer
  RDB_BINARY* {.dynlib: libduro, importc.}: pointer

type
  RDB_expression* = ptr object

  RDB_object* = object
    data: array[36, char]

  RDB_seq_item* = object
    attrname*: cstring
    asc*: cchar

  RDB_attr_update* = object
    name*: cstring
    exp*: RDB_expression

  RDB_exec_contextObj* = array[60, char]
  
  RDB_exec_context* = ptr RDB_exec_contextObj

  RDB_transactionObj* = object
    tx: array[7, pointer]  

  RDB_transaction* = ptr RDB_transactionObj

  RDB_ma_insert* = object
    tbp*: ptr RDB_object
    objp*: ptr RDB_object
    flags*: cint
    
  RDB_ma_update* = object
    tbp*: ptr RDB_object
    condp*: RDB_expression
    updc*: cint
    updv*: ptr RDB_attr_update

  RDB_ma_delete* = object
    tbp*: ptr RDB_object
    condp*: RDB_expression

  RDB_ma_vdelete* = object
    tbp*: ptr RDB_object
    objp*: ptr RDB_object
    flags*: cint

  RDB_ma_copy* = object
    tbp*: ptr RDB_object
    objp*: ptr RDB_object

  RDB_attr* = object
    name*: cstring
    typ*: pointer
    default*: RDB_expression
    options*: cint

proc RDB_init_exec_context*(pExecContext: RDB_exec_context)
   {.cdecl, dynlib: libduro, importc.}

proc RDB_destroy_exec_context*(pExecContext: RDB_exec_context)
   {.cdecl, dynlib: libduro, importc.}

proc RDB_open_env*(path: cstring, flags: cint, penv: pointer): pointer
   {.cdecl, dynlib: libduro, importc.}

proc RDB_close_env*(env: pointer, pExecContext: RDB_exec_context): cint
   {.cdecl, dynlib: libduro, importc.}

proc RDB_get_db_from_env*(name: cstring, env: pointer, pExecContext: RDB_exec_context, pTx: RDB_transaction): pointer
   {.cdecl, dynlib: libduro, importc.}

proc RDB_begin_tx*(pExecContext: RDB_exec_context, pTx: RDB_transaction, db: pointer, parent: pointer): cint
   {.cdecl, dynlib: libduro, importc.}

proc RDB_commit*(pExecContext: RDB_exec_context, pTx: RDB_transaction): cint
   {.cdecl, dynlib: libduro, importc.}

proc RDB_rollback*(pExecContext: RDB_exec_context, pTx: RDB_transaction): cint
   {.cdecl, dynlib: libduro, importc.}

proc RDB_get_table*(name: cstring, pExecContext: RDB_exec_context, pTx: RDB_transaction): ptr RDB_object
   {.cdecl, dynlib: libduro, importc.}

proc RDB_extract_tuple*(tbp: pointer, pExecContext: RDB_exec_context, pTx: RDB_transaction, tplp: pointer): cint
   {.cdecl, dynlib: libduro, importc.}

proc RDB_init_obj*(pObj: ptr RDB_object)
   {.cdecl, dynlib: libduro, importc.}

proc RDB_destroy_obj*(pObj: ptr RDB_object, pExecContext: RDB_exec_context): cint
   {.cdecl, dynlib: libduro, importc, discardable.}

proc RDB_tuple_get*(pObj: ptr RDB_object, name: cstring): ptr RDB_object
   {.cdecl, dynlib: libduro, importc.}

proc RDB_obj_bool*(pObj: ptr RDB_object): cchar
   {.cdecl, dynlib: libduro, importc.}

proc RDB_obj_string*(objp: ptr RDB_object): cstring
   {.cdecl, dynlib: libduro, importc.}

proc RDB_obj_int*(objp: ptr RDB_object): cint
  {.cdecl, dynlib: libduro, importc.}

proc RDB_int_to_obj*(objp: ptr RDB_object, val: cint)
  {.cdecl, dynlib: libduro, importc.}

proc RDB_obj_float*(objp: ptr RDB_object): cdouble
  {.cdecl, dynlib: libduro, importc.}

proc RDB_string_to_obj*(objp: ptr RDB_object, str: cstring, pExecContext: RDB_exec_context): cint
  {.cdecl, dynlib: libduro, importc.}

proc RDB_obj_type*(objp: ptr RDB_object): pointer
  {.cdecl, dynlib: libduro, importc.}

proc RDB_binary_length*(objp: ptr RDB_object): csize
  {.cdecl, dynlib: libduro, importc.}

proc RDB_binary_get*(objp: ptr RDB_object, pos: csize, len: csize, pExecContext: RDB_exec_context,
                     pp: ptr ptr byte, alenp: ptr csize): cint
  {.cdecl, dynlib: libduro, importc.}

proc RDB_binary_set*(objp: ptr RDB_object, pos: csize, scrp: pointer, len: csize, pExecContext: RDB_exec_context): cint
  {.cdecl, dynlib: libduro, importc.}

proc RDB_type_name*(objp: pointer): cstring
  {.cdecl, dynlib: libduro, importc.}

proc RDB_var_ref*(attrname: cstring, pExecContext: RDB_exec_context): RDB_expression
  {.cdecl, dynlib: libduro, importc.}

proc RDB_bool_to_expr*(b: cchar, pExecContext: RDB_exec_context): RDB_expression
  {.cdecl, dynlib: libduro, importc.}

proc RDB_string_to_expr*(str: cstring, pExecContext: RDB_exec_context):RDB_expression
  {.cdecl, dynlib: libduro, importc.}

proc RDB_int_to_expr*(val: cint, pExecContext: RDB_exec_context): RDB_expression
  {.cdecl, dynlib: libduro, importc.}

proc RDB_float_to_expr*(val: cdouble, pExecContext: RDB_exec_context): RDB_expression
  {.cdecl, dynlib: libduro, importc.}

proc RDB_obj_to_expr*(obj: ptr RDB_object, pExecContext: RDB_exec_context): RDB_expression
  {.cdecl, dynlib: libduro, importc.}

proc RDB_ro_op*(opname: cstring, pExecContext: RDB_exec_context): RDB_expression
  {.cdecl, dynlib: libduro, importc.}

proc RDB_add_arg*(dexp: RDB_expression, arg: RDB_expression)
  {.cdecl, dynlib: libduro, importc.}

proc RDB_expr_property*(arg: RDB_expression, propName: cstring,
  pExecContext: RDB_exec_context): RDB_expression   
  {.cdecl, dynlib: libduro, importc.}

proc RDB_evaluate*(exp: RDB_expression, getfnp: pointer, getdata: pointer, env: pointer,
  pExecContext: RDB_exec_context, pTx: RDB_transaction, val: pointer): cint
  {.cdecl, dynlib: libduro, importc.}

proc RDB_del_expr*(exp: RDB_expression, pExecContext: RDB_exec_context): cint
  {.cdecl, dynlib: libduro, importc, discardable.}

proc RDB_expr_obj*(exp: RDB_expression): ptr RDB_object
  {.cdecl, dynlib: libduro, importc, discardable.}

proc RDB_table_to_array*(arrp: ptr RDB_object, tbp: ptr RDB_object, seqitc: cint, seqitv: ptr RDB_seq_item, flags: cint,
  pExecContext: RDB_exec_context, pTx: RDB_transaction): cint
  {.cdecl, dynlib: libduro, importc.}

proc RDB_array_length*(arrp: ptr RDB_object, pExecContext: RDB_exec_context): cint
  {.cdecl, dynlib: libduro, importc.}

proc RDB_array_get*(arrp: ptr RDB_object, idx: cint, pExecContext: RDB_exec_context): ptr RDB_object
  {.cdecl, dynlib: libduro, importc.}

proc RDB_get_err*(pExecContext: RDB_exec_context): ptr RDB_object
  {.cdecl, dynlib: libduro, importc.}

proc RDB_tuple_set*(pObj: ptr RDB_object, attrname: cstring, obj: ptr RDB_object, pExecContext: RDB_exec_context): cint
  {.cdecl, dynlib: libduro, importc.}

proc RDB_tuple_set_string*(pObj: ptr RDB_object, attrname: cstring, value: cstring, pExecContext: RDB_exec_context): cint
  {.cdecl, dynlib: libduro, importc.}

proc RDB_tuple_set_bool*(pObj: ptr RDB_object, attrname: cstring, value: cchar, pExecContext: RDB_exec_context): cint
  {.cdecl, dynlib: libduro, importc.}

proc RDB_tuple_set_int*(pObj: ptr RDB_object, attrname: cstring, value: cint, pExecContext: RDB_exec_context): cint
  {.cdecl, dynlib: libduro, importc.}

proc RDB_tuple_set_float*(pObj: ptr RDB_object, attrname: cstring, value: cdouble, pExecContext: RDB_exec_context): cint
  {.cdecl, dynlib: libduro, importc.}

proc RDB_insert*(tb: ptr RDB_object, pObj: ptr RDB_object, pExecContext: RDB_exec_context, pTx: RDB_transaction): cint
  {.cdecl, dynlib: libduro, importc.}

proc RDB_delete*(tb: ptr RDB_object, cond: RDB_expression, pExecContext: RDB_exec_context, pTx: RDB_transaction): cint
  {.cdecl, dynlib: libduro, importc.}

proc RDB_update*(tb: ptr RDB_object, cond: RDB_expression, updc: cint, updv: ptr RDB_attr_update,
                 pExecContext: RDB_exec_context, pTx: RDB_transaction): cint
  {.cdecl, dynlib: libduro, importc.}

proc RDB_multi_assign*(insc: cint, insv: ptr RDB_ma_insert, updc: cint, updv: ptr RDB_ma_update,
                      delc: cint, delv: ptr RDB_ma_delete, vdelc: cint, vdelv: ptr RDB_ma_vdelete,
                      copyc: cint, copyv: ptr RDB_ma_copy, getfn: pointer, getarg: pointer,
                      pExecContext: RDB_exec_context, pTx: RDB_transaction): cint
  {.cdecl, dynlib: libduro, importc.}

proc RDB_init_table*(tb: ptr RDB_object, name: cstring, attrc: cint, attrv: ptr RDB_attr,
                   keyc: cint, keyv: pointer, pExecContext: RDB_exec_context): cint
  {.cdecl, dynlib: libduro, importc.}

proc RDB_call_ro_op_by_name*(name: cstring, argc: cint, argv: ptr ptr RDB_object,
                            pExecContext: RDB_exec_context, pTx: RDB_transaction,
                            pRetval: ptr RDB_object): cint
  {.cdecl, dynlib: libduro, importc.}

proc RDB_is_tuple*(pTpl: ptr RDB_object): cchar
  {.cdecl, dynlib: libduro, importc.}

proc RDB_obj_property*(pObj: ptr RDB_object, propname: cstring,
                       pPropVal: ptr RDB_object, pEnv: pointer,
                       pExecContext: RDB_exec_context, pTx: RDB_transaction): cint
  {.cdecl, dynlib: libduro, importc.}

proc RDB_set_array_length*(pObj: ptr RDB_object, len: cint, pExecContext: RDB_exec_context): cint
  {.cdecl, dynlib: libduro, importc.}

proc RDB_new_array_type*(basetyp: pointer, pExecContext: RDB_exec_context): pointer
  {.cdecl, dynlib: libduro, importc.}

proc RDB_set_expr_type*(exp: RDB_expression, typ: pointer)
  {.cdecl, dynlib: libduro, importc.}

proc RDB_type_is_relation*(typ: pointer): cchar
  {.cdecl, dynlib: libduro, importc.}
