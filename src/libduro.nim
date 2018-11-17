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
    
proc RDB_init_exec_context*(pExecContext: RDB_exec_context)
   {.cdecl, dynlib: libduro, importc.}

proc RDB_destroy_exec_context*(pExecContext: RDB_exec_context)
   {.cdecl, dynlib: libduro, importc.}

proc RDB_open_env*(path: cstring, flags: cint, penv: pointer): pointer
   {.cdecl, dynlib: libduro, importc.}

proc RDB_close_env*(env: pointer, pExecContext: RDB_exec_context): cint
   {.cdecl, dynlib: libduro, importc.}

proc RDB_get_db_from_env*(name: cstring, env: pointer, pExecContext: RDB_exec_context, tx: pointer): pointer
   {.cdecl, dynlib: libduro, importc.}

proc RDB_begin_tx*(pExecContext: RDB_exec_context, tx: pointer, db: pointer, parent: pointer): cint
   {.cdecl, dynlib: libduro, importc.}

proc RDB_commit*(pExecContext: RDB_exec_context, tx:pointer): cint
   {.cdecl, dynlib: libduro, importc.}

proc RDB_rollback*(pExecContext: RDB_exec_context, tx:pointer): cint
   {.cdecl, dynlib: libduro, importc.}

proc RDB_get_table*(name: cstring, pExecContext: RDB_exec_context, tx: pointer): pointer
   {.cdecl, dynlib: libduro, importc.}

proc RDB_extract_tuple*(tbp: pointer, pExecContext: RDB_exec_context, tx: pointer, tplp: pointer): cint
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

proc RDB_obj_float*(objp: ptr RDB_object): cdouble
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

proc RDB_evaluate*(exp: RDB_expression, getfnp: pointer, getdata: pointer, env: pointer,
  pExecContext: RDB_exec_context, tx: pointer, val: pointer): cint
  {.cdecl, dynlib: libduro, importc.}

proc RDB_del_expr*(exp: RDB_expression, pExecContext: RDB_exec_context): cint
  {.cdecl, dynlib: libduro, importc, discardable.}

proc RDB_expr_obj*(exp: RDB_expression): ptr RDB_object
  {.cdecl, dynlib: libduro, importc, discardable.}

proc RDB_table_to_array*(arrp: pointer, tbp: pointer, seqitc: cint, seqitv: ptr RDB_seq_item, flags: cint,
  pExecContext: RDB_exec_context, tx: pointer): cint
  {.cdecl, dynlib: libduro, importc.}

proc RDB_array_length*(arrp: pointer, pExecContext: RDB_exec_context): cint
  {.cdecl, dynlib: libduro, importc.}

proc RDB_array_get*(arrp: pointer, idx: cint, pExecContext: RDB_exec_context): pointer
  {.cdecl, dynlib: libduro, importc.}

proc RDB_get_err*(pExecContext: RDB_exec_context): ptr RDB_object
  {.cdecl, dynlib: libduro, importc.}

proc RDB_tuple_set*(pObj: ptr RDB_object, attrname: cstring, value: pointer, pExecContext: RDB_exec_context): cint
  {.cdecl, dynlib: libduro, importc.}

proc RDB_tuple_set_string*(pObj: ptr RDB_object, attrname: cstring, value: cstring, pExecContext: RDB_exec_context): cint
  {.cdecl, dynlib: libduro, importc.}

proc RDB_tuple_set_bool*(pObj: ptr RDB_object, attrname: cstring, value: cchar, pExecContext: RDB_exec_context): cint
  {.cdecl, dynlib: libduro, importc.}

proc RDB_tuple_set_int*(pObj: ptr RDB_object, attrname: cstring, value: cint, pExecContext: RDB_exec_context): cint
  {.cdecl, dynlib: libduro, importc.}

proc RDB_tuple_set_float*(pObj: ptr RDB_object, attrname: cstring, value: cdouble, pExecContext: RDB_exec_context): cint
  {.cdecl, dynlib: libduro, importc.}

proc RDB_insert*(tb: pointer, pObj: ptr RDB_object, pExecContext: RDB_exec_context, tx: pointer): cint
  {.cdecl, dynlib: libduro, importc.}

proc RDB_delete*(tb: pointer, cond: RDB_expression, pExecContext: RDB_exec_context, tx: pointer): cint
  {.cdecl, dynlib: libduro, importc.}

proc RDB_update*(tb: pointer, cond: RDB_expression, updc: cint, updv: ptr RDB_attr_update, pExecContext: RDB_exec_context, tx: pointer): cint
  {.cdecl, dynlib: libduro, importc.}
