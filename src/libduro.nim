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

proc RDB_init_exec_context*(pExecContext: pointer)
   {.cdecl, dynlib: libduro, importc.}

proc RDB_destroy_exec_context*(pExecContext: pointer)
   {.cdecl, dynlib: libduro, importc.}

proc RDB_open_env*(path: cstring, flags: cint, penv: pointer): pointer
   {.cdecl, dynlib: libduro, importc.}

proc RDB_close_env*(env: pointer, execContext: pointer): cint
   {.cdecl, dynlib: libduro, importc.}

proc RDB_get_db_from_env*(name: cstring, env: pointer, ecp: pointer, tx: pointer): pointer
   {.cdecl, dynlib: libduro, importc.}

proc RDB_begin_tx*(ecp: pointer, tx: pointer, db: pointer, parent: pointer): cint
   {.cdecl, dynlib: libduro, importc.}

proc RDB_commit*(ecp: pointer, tx:pointer): cint
   {.cdecl, dynlib: libduro, importc.}

proc RDB_rollback*(ecp: pointer, tx:pointer): cint
   {.cdecl, dynlib: libduro, importc.}

proc RDB_get_table*(name: cstring, ecp: pointer, tx: pointer): pointer
   {.cdecl, dynlib: libduro, importc.}

proc RDB_extract_tuple*(tbp: pointer, ecp: pointer, tx: pointer, tplp: pointer): cint
   {.cdecl, dynlib: libduro, importc.}

proc RDB_init_obj*(objp: pointer)
   {.cdecl, dynlib: libduro, importc.}

proc RDB_destroy_obj*(objp: pointer, ecp: pointer): cint
   {.cdecl, dynlib: libduro, importc, discardable.}

proc RDB_tuple_get*(objp: pointer, name: cstring): pointer
   {.cdecl, dynlib: libduro, importc.}

proc RDB_obj_bool*(objp: pointer): cchar
   {.cdecl, dynlib: libduro, importc.}

proc RDB_obj_string*(objp: pointer): cstring
   {.cdecl, dynlib: libduro, importc.}

proc RDB_obj_int*(objp: pointer): cint
  {.cdecl, dynlib: libduro, importc.}

proc RDB_obj_float*(objp: pointer): cdouble
  {.cdecl, dynlib: libduro, importc.}

proc RDB_obj_type*(objp: pointer): pointer
  {.cdecl, dynlib: libduro, importc.}

proc RDB_binary_length*(objp: pointer): csize
  {.cdecl, dynlib: libduro, importc.}

proc RDB_binary_get*(objp: pointer, pos: csize, len: csize, ecp: pointer, pp: ptr ptr byte, alenp: ptr csize): cint
  {.cdecl, dynlib: libduro, importc.}

proc RDB_binary_set*(objp: pointer, pos: csize, scrp: pointer, len: csize, ecp: pointer): cint
  {.cdecl, dynlib: libduro, importc.}

proc RDB_type_name*(objp: pointer): cstring
  {.cdecl, dynlib: libduro, importc.}

proc RDB_var_ref*(attrname: cstring, ecp: pointer): RDB_expression
  {.cdecl, dynlib: libduro, importc.}

proc RDB_bool_to_expr*(b: cchar, ecp: pointer): RDB_expression
  {.cdecl, dynlib: libduro, importc.}

proc RDB_string_to_expr*(str: cstring, ecp: pointer):RDB_expression
  {.cdecl, dynlib: libduro, importc.}

proc RDB_int_to_expr*(val: cint, ecp: pointer): RDB_expression
  {.cdecl, dynlib: libduro, importc.}

proc RDB_float_to_expr*(val: cdouble, ecp: pointer): RDB_expression
  {.cdecl, dynlib: libduro, importc.}

proc RDB_obj_to_expr*(obj: ptr RDB_object, ecp: pointer): RDB_expression
  {.cdecl, dynlib: libduro, importc.}

proc RDB_ro_op*(opname: cstring, ecp: pointer): RDB_expression
  {.cdecl, dynlib: libduro, importc.}

proc RDB_add_arg*(dexp: RDB_expression, arg: RDB_expression)
  {.cdecl, dynlib: libduro, importc.}

proc RDB_evaluate*(exp: RDB_expression, getfnp: pointer, getdata: pointer, env: pointer,
  ecp: pointer, tx: pointer, val: pointer): cint
  {.cdecl, dynlib: libduro, importc.}

proc RDB_del_expr*(exp: RDB_expression, ecp: pointer): cint
  {.cdecl, dynlib: libduro, importc, discardable.}

proc RDB_expr_obj*(exp: RDB_expression): ptr RDB_object
  {.cdecl, dynlib: libduro, importc, discardable.}

proc RDB_table_to_array*(arrp: pointer, tbp: pointer, seqitc: cint, seqitv: ptr RDB_seq_item, flags: cint,
  ecp: pointer, tx: pointer): cint
  {.cdecl, dynlib: libduro, importc.}

proc RDB_array_length*(arrp: pointer, ecp: pointer): cint
  {.cdecl, dynlib: libduro, importc.}

proc RDB_array_get*(arrp: pointer, idx: cint, ecp: pointer): pointer
  {.cdecl, dynlib: libduro, importc.}

proc RDB_get_err*(ecp: pointer): pointer
  {.cdecl, dynlib: libduro, importc.}

proc RDB_tuple_set*(obj: pointer, attrname: cstring, value: pointer, ecp: pointer): cint
  {.cdecl, dynlib: libduro, importc.}

proc RDB_tuple_set_string*(obj: pointer, attrname: cstring, value: cstring, ecp: pointer): cint
  {.cdecl, dynlib: libduro, importc.}

proc RDB_tuple_set_bool*(obj: pointer, attrname: cstring, value: cchar, ecp: pointer): cint
  {.cdecl, dynlib: libduro, importc.}

proc RDB_tuple_set_int*(obj: pointer, attrname: cstring, value: cint, ecp: pointer): cint
  {.cdecl, dynlib: libduro, importc.}

proc RDB_tuple_set_float*(obj: pointer, attrname: cstring, value: cdouble, ecp: pointer): cint
  {.cdecl, dynlib: libduro, importc.}

proc RDB_insert*(tb: pointer, obj: pointer, ecp: pointer, tx: pointer): cint
  {.cdecl, dynlib: libduro, importc.}

proc RDB_delete*(tb: pointer, cond: RDB_expression, ecp: pointer, tx: pointer): cint
  {.cdecl, dynlib: libduro, importc.}

proc RDB_update*(tb: pointer, cond: RDB_expression, updc: cint, updv: ptr RDB_attr_update, ecp: pointer, tx: pointer): cint
  {.cdecl, dynlib: libduro, importc.}
