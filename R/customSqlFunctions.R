#' Drop multiple tables in Oracle database
#' @param con Connection
#' @param tables Character vector with table names to drop.
#' @param force Force drop?
#' @param ... Other arguments passed to function
#' @rdname db_drop_tables
#' @export
db_drop_tables <- function(con, tables, force = FALSE, ...) {
  sapply(tables, function(table) {
    if (db_has_table(con, table)) {
      db_drop_table(con, table, force, ...)    
    } else {
      message('Table ', table, ' does not exist, skipping.')
    }
  })
  
  TRUE
}

#' Create tables in global environment
#' @param src Source
#' @param tables Character vector with table names.
#' @param envir Environment where tables are created.
#' @param tolower Should variables pointing to tables be lowercased?
#' @param ... Other arguments passed to function
#' @export
tbls <- function(src, ..., tables, envir = globalenv(), tolower = TRUE) {
  lapply(tables, function(table) {
    table_name <- if (tolower) tolower(table) else table
    assign(table_name, tbl(src, ..., table), envir = envir)
  })
  TRUE
}

#' Union all operator
#' @param x Table \code{x} to union.
#' @param y Table \code{y} to union.
#' @param ... Other arguments passed to function
#' @export
union_all <- function (x, y, ...) { 
  UseMethod("union_all")
}

#' Union all operator for Oracle
#' @param x Table \code{x} to union.
#' @param y Table \code{y} to union.
#' @param copy Should data be copied to source if not present?
#' @param ... Other arguments passed to function
#' @export
union_all.tbl_oracle <- function (x, y, copy = FALSE, ...) {
  y <- dplyr:::auto_copy(x, y, copy)
  sql <- sql_set_op(x$src$con, x, y, "UNION ALL")
  update(tbl(x$src, sql), group_by = groups(x))
}
