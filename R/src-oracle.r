#' Connect to oracle.
#' @param dbname Oracle database name
#' @param host Host name
#' @param port Port
#' @param user User name
#' @param password Password
#' @param ... Other arguments passed to function.
#' @details Note that you can also use SID based connection if supported by
#' your infrastructure
#' @examples
#' \dontrun{
#'host <- "11.111.11.11"
#'port <- 1521
#'sid <- "mydbident"
#'
#'connect.string <- paste(
#'  "(DESCRIPTION=",
#'  "(ADDRESS=(PROTOCOL=tcp)(HOST=", host, ")(PORT=", port, "))",
#'  "(CONNECT_DATA=(SID=", sid, ")))", sep = "")
#'
#'my_db <- src_oracle(username = "ilikedb", password = "verysecret", 
#'                    dbname = connect.string)
#' }
#' @export
src_oracle <- function(dbname = NULL, host = NULL, port = NULL, user = NULL,
                         password = NULL, ...) {
  if (!requireNamespace("ROracle", quietly = TRUE)) {
    stop("ROracle package required to connect to oracle db", call. = FALSE)
  }

  user <- user %||% if (in_travis()) "oracle" else ""

  con <- dbConnect(ROracle::Oracle(), host = host %||% "", dbname = dbname %||% "",
    user = user, password = password %||% "", port = port %||% "", ...)
  info <- dbGetInfo(con)

  src_sql("oracle", con,
    info = info, disco = db_disconnector(con, "oracle"))
}

#' @export
tbl.src_oracle <- function(src, from, ...) {
  tbl_sql("oracle", src = src, from = from, ...)
}

#' @export
src_desc.src_oracle <- function(x) {
  info <- x$info
  
  if(!is.null(info$dbname) && grepl('PROTOCOL', info$dbname)) {
    split <- unlist(strsplit(tolower(info$dbname), split = '(', fixed = TRUE))
    split <- gsub('[()]', replacement = '', x = split)
    split <- strsplit(split, split = '=')
    
    splitLength <- sapply(split, length)
    
    result <- sapply(split[splitLength == 2], function(x) x[2])
    names(result) <- sapply(split[splitLength == 2], function(x) x[1])
    
    sid <- result['sid']
    port <- ifelse(is.null(info$port), result['port'], info$port)
    host <- ifelse(is.null(info$host), result['host'], info$host)
    
    paste0("Oracle ", info$serverVersion, " [", info$username, "@",
           host, ":", port, "/", sid, "]")    
  } else {
  
  host <- if (is.null(info$dbname) == "") "NA" else info$dbname

  paste0("Oracle ", info$serverVersion, " [", info$username, "@",
    host, ":", info$port, "/", info$dbname, "]")
  }
}

#' @export
sql_translate_env.src_oracle <- function(x) {
  sql_variant(
    base_scalar,
    sql_translator(.parent = base_agg,
      n = function() sql("count(*)"),
      cor = sql_prefix("corr"),
      cov = sql_prefix("covar_samp"),
      sd =  sql_prefix("stddev_samp"),
      var = sql_prefix("var_samp")
    ),
    base_win
  )
}

#' @export
db_save_query.OraConnection <- function (con, sql, name, temporary = TRUE, ...) {
  
  tt_sql <- build_sql("CREATE ", if (temporary) sql("GLOBAL TEMPORARY "), 
                      "TABLE ", ident(name), 
                      if(temporary) sql(" ON COMMIT PRESERVE ROWS "), " AS ", sql, 
    con = con)

  dbGetQuery(con, tt_sql)
  name
}

#' Copy to oracle
#' @param dest Destination
#' @param df Data frame to copy
#' @param name Name in database
#' @param types Types
#' @param temporary Should table be temporary?
#' @param indexes Table indexes
#' @param analyze Should table statistics be recalculated after copy?
#' @param ... Other arguments passed to function.
#' @rdname copy_to
#' @export
copy_to.src_oracle <- function (dest, df, name = deparse(substitute(df)), types = NULL, 
                                   temporary = FALSE, indexes = NULL, analyze = TRUE, ...) 
{
  assert_that(is.data.frame(df), is.string(name), is.flag(temporary))
  class(df) <- "data.frame"
  if (isTRUE(db_has_table(dest$con, name))) {
    stop("Table ", name, " already exists.", call. = FALSE)
  }
  types <- types %||% db_data_type(dest$con, df)
  names(types) <- names(df)
  con <- dest$con
  db_begin(con)
  on.exit(db_rollback(con))
  ROracle::dbWriteTable(con, name, df)
  #db_create_table(con, name, types, temporary = temporary)
  #db_insert_into(con, name, df)
  dplyr:::db_create_indexes(con, name, indexes)
  if (analyze) 
    db_analyze(con, name)
  db_commit(con)
  on.exit(NULL)
  tbl(dest, name)
}

# DBI methods ------------------------------------------------------------------

# Doesn't return TRUE for temporary tables
#' @export
db_has_table.OraConnection <- function(con, table, ...) {
  table %in% db_list_tables(con)
}

#' @export
db_begin.OraConnection <- function(con, ...) {
  dbGetQuery(con, "SET TRANSACTION NAME 'plyr_transaction'")
}


#' @export
db_explain.OraConnection <- function(con, sql, format = "text", ...) {
  #format <- match.arg(format, c("text", "json", "yaml", "xml"))

  exsql <- build_sql("EXPLAIN PLAN FOR ", sql)
    #if (!is.null(format)) build_sql("(FORMAT ", sql(format), ") "), sql)
  print(exsql)
  expl <- dbGetQuery(con, exsql)

  paste(expl[[1]], collapse = "\n")
}

#' @export
db_insert_into.OraConnection <- function(con, table, values, ...) {
  ROracle::dbWriteTable(conn = con, name = table, value = values, append = TRUE)
}

#' @export
db_query_fields.OraConnection <- function(con, sql, ...) {
  fields <- build_sql("SELECT * FROM ", sql, " WHERE 0=1", con = con)

  qry <- dbSendQuery(con, fields)
  on.exit(dbClearResult(qry))
  
  dbGetInfo(qry)$fields$name
}


#' @export
db_create_table.OraConnection <- function(con, table, types,
                                          temporary = FALSE, ...) {
  assert_that(is.string(table), is.character(types))
  field_names <- escape(ident(names(types)), collapse = NULL, con = con)

  fields <- dplyr:::sql_vector(paste0(field_names, " ", types), parens = TRUE,
                       collapse = ", ", con = con)
  sql <- build_sql("CREATE ", if (temporary) sql("GLOBAL TEMPORARY "),
                   "TABLE ", ident(table), " ", fields, con = con)
  print(sql)
  dbGetQuery(con, sql)
}

#' @export
db_data_type.OraConnection <- function(con, fields, ...) {
  raw <- vapply(fields, dbDataType, dbObj = con, FUN.VALUE = character(1))
  ifelse(raw == 'double', 'number', raw)
}

#' @export
db_analyze.OraConnection <- function(con, table, ...) {
  return(TRUE)
}

#' @export
sql_subquery.OraConnection <- function(con, sql, name = dplyr:::unique_name(), ...) {
  if (is.ident(sql)) return(sql)
  build_sql("(", sql, ") ", ident(name), con = con)
}

"%||%" <- function(x, y) if(is.null(x)) y else x

in_travis <- function() identical(Sys.getenv("TRAVIS"), "true")


# Creates an environment that disconnects the database when it's
# garbage collected
db_disconnector <- function(con, name, quiet = FALSE) {
  reg.finalizer(environment(), function(...) {
    if (!quiet) {
      message("Auto-disconnecting ", name, " connection ",
        "(", paste(con@Id, collapse = ", "), ")")
    }
    dbDisconnect(con)
  })
  environment()
}

