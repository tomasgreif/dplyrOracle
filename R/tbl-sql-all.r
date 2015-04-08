#' Build query
#' @export
#' @param x Object for which query is to be build.
#' @param limit Limit to apply
#' @rdname build_query
build_query <- function(x, limit = NULL) {
  assert_that(is.null(limit) || (is.numeric(limit) && length(limit) == 1))
  translate <- function(expr, ...) {
    translate_sql_q(expr, tbl = x, env = NULL, ...)
  }

  if (x$summarise) {
    # Summarising, so SELECT needs to contain grouping variables
    select <- c(x$group_by, x$select)
    select <- select[!duplicated(select)]

    select_sql <- translate(select)
    vars <- dplyr:::auto_names(select)

    group_by_sql <- translate(x$group_by)
    order_by_sql <- translate(x$order_by)
  } else {
    # Not in summarise, so assume functions are window functions
    select_sql <- translate(x$select, window = dplyr:::uses_window_fun(x$select, x))
    vars <- dplyr:::auto_names(x$select)

    # Don't use group_by - grouping affects window functions only
    group_by_sql <- NULL

    # If the user requested ordering, ensuring group_by is included
    # Otherwise don't, because that may make queries substantially slower
    if (!is.null(x$order_by) && !is.null(x$group_by)) {
      order_by_sql <- translate(c(x$group_by, x$order_by))
    } else {
      order_by_sql <- translate(x$order_by)
    }
  }

  if (!dplyr:::uses_window_fun(x$where, x)) {
    from_sql <- x$from
    where_sql <- translate(x$where)
  } else {
    # window functions in WHERE need to be performed in subquery
    where <- dplyr:::translate_window_where(x$where, x, con = x$src$con)
    base_query <- update(x,
      group_by = NULL,
      where = NULL,
      select = c(x$select, where$comp))$query

    from_sql <- build_sql("(", base_query$sql, ") ", ident(dplyr:::unique_name()),
      con = x$src$con)
    where_sql <- translate(where$expr)
  }


  sql <- sql_select(x$src$con, from = from_sql, select = select_sql,
    where = where_sql, order_by = order_by_sql, group_by = group_by_sql,
    limit = limit)
  query(x$src$con, sql, vars)
}

#' Update
#' @export
#' @param object Object to update.
#' @param ... Other functions passed to function.
#' @rdname update
update.tbl_oracle <- function (object, ...) {
  args <- list(...)
  assert_that(dplyr:::only_has_names(args, c("select", "where", "group_by", 
                                     "order_by", "summarise")))
  for (nm in names(args)) {
    object[[nm]] <- args[[nm]]
  }
  if (is.null(object$select)) {
    var_names <- db_query_fields(object$src$con, object$from)
    vars <- lapply(var_names, as.name)
    object$select <- vars
  }
  object$query <- build_query(object)
  object
}