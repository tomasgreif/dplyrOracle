#' Set of SQL Translator functtions
#' @keywords internal
#' NULL

#' SQL Translator
#' @export
#' @rdname sql_translator
sql_translator <- function (..., .funs = list(), .parent = new.env(parent = emptyenv())) 
{
  funs <- c(list(...), .funs)
  if (length(funs) == 0) 
    return(.parent)
  list2env(funs, copy_env(.parent))
}

#' Copy Environment
#' @export
#' @param from Envrironment to copy from
#' @param to Environment to copy to
#' @param parent Paren environment of copied elements
#' @rdname copy_env 
copy_env <- function (from, to = NULL, parent = parent.env(from)) 
{
  list2env(as.list(from), envir = to, parent = parent)
}


#' Win recycled Oracle
#' I had to create a copy of win_cumulative in order to use my own
#' sql_vector function
#' @param f Function to create
win_recycled <- function(f) {
  force(f)
  function(x) {
    over(build_sql(sql(f), list(x)), dplyr:::partition_group(), NULL, frame = NULL)
  }
}

#' Win cumulative Oracle
#' I had to create a copy of win_cumulative in order to use my own
#' sql_vector function
#' @param f Function to create
win_cumulative <- function (f) {
  force(f)
  function(x) {
    over(build_sql(sql(f), list(x)), dplyr:::partition_group(), dplyr:::partition_order(), 
         frame = c(-Inf, 0))
  }
}

#' Win Rank
#' I had to create a copy of win_cumulative in order to use my own
#' sql_vector function
#' @param f Function to create
win_rank <- function (f) 
{
  force(f)
  function(order = NULL) {
    over(build_sql(sql(f), list()), dplyr:::partition_group(), order %||% 
           dplyr:::partition_order())
  }
}

#' win single arg
#' @param f Function to create
win_single_arg <- function (f) {
  force(f)
  function(order_by, n) {
    over(
      build_sql(sql(f), list(n)),
      dplyr:::partition_group(),
      order_by %||% dplyr:::partition_order()
    )
  }
}


#' SQL Variant
#' @export
#' @rdname sql_variant
#' @format NULL
base_win <- sql_translator(
  # rank functions have a single order argument that overrides the default
  row_number   = win_rank("row_number"),
  min_rank     = win_rank("rank"),
  rank         = win_rank("rank"),
  dense_rank   = win_rank("dense_rank"),
  percent_rank = win_rank("percent_rank"),
  cume_dist    = win_rank("cume_dist"),
  ntile        = win_single_arg('ntile'),
  #pcont        = win_single_arg('percentile_cont'),
  
  
  # Recycled aggregate fuctions take single argument, don't need order and
  # include entire partition in frame.
  mean  = win_recycled("avg"),
  sum   = win_recycled("sum"),
  min   = win_recycled("min"),
  max   = win_recycled("max"),
  n     = function() {
    over(sql("COUNT(*)"), dplyr:::partition_group(), frame = NULL)
  },
  
  # Cumulative function are like recycled aggregates except that R names
  # have cum prefix, order_by is inherited and frame goes from -Inf to 0.
  cummean = win_cumulative("avg"),
  cumsum  = win_cumulative("sum"),
  cummin  = win_cumulative("min"),
  cummax  = win_cumulative("max"),
  
  # Finally there are a few miscellaenous functions that don't follow any
  # particular pattern
  median = function(x) {
    over(build_sql("MEDIAN", list(x)), dplyr:::partition_group())
  },
  nth = function(x, order = NULL) {
    over(build_sql("NTH_VALUE", list(x)), dplyr:::partition_group(), order %||% dplyr:::partition$order())
  },
  first = function(x, order = NULL) {
    over(build_sql("FIRST_VALUE", list(x)), dplyr:::partition_group(), order %||% dplyr:::partition_order())
  },
  last = function(x, order = NULL) {
    over(build_sql("LAST_VALUE", list(x)), dplyr:::partition_group(), order %||% dplyr:::partition_order())
  },
  
  lead = function(x, n = 1L, default = NA, order = NULL) {
    over(
      build_sql("LEAD", list(x, n, default)),
      dplyr:::partition_group(),
      order %||% dplyr:::partition_order()
    )
  },
  lag = function(x, n = 1L, default = NA, order = NULL) {
    over(
      build_sql("LAG", list(x, n, default)),
      dplyr:::partition_group(),
      order %||% dplyr:::partition_order()
    )
  },
  
  order_by = function(order_by, expr) {
    old <- dplyr:::set_partition(dplyr:::partition_group(), order_by)
    on.exit(dplyr:::set_partition(old))
    
    expr
  }
)

