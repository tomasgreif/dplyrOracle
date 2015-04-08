
# Custom method for HEAD
#' @export
head.tbl_oracle <- function(x, n = 6L, ...) {
  assert_that(length(n) == 1, n > 0L)
  query <- build_query(x)
  query$sql <- build_sql('select * from  (', query$sql, ') A where ROWNUM <= ', n)
  query$fetch()
}
