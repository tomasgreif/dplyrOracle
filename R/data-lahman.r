#' Cache and retrieve an \code{src_oracle} of the Lahman baseball database.
#'
#' This creates an interesting database using data from the Lahman baseball
#' data source, provided by Sean Lahman at
#' \url{http://www.seanlahman.com/baseball-archive/statistics/}, and
#' made easily available in R through the \pkg{Lahman} package by
#' Michael Friendly, Dennis Murphy and Martin Monkman. See the documentation
#' for that package for documentation of the inidividual tables.
#'
#' @param src Source to use.
#' @param ... Other arguments passed to function
#' @export
#' @rdname lahman
lahman_oracle <- function(src, ...) {
  copy_lahman(src, ...)
}
