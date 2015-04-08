# dplyrOracle
dplyrOracle is a R package which extends dplyr by providing Oracle backend.
Currently it is experimental and it is probably not a good idea to use
it for any critical tasks.

Please let me know if you would like to contribute to the development. I believe
there are many users who would love to use dplyr with Oracle.

Besides adding Oracle as a dplyr backend, this package provides few other
functions, mainly to save some typing during interactive analysis. These are:

 - `db_remove_tables` - remove multiple tables at once, skipping non-existing
 - `tbls` - create multiple tables in global environment. Use with care as it
 overwrites any existing variables where name is the same as table name.
 - `union all` - union all operator

Read the following to understand what works and what does not. 
`my_db` is an object created by `src_oracle`. Let me know if you know how to 
fix something :)

```
# Copy lahman data
lahman_oracle(my_db)

# Here we'll use the Lahman database: to create your own local copy,
# create a local database called "lahman", or tell lahman_oracle() how to
# a database that you can write to

lahman_p <- my_db
# Methods -------------------------------------------------------------------
batting <- tbl(lahman_p, "Batting")
dim(batting)
colnames(batting)
head(batting)
glimpse(batting)

# Data manipulation verbs ---------------------------------------------------
filter(batting, yearID > 2005, G > 130)
filter(batting, between(yearID, 2005, 2008), G > 130)
filter(batting, between(yearID, 2005, 2008), G > 130)


select(batting, playerID:lgID)
arrange(batting, playerID, desc(yearID))
summarise(batting, G = mean(G), n = n())

summarise(group_by(batting, yearID), 
          G = mean(G), 
          n = n(), 
          s = sd(G), 
          c = cor(G, AB), 
          cov = cov(G, AB),
          med = median(G)
)


mutate(batting, rbi2 = if(is.null(AB)) 1.0 * R / AB else 0)

# note that all operations are lazy: they don't do anything until you
# request the data, either by `print()`ing it (which shows the first ten
# rows), by looking at the `head()`, or `collect()` the results locally.

system.time(recent <- filter(batting, yearID > 2010))
system.time(collect(recent))

# Group by operations -------------------------------------------------------
# To perform operations by group, create a grouped object with group_by
players <- group_by(batting, playerID)
group_size(players)

x <- filter(players, AB == max(AB))
x %>% show_query()


summarise(players, mean_g = mean(G), best_ab = max(AB))
best_year <- filter(players, AB == max(AB) | G == max(G))
best_year %>% show_query()
best_year


progress <- mutate(players,
                   cyear = yearID - min(yearID) + 1,
                   ab_rank = rank(desc(AB)),
                   cumulative_ab = order_by(yearID, cumsum(AB)))

# When you group by multiple level, each summarise peels off one level
per_year <- group_by(batting, playerID, yearID)
stints <- summarise(per_year, stints = max(stint))
filter(stints, stints > 3)
summarise(stints, max(stints))
out <- mutate(stints, x = order_by(yearID, cumsum(stints)))
out

# Joins ---------------------------------------------------------------------
player_info <- select(tbl(lahman_p, "Master"), playerID, birthYear)
hof <- select(filter(tbl(lahman_p, "HallOfFame"), inducted == "Y"),
              playerID, votedBy, category)

# Match players and their hall of fame data
inner_join(player_info, hof)
# Keep all players, match hof data where available
left_join(player_info, hof)
# Find only players in hof
semi_join(player_info, hof)
# Find players not in hof
anti_join(player_info, hof)

# Arbitrary SQL -------------------------------------------------------------
# You can also provide sql as is, using the sql function:
batting2008 <- tbl(lahman_p,
                   sql('SELECT * FROM "Batting" WHERE "yearID" = 2008'))
batting2008


##################### COLLECT, COLLAPSE
remote <- select(filter(batting, yearID > 2010 && stint == 1), playerID:H)
remote2 <- collapse(remote)
cached <- compute(remote)
local  <- collect(remote)

##################### DBI

#db_list_tables(con)
db_list_tables(my_db$con)

#db_create_table(con, table, types, temporary = FALSE, ...)
db_create_table(my_db$con, 'DPLYR_TEST', c(a = 'number',  b = 'varchar(20)'))

#db_has_table(con, table)
db_has_table(my_db$con, 'DPLYR_TEST')

#db_data_type(con, fields)
db_data_type(my_db$con, iris)

#db_save_query(con, sql, name, temporary = TRUE, ...)
db_save_query(my_db$con, sql('select "a", "b", "a"+2 as c from DPLYR_TEST'), temporary = FALSE,
              name = dplyr:::random_table_name())

#db_begin(con, ...)
db_begin(my_db$con)

#db_commit(con, ...)
db_commit(my_db$con)

#db_rollback(con, ...)
db_rollback(my_db$con)

#db_insert_into(con, table, values, ...)
df <- data.frame(a = runif(5), b = LETTERS[1:5])
db_insert_into(my_db$con, 'DPLYR_TEST', df)
tbl(my_db, 'DPLYR_TEST')

#db_create_index(con, table, columns, name = NULL, ...)
db_create_index(my_db$con, 'DPLYR_TEST', 'a')

#db_analyze(con, table, ...)
# tbd....

#db_explain(con, sql, ...)
# works, but does not return explain plan (it is stored in plan_table)
# http://docs.oracle.com/cd/B10500_01/server.920/a96533/ex_plan.htm
db_explain(my_db$con, sql('select * from DPLYR_TEST'))

#db_query_fields(con, sql, ...)
db_query_fields(my_db$con, sql("DPLYR_TEST"))

#db_query_rows(con, sql, ...)
db_query_rows(my_db$con, sql('select * from DPLYR_TEST'))

#db_drop_table(con, table, force = FALSE, ...)
db_drop_table(my_db$con, 'DPLYR_TEST')
db_has_table(my_db$con, 'DPLYR_TEST')