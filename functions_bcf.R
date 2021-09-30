# ---- Environment ----

library(magrittr)

# Get elements by exact name only
`$` <- function(x, name) {
  name <- deparse(substitute(name))
  x[[name, exact = TRUE]]
}

# ---- Helpers ----

#' Pipe-friendly conditional dplyr::mutate
#' 
#' Assigns values to variables in a data.frame only for rows matching a condition.
#' 
#' @param .data Data table (passed to dplyr::filter)
#' @param condition Logical condition (passed to dplyr::filter)
#' @param ... Variable assignments (passed to dplyr::mutate)
#' @param .default Default value for new variables
#' @param envir Environment in which `condition` is evaluated
#' @return Object of the same class as `.data`
mutate_when <- function(.data, condition, ..., .default = NA) {
  new_vars <- substitute(list(...)) %>%
    extract(-1) %>%
    sapply(deparse) %>%
    names() %>%
    setdiff(names(.data))
  .data[, new_vars] <- .default
  condition <- rlang::eval_tidy(rlang::enquo(condition), .data) %>%
    replace(is.na(.), FALSE)
  .data[condition, ] <- .data %>%
    dplyr::filter(condition) %>%
    dplyr::mutate(...)
  .data
}

#' Parse hex to string
#'
#' @param x (character) Hexadecimal strings
#' @examples
#' parse_hex(c("4f7374617261", "7069636b6c6562726963"))
parse_hex <- function(x) {
  sapply(x, function(s) {
    seq(1, nchar(s), by = 2) %>%
      sapply(function(x) substr(s, x, x + 1)) %>%
      strtoi(base = 16L) %>%
      as.raw() %>%
      rawToChar()
  }, USE.NAMES = FALSE)
}

#' Format object for SQL query
#'
#' Strings are wrapped in single quotes (\code{"'x'"}) and \code{NA}, \code{NULL} converted to \code{"NULL"}.
#' @param x Object to format
#' @examples
#' format_sql(1)
#' format_sql("foodclub")
#' format_sql(NA)
#' format_sql(NULL)
format_sql <- function(x) {
  if (is.character(x)) {
    paste0("'", x, "'")
  } else if (is.null(x) || is.na(x)) {
    "NULL"
  } else {
    x
  }
}

# ---- Foodclub (website) ----

#' Login to Foodclub
#'
#' @param username (character) Username
#' @param password (character) Password
#' @return Authorization token
#' @examples
#' \dontrun{
#' foodclub_login("username", "password")
#' }
foodclub_login <- function(username, password) {
  url <- "https://foodclub.org/bouldercoopfood/login"
  response <- httr::POST(url, body = list(user_id = username, password = password, login = "Login"))
  index_url <- "https://foodclub.org/bouldercoopfood/index"
  if (httr::GET(index_url)$url == index_url) {
    return(httr::handle_find(url))
  } else {
    stop("Login failed")
  }
}

# ---- Foodclub (phpMyAdmin) ----

#' Login to Foodclub phpMyAdmin
#'
#' @param username (character) Username
#' @param password (character) Password
#' @return Authorization token
#' @examples
#' \dontrun{
#' pma_login("username", "password")
#' }
pma_login <- function(username, password) {
  url <- "https://foodclub.org/phpmyadmin/index.php"
  body <- list(pma_username = username, pma_password = password)
  response <- httr::POST(url, body = body)
  token <- response$url %>%
    httr::parse_url() %$% query %$% token
  if (response$status_code != 200 || is.null(token)) {
    stop("Login failed")
  }
  token
}

#' Run SQL query via Foodclub phpMyAdmin
#'
#' @param sql (character) SQL query
#' @param table (character) Database table (default: \code{NULL})
#' @param db (character) Database (default: \code{"foodclub"})
#' @param token (character) Authentication token (see \code{\link{pma_login}})
#' @param result (boolean) Whether to return the query result (\code{TRUE}) or a boolean success flag (\code{FALSE})
#' @return Query result as a \code{\code{data.frame}} or boolean success flag (\code{result = FALSE})
#' @examples
#' \dontrun{
#' token <- pma_login("username", "password")
#' pma_query("select * from private_bcf_goldenorganics", token = token, result = TRUE)
#' }
pma_query <- function(sql, table = NULL, db = "foodclub", token, result = FALSE) {
  url <- "https://foodclub.org/phpmyadmin/import.php"
  query <- list(
    token = token,
    table = table,
    db = db,
    pos = 0,
    goto = "tbl_sql.php",
    sql_query = sql,
    show_query = 0
  )
  xml <- httr::POST(url, query = query) %>%
    httr::content()
  success <- xml %>%
    xml2::xml_find_first("//div[@class = 'success']")
  if (length(success) > 0) {
    if (result) {
      xml %>%
        xml2::xml_find_first(xpath = "//table[@id = 'table_results']") %>%
        rvest::html_table(header = TRUE, fill = TRUE, trim = TRUE) %>%
        .[, names(.) != "" & !is.na(names(.)), drop = FALSE] %>%
        # Drop intermediate header rows
        {.[apply(., 1, FUN = function(x) all(x != names(.))), , drop = FALSE]} %>%
        dplyr::as_tibble() %>%
        # Convert all to character
        dplyr::mutate_all(.funs = as.character) %>%
        # Replace "NULL" with NA
        dplyr::mutate_all(.funs = function(x) {gsub("^NULL$", NA, x)}) %>%
        # Replace "0000-00-00 00:00:00" with NA
        dplyr::mutate_all(.funs = function(x) {gsub("^0000-00-00 00:00:00$", NA, x)})
    } else {
      TRUE
    }
  } else {
    FALSE
  }
}

#' Get table via Foodclub phpMyAdmin
#'
#' May be slow for very large tables since the function works by scraping HTML.
#'
#' @param table (character) Database table
#' @param db (character) Database (default: \code{"foodclub"})
#' @param token (character) Authentication token (see \code{\link{pma_login}})
#' @param hex (character) Field names to parse from hex (see \code{\link{parse_hex}})
#' @param block (integer) Number of rows to process at a time
#' @return Database table as a \code{\link{data.frame}}
#' @examples
#' \dontrun{
#' token <- pma_login("username", "password")
#' pma_get_table("private_bcf_goldenorganics", token = token)
#' }
pma_get_table <- function(table, db = "foodclub", token, hex = c("user_id"), block = 1000) {
  # Count table rows
  sql <- paste0("SELECT COUNT(*) as rows FROM ", db, ".", table, ";")
  result <- pma_query(sql = sql, db = db, token = token, result = TRUE)
  if (isFALSE(result)) {
    "Failed to retrieve table" %>%
      paste(table) %>%
      stop()
  } else {
    rows <- result$rows %>%
      as.numeric()
  }
  # Read table in blocks
  starts <- seq(0, rows - 1, block)
  success <- TRUE
  dfs <- starts %>%
    lapply(function(i) {
      sql <- paste0("SELECT * FROM ", db, ".", table, " LIMIT ", i, ", ", block, ";")
      df <- pma_query(sql = sql, db = db, token = token, result = TRUE)
      if (isFALSE(df)) {
        success <- FALSE
        break
      } else {
        df %>%
          extract(.[[2]] != "", !grepl("^\\.\\.", names(.)))
      }
    })
  if (success) {
    dfs %>%
      dplyr::bind_rows() %>%
      dplyr::mutate_all(.funs = readr::parse_guess) %>%
      dplyr::mutate_at(.funs = parse_hex, .vars = intersect(hex, names(.)))
  } else {
    FALSE
  }
}

#' Get (cached) table via Foodclub phpMyAdmin
#' 
#' @param table (character) Database table
#' @param path (character) Path to file from which to read or write result
#' @param overwrite (logical) Whether to overwrite `path` if already exists
#' @param token (character) Authorization token
#' @param hex (character) Field names to parse from hex (see \code{\link{parse_hex}})
#' @param block (integer) Number of rows to process at a time
#' @return Database table as a \code{\link{data.frame}}
pma_get_table_cache <- function(
  table, path, overwrite = FALSE, token, hex = c("user_id"), block = 1000) {
  dir.create(dirname(path), showWarnings = FALSE)
  if (overwrite || !file.exists(path)) {
    pma_get_table(table, token = token, hex = hex, block = block) %T>%
      saveRDS(file = path)
  } else {
    readRDS(path)
  }
}

#' Import CSV via Foodclub phpMyAdmin
#'
#' Expects a CSV file with column names in the first row which all correspond to Foodclub database schema fields.
#'
#' @param path (character) Path to CSV file
#' @param table (character) Database table
#' @param db (character) Database (default: \code{"foodclub"}).
#' @param csv_replace (boolean) Whether to delete existing table rows before import (default: \code{TRUE})
#' @param csv_columns (character) Table fields corresponding to each column of the csv file (default: \code{names(read.csv(import_file)})
#' @param skip_queries (numeric) Number of lines to skip when reading \code{file} (default: \code{1})
#' @param token (character) Authentication token (see \code{\link{pma_login}})
#' @examples
#' \dontrun{
#' token <- pma_login("username", "password")
#' pma_import_csv(file.csv, "table", token = token)
#' }
pma_import_csv <- function(path, table, db = "foodclub", csv_replace = TRUE, csv_columns = names(read.csv(path)), skip_queries = 1, token) {
  csv_columns <- paste(csv_columns, collapse = ",")
  url <- "https://foodclub.org/phpmyadmin/import.php"
  body = c(
    mget(c("token", "table", "db", "csv_replace", "csv_columns", "skip_queries")),
    list(
      import_type = "table", format = "csv", charset_of_file = "utf-8",
      csv_terminated = ",", csv_enclosed = "\"", csv_escaped = "\"", csv_new_line = "auto",
      import_file = httr::upload_file(path))
  )
  response <- httr::POST(url, body = body)
  # Print query result
  httr::content(response) %>%
    xml2::xml_find_first("//div[@id='result_query']") %>%
    xml2::xml_text() %>%
    cat()
}

#' Write object to CSV for import via Foodclub phpMyAdmin
#'
#' As expected by phpMyAdmin, \code{NA} is written as NULL (without quotes) and row names are left out.
#'
#' @param x (coercible to data.frame) Object to write
#' @param path (character) Path to write to (default: \code{""}, output to console)
#' @examples
#' x <- data.frame(code = c("a", "b"), price = c(10, NA))
#' pma_write_import_csv(x)
pma_write_import_csv <- function(x, path = "") {
  write.csv(x, file = path, na = "NULL", row.names = FALSE)
}

# ---- Finance ----

#' Clean Foodclub orders
#' 
#' Corrects errors in table `custom_view_dw_archived_invoice_user_totals_bouldercoopfood` so that the data follows the following format:
#' 
#' - `pretax` (Pre-tax total): `pretax` + `refunds`
#' - `tax`: `pretax` * tax rate
#' - `invoice`: `pretax` + `tax`
#' - `refunds`: 0
#' - `order_subtotal`: `invoice`
#' - `member_fees`: `order_subtotal` * markup
#' - `custom_fees`: 0
#' - `overall_order`: `member_fees` + `order_subtotal`
#' 
#' @param orders (data.frame) Table
#' `custom_view_dw_archived_invoice_user_totals_bouldercoopfood`
#' @return Cleaned `orders`
clean_foodclub_orders <- function(orders) {
  
  # ---- Remove empty orders ----
  
  is_empty <- orders %>%
    dplyr::select_if(is.numeric) %>%
    rowSums() %>%
    equals(0)
  orders %<>%
    dplyr::filter(!is_empty)
  
  # ---- Round to nearest cent ----
  
  orders %<>%
    dplyr::mutate_if(is.numeric, round, digits = 2)
  
  # ---- Reallocate Costco true-ups ----
  
  # Early Costco orders lacked receipts, so custom fees were applied to match the charges on the debit card. Reallocate these custom fees to the price and tax paid by members.
  mask <- orders %>%
    with(
      order_date >= as.Date("2017-02-08") &
        order_date <= as.Date("2017-04-29") &
        account_id %in% c("bcf_costco", "bcf_costco_nf") &
        custom_fees != 0
    )
  modified <- orders[mask, ] %>%
    dplyr::mutate(
      pretax = (pretax + custom_fees * (1 - tax / pretax)) %>%
        round(2),
      # Set tax so that total remains unchanged
      tax = (overall_order - pretax) %>%
        round(2),
      invoice = pretax + tax,
      order_subtotal = invoice,
      custom_fees = 0,
      overall_order = order_subtotal
    )
  err <- (orders$overall_order[mask] - modified$overall_order) %>%
    round(2)
  if (any(err != 0)) {
    stop("Change in order total")
  }
  orders[mask, ] <- modified
  
  # ---- Fix isolated invoice rounding error (2017-02-20) ----
  
  orders %<>%
    mutate_when(
      account_id == "bcf_costco" & order_date == as.Date("2017-02-20") &
        user_id %in% c("Masala", "picklebric"),
      tax = invoice - pretax
    )
  
  # ---- Move custom fees to member fees ----
  
  # The first several months of orders used `custom_fees` for markups, rather than the standard `member_fees`. Reassign these custom fees as member fees.
  orders %<>%
    mutate_when(
      order_date >= as.Date("2017-02-08") & order_date <= as.Date("2017-06-15"),
      member_fees = custom_fees,
      custom_fees = 0
    )
  
  # ---- Reassign misassigned orders ----
  
  # Foodclub accounts do not always map perfectly to BCF membership. Reassign misassigned orders to the correct account.
  misassigned_orders <- list(
    list(
      from = "illorenzo",
      to = "Ostara",
      begin = as.Date("2017-02-08"),
      end = as.Date("2017-02-20")
    )
  )
  for (x in misassigned_orders) {
    orders %<>%
      mutate_when(
        user_id == x$from & order_date >= x$begin & order_date <= x$end,
        user_id = x$to
      )
  }
  
  # ---- Split out total-only orders ----
  
  # The first few orders did not separate price components. Reconstruct wholesale price, tax, and markup for these orders.
  total_only_orders <- list(
    list(
      account_id = "bcf_goldenorganics",
      date = as.Date("2017-02-08"),
      markup = 0.1
    ),
    list(
      account_id = "bcf_costco",
      date = as.Date("2017-02-08"),
      tax = 0.0346
    ),
    list(
      account_id = "bcf_fiordilatte",
      date = as.Date("2017-02-13"),
      markup = 0.039803,
      tax_applied = 0.08995
    )
  )
  for (x in total_only_orders) {
    mask <- orders %>%
      with(account_id == x$account_id & order_date == x$date)
    modified <- orders[mask, ] %>%
      dplyr::mutate(
        pretax = pretax %>%
          divide_by(1 + sum(x$tax, x$markup)) %>%
          round(2),
        tax = pretax %>%
          multiply_by(sum(x$tax, x$tax_applied)) %>%
          round(2),
        invoice = pretax + tax,
        order_subtotal = invoice,
        member_fees = invoice %>%
          multiply_by(sum(x$markup, x$markup_applied)) %>%
          round(2),
        # Adjust member fee rounding so that total remains unchanged
        member_fees = member_fees %>%
          add(orders$overall_order[mask] - (order_subtotal + member_fees)) %>%
          round(2),
        overall_order = order_subtotal + member_fees
      )
    err <- (orders$overall_order[mask] - modified$overall_order) %>%
      round(2)
    if (any(err != 0)) {
      stop("Change in order total")
    }
    orders[mask, ] <- modified
  }
  
  # ---- Fix buggy invoice totals (2017-06-26) ----
  
  # For certain orders, invoice and order_subtotal equal pretax + tax + member_fees, instead of pretax + tax as expected.
  orders %<>%
    mutate_when(
      order_date == as.Date("2017-06-26") &
        account_id %in% c("bcf_consciouscoffee", "bcf_costco_nf", "bcf_goldenorganics"),
      invoice = pretax + tax,
      order_subtotal = invoice
    )
  
  # ---- Split mixed bcf_internal order (2018-04-16) ----
  
  # Olive oil was sold in the same bcf_internal order as member shares. Move the olive oil portion to bcf_other.
  mixed_orders <- list(
    list(
      user_id = "ingram",
      shares = 50
    ),
    list(
      user_id = "oragoldman",
      shares = 25
    )
  )
  for (x in mixed_orders) {
    mask <- orders %>%
      with(
        order_date == as.Date("2018-04-16") & account_id == "bcf_internal" &
          user_id == x$user_id
      )
    # Set bcf_internal to only member shares
    internal <- orders[mask, ] %>%
      dplyr::mutate(
        pretax = x$shares %>%
          # Shares were invoiced with 10% (member) markup, rather than non-member markup.
          divide_by(1.10) %>%
          round(2),
        tax = 0,
        invoice = pretax + tax,
        order_subtotal = invoice,
        member_fees = x$shares %>%
          multiply_by(0.10 / 1.10) %>%
          round(2),
        overall_order = order_subtotal + member_fees
      )
    # Set bcf_other to only olive oil
    other <- orders[mask, ] %>%
      dplyr::mutate(
        account_id = "bcf_other"
      )
    numeric_vars <- other %>% 
      dplyr::select_if(is.numeric) %>%
      names()
    other[, numeric_vars] %<>%
      subtract(internal[numeric_vars])
    # Check and combine
    err <- (x$shares - internal$overall_order) %>%
      round(2)
    if (err != 0) {
      stop("Change in order total")
    }
    orders[mask, ] <- internal
    orders %<>%
      dplyr::bind_rows(other)
  }
  
  # ---- Move bcf_internal olive oil order to bcf_other (2018-05-07) ----
  
  # Olive oil (sold from inventory) was sold as bcf_internal. Reassign this order to bcf_other. Furthermore, reduce pretax per gallon from 26.85 to 25, and transfer the difference to member fees.
  orders %<>%
    mutate_when(
      order_date == as.Date("2018-05-07") & account_id == "bcf_internal",
      account_id = "bcf_other",
      pretax = (pretax * 25 / 26.85) %>%
        round(2),
      tax = (pretax * 0.0386) %>%
        round(2),
      invoice = pretax + tax,
      order_subtotal = invoice,
      member_fees = (overall_order - order_subtotal) %>%
        round(2)
    )
  
  # ---- Correct Chrysalis markup fiasco ----
  
  # Remove spurious Costco (non-food) order
  orders %<>%
    dplyr::filter(
      !(order_date == as.Date("2018-09-14") & account_id == "bcf_costco_nf")
    )
  
  # Fix Chrysalis member_fees (15% -> 10%)
  # 2018-08-30 Golden Organics, 2018-09-04 Costco
  orders %<>%
    mutate_when(
      user_id == "Chrysalis" & (
        (order_date == as.Date("2018-08-30") & account_id == "bcf_goldenorganics") |
          (order_date == as.Date("2018-09-04") & account_id == "bcf_costco")),
      member_fees = order_subtotal * 0.10,
      overall_order = order_subtotal + member_fees
    )
  
  # ---- Set member shares to pretax only ----
  
  orders %<>%
    mutate_when(
      account_id == "bcf_internal",
      pretax = overall_order,
      tax = 0,
      invoice = overall_order,
      order_subtotal = overall_order,
      member_fees = 0
    )
  
  # ---- Fix rounding error in order_subtotal ----
  
  # For some reason, order_subtotal is sometimes off by 1 cent. Adjust markup to fix the sum without modifying the total.
  err <- orders %>%
    with(overall_order - (order_subtotal + member_fees)) %>%
    round(2)
  mask <- abs(err) == 0.01
  orders$member_fees[mask] %<>%
    add(err[mask])

  # ---- Apply refunds ----
  
  orders$pretax <- orders$pretax + orders$refunds
  orders$invoice <- orders$invoice + orders$refunds
  orders$refunds <- 0
  
  # ---- Check result ----
  
  is_negative <- orders %>%
    dplyr::select_if(is.numeric) %>%
    is_less_than(0)
  if (any(is_negative)) {
    warning("Negative values")
  }
  if (any(orders$refunds != 0)) {
    warning("Non-zero refunds")
  }
  if (any(orders$custom_fees != 0)) {
    warning("Non-zero custom fees")
  }
  if (any(orders$order_subtotal != orders$invoice)) {
    warning("order_subtotal != invoice")
  }
  err <- orders$invoice - (orders$pretax + orders$tax)
  if (any(round(err, 2) > 0.01)) {
    warning("pretax + tax != invoice")
  }
  err <- orders$overall_order - (orders$order_subtotal + orders$member_fees)
  if (any(round(err, 2) != 0)) {
    warning("order_subtotal + member_fees != overall_order")
  }
  tax_rate <- orders$tax / orders$pretax
  if (any(round(tax_rate, 2) > 0.09)) {
    warning("Tax rate greater than 9%")
  }
  markup <- orders$member_fees / orders$order_subtotal
  # Ignore olive oil sold from inventory
  mask <- orders %>%
    with(account_id == "bcf_other" & order_date == as.Date("2018-05-07")) %>%
    not()
  if (any(round(markup[mask], 2) > 0.25)) {
    warning("Markup greater than 25%")
  }
  
  # ---- Return result ----
  orders
}

#' Format Foodclub orders
#' 
#' Reformats orders data into the format used for tax filing and patronage calculations.
#' 
#' - `price_paid`: Pretax (wholesale) price paid to supplier
#' - `tax_paid`: Tax paid to supplier
#' - `sales`: Sales (price + markup) collected from user
#' - `tax`: Tax collected from member
#' - `food`: Fraction of food in order
#' - `tax_exempt`: Whether transaction was tax-exempt
#' 
#' @param orders (data.frame) Table `custom_view_dw_archived_invoice_user_totals_bouldercoopfood`
#' @param exempt (list) Tax-exempt periods. Use the format list(user_id = ., from = ., to = .) for each user user_id and date interval [from, to].
#' @param food_tax (numeric) Sales tax rate for food sales
#' @param nonfood_tax (numeric) Sales tax rate for non-food sales
#' @return Reformatted `orders`
format_foodclub_orders <- function(orders, exempt = list(), food_tax = 0.0386, nonfood_tax = 0.08845) {
  orders %<>%
    dplyr::mutate(
      price_paid = pretax,
      # Rate of sales tax paid to supplier
      tax_rate_paid = dplyr::case_when(
        # Costco
        # Before 2017-06-10: Tax paid on food (0.0346) and non-food (0.08445)
        # Since 2017-06-10: No tax paid (reimbursed for orders through 2017-07-20)
        account_id == "bcf_costco" & order_date < as.Date("2017-06-10") ~ 0.0346,
        account_id == "bcf_costco" & order_date >= as.Date("2017-06-10") ~ 0,
        account_id == "bcf_costco_nf" & order_date < as.Date("2017-06-10") ~ 0.08445,
        account_id == "bcf_costco_nf" & order_date >= as.Date("2017-06-10") ~ 0,
        # Frontier
        # Before 2017-08-31: Tax paid combination of food (0.0386) and non-food (0.08845)
        # Since 2017-08-31: No tax paid
        account_id == "bcf_frontiernaturalfoods" & order_date < as.Date("2017-08-31") ~ tax / pretax,
        account_id == "bcf_frontiernaturalfoods" & order_date >= as.Date("2017-08-31") ~ 0,
        # All others: No tax paid
        TRUE ~ 0
      ),
      tax_paid = (pretax * tax_rate_paid) %>%
        round(2),
      collected = overall_order,
      food = dplyr::case_when(
        account_id == "bcf_frontiernaturalfoods" ~ ((tax / pretax) - nonfood_tax) / (food_tax - nonfood_tax),
        account_id %in% c("bcf_costco_nf", "bcf_goldenorganics_nf", "bcf_internal") ~ 0,
        TRUE ~ 1
      ),
      # Rate of sales tax owed to government
      tax_rate_owed = dplyr::case_when(
        account_id == "bcf_internal" ~ 0,
        TRUE ~ food * food_tax + (1 - food) * nonfood_tax
      ),
      sales = (collected / (1 + tax_rate_owed)) %>%
        round(2),
      tax = (sales * tax_rate_owed) %>%
        round(2),
      tax_exempt = FALSE
    ) %>%
    dplyr::select(
      order_date, account_id, user_id, price_paid, tax_paid, collected, sales,
      tax, food, tax_exempt
    )
  # Process tax exemptions
  for (x in exempt) {
    mask <- orders %>%
      with(user_id == x$user_id & order_date >= x$from & order_date <= x$to)
    orders$tax_exempt[mask] <- TRUE
  }
  # Set all but collected to zero for member shares
  orders %<>%
    mutate_when(
      account_id == "bcf_internal",
      price_paid = 0,
      sales = 0
    )
  # Return result
  orders
}

# ---- Golden Organics ----

#' Read Golden Organics pricelist
#' 
#' Since Golden Organics frequently makes mistakes in their pricelists, attempts are made to clean the data.
#' See functions go_parse_sizes, go_parse_categories, and go_parse_origins.
#' 
#' @param path (character) Path to pricelist. Expects an Excel spreadsheet (xls or xlsx).
#' @param skip (integer) Number of rows to skip in the spreadsheet. Should correspond to the number of rows before the row containing column names.
#' @param code (character) Name of column with product codes
#' @param description (character) Name of column with product descriptions
#' @param origin (character) Name of column with product origins
#' @param size (character) Name of column with product sizes
#' @param price (character) Name of column with product price
#' @return Pricelist as a data.frame
go_read_pricelist <- function(
  path, skip = 7, code = 'Item ID', description = 'Item Description',
  origin = 'Location', size = 'Stocking U/M', price = 'Commercial') {
  df <- path %>%
    readxl::read_excel(skip = skip) %>%
    # Drop empty columns
    dplyr::select_if(.predicate = function(x) {!all(is.na(x))}) %>%
    # Drop empty rows
    dplyr::filter_all(dplyr::any_vars(!is.na(.))) %>%
    dplyr::mutate(category = NA_character_)
  # Determine index ranges for each category
  breaks <- df %>%
    # Category rows have content in first column but are empty elsewhere
    {!is.na(.[[1]]) & apply(is.na(.[, -1]), 1, all)} %>%
    which() %>%
    c(nrow(df) + 1)
  for (i in seq_along(breaks[-1])) {
    df$category[breaks[i]:(breaks[i + 1] - 1)] <- df[[breaks[i], 1]]
  }
  df %<>%
    # Drop category rows
    .[!seq_len(nrow(.)) %in% breaks, ] %>%
    # Rename columns
    dplyr::rename(
      code = code,
      description = description,
      origin = origin,
      size = size,
      price = price
    ) %>%
    dplyr::select(category, code, description, origin, size, price)
  # Parse size strings
  sizes <- df$size %>%
    go_parse_sizes()
  df %<>%
    dplyr::mutate(
      # Copy results parsed from size strings
      size = sizes$size,
      valid_split_increment = sizes$valid_split_increment,
      description = description %>%
        ifelse(
          is.na(sizes$description),
          ., paste0(., " (", sizes$description, ")")
        ) %>%
        gsub("[ ]+", " ", .),
      # Parse categories
      category = go_parse_categories(category),
      # Parse origins
      origin = go_parse_origins(origin)
    )
  # Check results
  # Duplicate code
  is_duplicate_code <- df$code %>%
    duplicated()
  if (any(is_duplicate_code)) {
    w <- df$code[is_duplicate_code] %>%
      paste0(collapse = ", ") %>%
      paste0("Items with duplicate codes [", ., "]")
    warning(w)
  }
  # Missing size
  is_missing_size <- df$size %>%
    is.na()
  if (any(is_missing_size)) {
    w <- df$code[is_missing_size] %>%
      paste0(collapse = ", ") %>%
      paste0("Items missing size [", ., "]")
    warning(w)
  }
  # Size (weight in pounds) inconsistent with item code
  has_pounds <- df$size %>%
    grepl("[0-9\\.]+ lb", .)
  pounds_from_size <- df$size[has_pounds] %>%
    stringr::str_match("^([0-9\\.]+) lb$") %>%
    .[, 2] %>%
    as.numeric()
  pounds_from_code <- df$code[has_pounds] %>%
    stringr::str_match("^[a-zA-Z]+([0-9]+)$") %>%
    .[, 2] %>%
    as.numeric()
  inconsistent_size <- (round(pounds_from_size) != pounds_from_code) %>%
    replace(is.na(.), FALSE)
  if (any(inconsistent_size)) {
    w <- df[has_pounds, ][inconsistent_size, ] %>%
      dplyr::mutate(
        `price/lb` = (price / pounds_from_size[inconsistent_size]) %>%
          round(2)
      ) %>%
      dplyr::select(code, size, `price/lb`, description) %>%
      as.data.frame() %>%
      {capture.output(print(., row.names = FALSE))} %>%
      paste0(collapse = "\n") %>%
      paste0("Items with size (weight in pounds) inconsistent with item code.\n\n", .)
    # Warnings are truncated (see https://stackoverflow.com/a/50387968)
    options(warning.length = 8000)
    warning(w)
  }
  # Return result
  df
}

#' Parse Golden Organics item sizes
#' 
#' - Assume unitless numbers are pounds (e.g. "1" -> "1 lb")
#' - Convert ounces to pounds (e.g. "12 oz" -> "0.75 lb")
#' - Generate description suffixes and split increments for multiples (e.g. "2 x 12 oz" -> size: "1.5 lb", description: "2 x 12 oz / 0.75 lb", valid_split_increment: "0.75")
#' - Assume empty sizes are single and unitless (e.g. "" -> "ea", short for "each")
#'
#' @param sizes (character) Vector of size strings from Golden Organics pricelist
#' @return data.frame with columns matching Foodclub product database (size, description, valid_split_increment)
#' @examples
#' go_parse_sizes(c("", NA, "1", "12 oz", "2 x 12 oz", "2 x 1 lb", "2 gal", "6 x #10", "6x108oz"))
go_parse_sizes <- function(sizes) {
  parts <- sizes %>%
    gsub("#|\\.$", "", .) %>%
    gsub("^([a-z])", "1 \\1", .) %>%
    gsub("([0-9])([a-z])", "\\1 \\2", .) %>%
    gsub("([0-9])$", "\\1 lb", .) %>%
    stringr::str_match("([0-9\\.]+)\\s*x*\\s*([0-9\\.]+)* ([a-z]+)")
  x <- ifelse(is.na(parts[, 3]), 1, parts[, 2]) %>%
    as.numeric()
  value <- ifelse(is.na(parts[, 3]), parts[, 2], parts[, 3]) %>%
    as.numeric()
  unit <- parts[, 4]
  new_value <- value
  new_unit <- unit
  # 16 oz = 1 lb
  oz = !is.na(unit) & unit == "oz"
  new_value[oz] %<>%
    divide_by(16) %>%
    round(digits = 3)
  new_unit[oz] <- "lb"
  # Results
  df <- data.frame(
    size = paste(new_value * x, new_unit) %>%
      replace(is.na(new_value), NA),
    description = paste(x, "x", value, unit) %>%
      replace(x == 1, NA),
    valid_split_increment = ifelse(x == 1, 0, new_value),
    stringsAsFactors = FALSE
  )
  unit_changed <- !is.na(df$description) & !is.na(new_unit) & new_unit != unit
  df$description[unit_changed] %<>%
    paste("/", new_value[unit_changed], new_unit[unit_changed])
  # Replace NA and "1 ea" with "ea"
  df$size[is.na(df$size) | df$size == "1 ea"] <- "ea"
  df
}

#' Parse Golden Organics item categories
#'
#' - Replace dash ("-") with space (" ")
#' - Convert to title case (e.g. "Dried Fruit")
#' - Enforce an (arbitrary) standard for pluralization and word separation (e.g. "Trailmix" rather than "Trail Mix" or "Trailmixes")
#' 
#' @param categories (character) Vector of category strings from Golden Organics pricelist
#' @return Vector with standardized category names (hopefully matching those already used in the Foodclub product database)
#' @examples
#' go_parse_categories(c("SPICE", "BAKING", "Sweetner", "Nonfoods"))
go_parse_categories <- function(categories) {
  replacements <- c(
    Bean = "Beans",
    Cereal = "Cereals",
    `Dried Fruits` = "Dried Fruit",
    Feeds = "Feed",
    Flours = "Flour",
    Grains = "Grain",
    Lentil = "Lentils",
    Nonfoods = "Nonfood",
    Nonfood = "Non Food",
    `Non Foods` = "Non Food",
    `Nut Butter` = "Nut Butters",
    Nut = "Nuts",
    Oil = "Oils",
    Pastas = "Pasta",
    Pea = "Peas",
    Seed = "Seeds",
    Spice = "Spices",
    Sweetner = "Sweetners",
    Sweetners = "Sweeteners",
    `Trail mixes` = "Trail mix",
    `Trail mix` = "Trailmix",
    Trailmixes = "Trailmix"
  ) %>%
    set_names(paste0("^", names(.), "$"))
  categories %>%
    gsub("-", " ", .) %>%
    stringr::str_to_title() %>%
    stringr::str_replace_all(replacements)
}

#' Parse Golden Organics item origins
#'
#' - Convert to title case (e.g. "Dominican Republic")
#' - Correct common spelling mistakes (e.g. "Thailnd" -> "Thailand")
#' 
#' @param origins (character) Vector of origin strings from Golden Organics pricelist
#' @return Vector with standardized origin names (hopefully matching those already used in the Foodclub product database)
#' @examples
#' go_parse_origins(c("USA", "US", "F38", "Philppines"))
go_parse_origins <- function(origins) {
  replacements <- c(
    Dominica = "Dominican Republic",
    `Dom Repub` = "Dominican Republic",
    F38 = NA,
    `N. Dakota` = "North Dakota",
    `S. Dakota` = "South Dakota",
    Philppines = "Philippines",
    Philippine = "Philippines",
    Philipines = "Philippines",
    Spaon = "Spain",
    Usa = "USA",
    Ua = "USA",
    Us = "USA",
    Netherland = "Netherlands",
    Thailnd = "Thailand"
  ) %>%
    set_names(paste0("^", names(.), "$"))
  origins %>%
    stringr::str_to_title() %>%
    stringr::str_replace_all(replacements)
}

#' Build SQL query to update product database
#'
#' @param old (data.frame) Foodclub Golden Organics product database (see \code{\link{pma_get_table}})
#' @param new (data.frame) Golden Organics pricelist
#' @param token (character) Authorization token (see \code{\link{pma_login}})
#' @return Vector of UPDATE SQL commands
#' @examples
#' \dontrun{
#' token <- pma_login("username", "passworld")
#' old <- pma_get_table("private_bcf_goldenorganics", token = token)
#' new <- go_read_pricelist("pricelist.xlsx")
#' go_build_sql(old, new, token = token)
#' }
go_build_sql <- function(old, new, token) {
  update_sql <- function(i, j = NULL) {
    updates <- list()
    if (length(j) == 0) {
      # Out of stock
      if (is.na(old$num_available[i])) {
        updates["num_available"] <- 0
      }
    } else {
      # In stock
      if (!new$price[j] %in% as.numeric(old$price[i])) {
        updates["price"] <- new$price[j]
      }
      if (!new$size[j] %in% old$size[i]) {
        updates["size"] <- new$size[j]
      }
      if (!new$description[j] %in% old$description[i]) {
        updates["description"] <- new$description[j]
      }
      if (!new$category[j] %in% old$category[i]) {
        updates["category"] <- new$category[j]
      }
      if (!new$origin[j] %in% old$origin[i]) {
        updates["origin"] <- new$origin[j]
      }
      if (!is.na(old$num_available[i])) {
        updates["num_available"] <- NA
      }
    }
    if (length(updates) > 0) {
      updates %>%
        lapply(format_sql) %>%
        paste(names(.), ., sep = " = ", collapse = ", ") %>%
        paste("UPDATE private_bcf_goldenorganics SET", ., "WHERE code =", format_sql(old$code[i]))
    }
  }
  insert_sql <- function(j) {
    defaults <- list(
      valid_order_increment = 1
    )
    values <- c(defaults, new[j, ])
    values %>%
      lapply(format_sql) %>%
      {
        paste0(
          "INSERT INTO private_bcf_goldenorganics (",
          paste(names(.), collapse = ", "),
          ") VALUES (",
          paste(., collapse = ", "), ")")
      }
  }
  union(old$code, new$code) %>%
    lapply(function(code) {
      i = which(old$code == code)
      j = which(new$code == code)
      is_i <- length(i) == 1
      is_j <- length(j) == 1
      if (is_i & !is_j) {
        # UPDATE: Out of stock
        sql <- update_sql(i)
      } else if (is_i & is_j) {
        # UPDATE: In stock
        sql <- update_sql(i, j)
      } else if (!is_i & is_j) {
        # INSERT
        sql <- insert_sql(j)
      } else {
        stop(paste0(code, " - i: ", i, ", j: ", j))
      }
      sql %>%
        gsub("'NA'", "NULL", .)
    }) %>%
    unlist()
}
