# Мини-проект №4, Катаев Максим

# Подключаются необходимые библиотеки
library(DBI)
library(RSQLite)
library(data.table)

#####  Знакомство с данными  #####

# path.expand("~") автоматически подставляет путь к домашней папке.
# Ниже реализован вариант для файлов из архива moex в папке "Загрузки",
# при необходимости можно изменить название папки
db_path_1 <- file.path(path.expand("~"), "Downloads/moex", "tradelog.sqlite")
db_path_2 <- file.path(path.expand("~"), "Downloads/moex", "orderlog.sqlite")

# Проверка на то, существуют ли файлы
if (!file.exists(db_path_1)) {
  stop("К сожалению, файл не найден. Рекомендуется проверить путь: ", db_path_1)
}
if (!file.exists(db_path_2)) {
  stop("К сожалению, файл не найден. Рекомендуется проверить путь: ", db_path_2)
}


# Подключение к базе данных с помощью dbConnect
con <- list(
  tradelog = dbConnect(SQLite(), dbname = db_path_1),
  orderlog = dbConnect(SQLite(), dbname = db_path_2)
)

# Получение списка всех таблиц в базе
dbListTables(con$tradelog)
dbListTables(con$orderlog) 

# Важно, что в orderlog меньше дней, их всего 5


#####  Вспомогательные функции для заданий 1-3  #####

# Устраняем возможные неточности в данных:
# сейчас они хранятся в формате YYYYMMDD, и код реализован так, чтобы дата
# воспринималась программой в этом формате или в формате YYYY-MM-DD
normalize_date <- function(date) {
  if (inherits(date, "Date")) {
    date_str <- format(date, "%Y-%m-%d")
  } else {
    date <- as.character(date)
    
    # grepl для поиска совпадений по возможным символам в дате
    if (grepl("^\\d{8}$", date)) {
      date_str <- sprintf(
        "%s-%s-%s",
        substr(date, 1, 4),
        substr(date, 5, 6),
        substr(date, 7, 8)
      )
    } else if (grepl("^\\d{4}-\\d{2}-\\d{2}$", date)) {
      date_str <- date
    } else {
      stop("Дата должна быть в формате 'YYYYMMDD' или 'YYYY-MM-DD'")
    }
  }
  
  # Замена для дефисов
  table_name <- gsub("-", "", date_str)
  
  list(
    date = date_str,
    table = table_name
  )
}

# В задании 3 нам нужно будет брать произвольный интервал для кода из задания 1.
# Будем исходить из следующей логики. Зададим в функции get_ohlcv() для свечей
# параметр freq (частота данных). Сделаем так, чтобы пользователь мог передать
# в этот параметр разные обозначения времени, в том числе в виде текста.
# Например, 1 день пользователь может обозначить как "1 день", "1 day"
# или "1 d", и программа все это поймет. Но базово будем считать, что freq
# принимает на вход количество секунд, и если пользователю нужен 1 день,
# то нужно ввести freq = 86400 (количество секунд в 1 дне).

parse_freq_seconds <- function(freq) {
  if (inherits(freq, "difftime")) {
    freq_sec <- as.numeric(freq, units = "secs")
  } else if (is.numeric(freq)) {
    freq_sec <- as.numeric(freq)
  } else if (is.character(freq)) {
    x <- tolower(trimws(freq))
    
    num <- suppressWarnings(as.numeric(sub("^([0-9]+).*", "\\1", x)))
    unit <- trimws(sub("^[0-9]+\\s*", "", x))
    
    if (is.na(num) || unit == x || unit == "") {
      stop("Не удалось распознать freq. Примеры: 60, '1 min', '5 mins', '1 h'")
    }
    
    mult <- if (unit %in% c("s", "sec", "secs", "second", "seconds",
                            "сек", "секунда", "секунды", "секунд")) {
      1
    } else if (unit %in% c("m", "min", "mins", "minute", "minutes",
                           "мин", "минута", "минуты", "минут")) {
      60
    } else if (unit %in% c("h", "hour", "hours",
                           "час", "часа", "часов")) {
      3600
    } else if (unit %in% c("d", "day", "days",
                           "день", "дня", "дней")) {
      86400
    } else if (unit %in% c("w", "week", "weeks",
                           "неделя", "недели", "недель")) {
      604800
    } else {
      stop("Неизвестная единица freq: ", unit)
    }
    
    freq_sec <- num * mult
  } else {
    stop("freq должен быть numeric, difftime или character")
  }
  
  if (!is.finite(freq_sec) || freq_sec <= 0 || freq_sec != floor(freq_sec)) {
    stop("freq должен быть положительным целым числом секунд")
  }
  
  as.integer(freq_sec)
}

# Переводим длинные числа обозначения времени в количество секунд
hms_to_seconds <- function(x) {
  x <- as.numeric(x)
  h <- x %/% 10000
  m <- (x %/% 100) %% 100
  s <- x %% 100
  as.integer(h * 3600 + m * 60 + s)
}

# Переводим длинные числа обозначения времени в количество секунд
hmsms_to_seconds <- function(x) {
  x <- as.numeric(x)
  hms <- x %/% 1000
  hms_to_seconds(hms)
}

# Получение ключа (понадобится в задании 2)
price_key <- function(x) {
  format(x, scientific = FALSE, trim = TRUE, digits = 15)
}


#####  Задание 1 (и задание 3)  #####

# Основная функция
get_ohlcv <- function(con_tradelog,
                      date,
                      seccode,
                      freq = "1 min",
                      fill_empty = FALSE,
                      tz = "UTC") {
  
  # Используем ранее созданные функции для времени
  date_info <- normalize_date(date)
  start_date <- as.Date(date_info$date)
  freq_sec <- parse_freq_seconds(freq)
  
  # В задании 3 мы не смотрим дальше недели (по условию)
  if (freq_sec > 604800) {
    stop("Для свечей freq не должен превышать 1 неделю")
  }
  
  # В задании 3 мы не смотрим дальше недели, но можем смотреть дальше 1 дня,
  # хотя базово мы подаем в функцию значение 1 дня (одна таблица).
  
  # Реализуем следующий подход. Если пользователь введет больше 1 дня для freq,
  # то программа будет искать в базе данных таблицы, у которых название
  # соответствует следующим дням (если они закончатся, то программа остановит
  # поиск). Если freq = 2 дня, например, то программа сделает 1 свечу по 2 дням,
  # как этого и требует задача агрегирования.
  
  
  # Сколько всего дней нужно пользователю
  n_days <- ceiling(freq_sec / 86400)
  
  # Получаем все дни, то есть берем необходимые таблицы
  available_tables <- dbListTables(con_tradelog)
  
  # Берем даты
  available_tables <- available_tables[grepl("^\\d{8}$", available_tables)]
  available_tables <- sort(available_tables)
  
  # Стартовый день
  start_table <- normalize_date(date)$table
  
  if (!(start_table %in% available_tables)) {
    stop("Стартовая дата отсутствует в базе")
  }
  
  start_idx <- which(available_tables == start_table)
  
  # Поиск доступных дней
  selected_tables <- available_tables[
    start_idx:min(start_idx + n_days - 1, length(available_tables))
  ]
  
  all_trades <- list()
  
  for (table_name in selected_tables) {
    
    if (!dbExistsTable(con_tradelog, table_name)) next
    
    date_str <- paste0(
      substr(table_name, 1, 4), "-",
      substr(table_name, 5, 6), "-",
      substr(table_name, 7, 8)
    )
    
    tbl <- as.character(dbQuoteIdentifier(con_tradelog, table_name))
    
    # Запрос SQL
    sql <- sprintf("
    SELECT TRADENO, SECCODE, TIME, PRICE, VOLUME
    FROM %s
    WHERE SECCODE = ?
    ORDER BY TIME, TRADENO
  ", tbl)
    
    tmp <- as.data.table(
      dbGetQuery(con_tradelog, sql, params = list(seccode))
    )
    
    if (nrow(tmp) == 0) next
    
    # Для формата времени
    base_time <- as.POSIXct(date_str, tz = tz)
    tmp[, sec := hms_to_seconds(TIME)]
    tmp[, datetime := base_time + sec]
    
    all_trades[[length(all_trades) + 1]] <- tmp
  }
  
  if (length(all_trades) == 0) {
    return(data.table())
  }
  
  # Собираем значения вместе с помощью rbindlist
  trades <- rbindlist(all_trades)
  setorder(trades, datetime)
  
  # По временным периодам все собрали, переходим к формированию свечей
  origin <- min(trades$datetime)
  
  trades[, bucket := as.integer(
    difftime(datetime, origin, units = "secs")) %/% freq_sec]
  
  candles <- trades[
    ,
    .(
      Time = origin + bucket[1] * freq_sec,
      Open = PRICE[1],
      High = max(PRICE),
      Low = min(PRICE),
      Close = PRICE[.N],
      Volume = sum(as.numeric(VOLUME))
    ),
    by = bucket
  ]
  
  setorder(candles, Time)
  
  return(candles[])
}


# На основе get_ohlcv() отдельно создадим функцию для минутных свечей
get_minute_ohlcv <- function(con_tradelog,
                             date,
                             seccode,
                             fill_empty = FALSE,
                             tz = "UTC") {
  get_ohlcv(
    con_tradelog = con_tradelog,
    date = date,
    seccode = seccode,
    freq = "1 min",
    fill_empty = fill_empty,
    tz = tz
  )
}


#####  Задание 2 (и задание 3)  #####

# Основная функция
get_best_quotes <- function(con_orderlog,
                            date,
                            seccode,
                            freq = 1,
                            tz = "UTC") {
  
  # Используем ранее созданные функции для времени
  date_info <- normalize_date(date)
  table_name <- date_info$table
  date_str <- date_info$date
  freq_sec <- parse_freq_seconds(freq)
  
  # Проверка
  if (!dbExistsTable(con_orderlog, table_name)) {
    stop("В orderlog.sqlite нет таблицы ", table_name)
  }
  
  tbl <- as.character(dbQuoteIdentifier(con_orderlog, table_name))
  
  # Запрос SQL
  sql <- sprintf("
    SELECT
      NO,
      SECCODE,
      BUYSELL,
      TIME,
      ORDERNO,
      ACTION,
      PRICE,
      VOLUME
    FROM %s
    WHERE SECCODE = ?
    ORDER BY TIME, NO
  ", tbl)
  
  # Сформируем список событий
  events <- as.data.table(
    dbGetQuery(con_orderlog, sql, params = list(seccode))
  )
  
  if (nrow(events) == 0) {
    return(data.table(
      Time = as.POSIXct(character(), tz = tz),
      BestBid = numeric(),
      BestAsk = numeric()
    ))
  }
  
  events[, sec := hmsms_to_seconds(TIME)]
  setorder(events, TIME, NO)
  
  base_time <- as.POSIXct(date_str, tz = tz)
  
  
  # Агрегирование данных для стакана
  
  # Создадим новое окружение для активных заявок
  order_side <- new.env(parent = emptyenv())
  order_price <- new.env(parent = emptyenv())
  order_vol <- new.env(parent = emptyenv())
  
  # Создадим новое окружение для объемов на ценовых уровнях
  bid_levels <- new.env(parent = emptyenv())
  ask_levels <- new.env(parent = emptyenv())
  best_bid <- NA_real_
  best_ask <- NA_real_
  
  # Функция для подбора лучшей цены спроса (максимальная)
  recalc_best_bid <- function() {
    keys <- ls(bid_levels, all.names = TRUE)
    if (length(keys) == 0) {
      NA_real_
    } else {
      max(as.numeric(keys))
    }
  }
  
  # Функция для подбора лучшей цены предложения (минимальная)
  recalc_best_ask <- function() {
    keys <- ls(ask_levels, all.names = TRUE)
    if (length(keys) == 0) {
      NA_real_
    } else {
      min(as.numeric(keys))
    }
  }
  
  
  # Формирование новых записей и анализ объемов в свечах
  update_level <- function(side, price, delta) {
    pk <- price_key(price)
    
    # Определение характера заявки (покупка или продажа)
    levels <- if (side == "B") bid_levels else ask_levels
    
    old_vol <- levels[[pk]]
    if (is.null(old_vol)) old_vol <- 0
    
    new_vol <- old_vol + delta
    
    removed <- FALSE
    
    if (new_vol <= 1e-9) {
      if (exists(pk, envir = levels, inherits = FALSE)) {
        rm(list = pk, envir = levels)
      }
      removed <- TRUE
      new_vol <- 0
    } else {
      levels[[pk]] <- new_vol
    }
    
    if (side == "B") {
      if (delta > 0 && (is.na(best_bid) || price > best_bid)) {
        best_bid <<- price
      }
      
      if (removed && !is.na(best_bid) && pk == price_key(best_bid)) {
        best_bid <<- recalc_best_bid()
      }
    } else {
      if (delta > 0 && (is.na(best_ask) || price < best_ask)) {
        best_ask <<- price
      }
      
      if (removed && !is.na(best_ask) && pk == price_key(best_ask)) {
        best_ask <<- recalc_best_ask()
      }
    }
    
    invisible(new_vol)
  }
  
  remove_order <- function(ord_key) {
    cur_vol <- order_vol[[ord_key]]
    
    if (is.null(cur_vol)) {
      return(invisible(FALSE))
    }
    
    cur_side <- order_side[[ord_key]]
    cur_price <- order_price[[ord_key]]
    
    # Формирование новых записей и анализ объемов в свечах с помощью
    # ранее созданной функции
    update_level(
      side = cur_side,
      price = cur_price,
      delta = -cur_vol
    )
    
    rm(list = ord_key, envir = order_side)
    rm(list = ord_key, envir = order_price)
    rm(list = ord_key, envir = order_vol)
    
    invisible(TRUE)
  }
  
  
  # Реализуем функцию для работы с состоянием стакана. Заявка считается 
  # активной с момента постановки (ACTION=1) до снятия (ACTION=0) или полного 
  # исполнения через сделки (ACTION=2). При ACTION=2 из стакана списывается 
  # объём сделки, а при ACTION=0 заявка снимается целиком.
  
  process_event <- function(side, action, ord_key, price, volume) {
    if (is.na(ord_key) || ord_key == "NA") {
      return(invisible(NULL))
    }
    
    volume <- as.numeric(volume)
    price <- as.numeric(price)
    
    if (action == 1) {
      
      # Новая заявка. В том случае, если заявка с таким ORDERNO уже активна,
      # сначала удалим старое состояние
      
      if (!is.null(order_vol[[ord_key]])) {
        remove_order(ord_key)
      }
      
      # Формируем структуру по базе данных
      order_side[[ord_key]] <- side
      order_price[[ord_key]] <- price
      order_vol[[ord_key]] <- volume
      
      update_level(
        side = side,
        price = price,
        delta = volume
      )
      
    } else if (action == 2) {
      
      # Сделка, из стакана списывается объём сделки
      
      cur_vol <- order_vol[[ord_key]]
      
      if (!is.null(cur_vol)) {
        cur_side <- order_side[[ord_key]]
        cur_price <- order_price[[ord_key]]
        
        traded_vol <- min(as.numeric(cur_vol), volume)
        
        # Формирование новых записей с помощью ранее созданной функции
        update_level(
          side = cur_side,
          price = cur_price,
          delta = -traded_vol
        )
        
        new_vol <- as.numeric(cur_vol) - volume
        
        # Формирование объема для срезов
        if (new_vol <= 1e-9) {
          rm(list = ord_key, envir = order_side)
          rm(list = ord_key, envir = order_price)
          rm(list = ord_key, envir = order_vol)
        } else {
          order_vol[[ord_key]] <- new_vol
        }
      }
      
    } else if (action == 0) {
      
      # Снятие заявки, заявка снимается целиком
      
      remove_order(ord_key)
    }
    
    invisible(NULL)
  }
  
  
  # Сформируем временную сетку срезов
  first_sec <- min(events$sec)
  last_sec <- max(events$sec)
  
  # Срез для интервала
  first_end <- (first_sec %/% freq_sec) * freq_sec + freq_sec - 1
  last_end <- (last_sec %/% freq_sec) * freq_sec + freq_sec - 1
  grid_end <- seq(first_end, last_end, by = freq_sec)
  n_grid <- length(grid_end)
  
  out_bid <- rep(NA_real_, n_grid)
  out_ask <- rep(NA_real_, n_grid)
  
  
  # Создадим структуру для итерации по записям
  ev_sec <- events$sec
  ev_side <- events$BUYSELL
  ev_action <- events$ACTION
  ev_order <- as.character(events$ORDERNO)
  ev_price <- events$PRICE
  ev_volume <- events$VOLUME
  
  # Установим счетчик для цикла
  i <- 1L
  
  # Определим общее количество событий
  n_events <- nrow(events)
  
  for (g in seq_along(grid_end)) {
    boundary <- grid_end[g]
    
    # Применяем все события до конца текущего интервала включительно
    while (i <= n_events && ev_sec[i] <= boundary) {
      process_event(
        side = ev_side[i],
        action = ev_action[i],
        ord_key = ev_order[i],
        price = ev_price[i],
        volume = ev_volume[i]
      )
      
      i <- i + 1L
    }
    
    # Лучшие цены спроса и предложения
    out_bid[g] <- best_bid
    out_ask[g] <- best_ask
  }
  
  # Итоговые значения
  result <- data.table(
    Time = base_time + grid_end,
    BestBid = out_bid,
    BestAsk = out_ask
  )
  
  result[]
}


# На основе get_best_quotes() отдельно создадим функцию для freq = 1 сек,
# в том числе для проверки скорости работы
get_second_best_quotes <- function(con_orderlog,
                                   date,
                                   seccode,
                                   tz = "UTC") {
  get_best_quotes(
    con_orderlog = con_orderlog,
    date = date,
    seccode = seccode,
    freq = 1,
    tz = tz
  )
}


#####  Примеры запуска итоговых функций для заданий 1-3  #####

# Необходимо ввести стартовый день в формате YYYY-MM-DD или YYYYMMDD
day <- "20150421"

# Необходимо ввести тикер акции, торгующейся на МосБирже
sec <- "SBER"

# Задание 1: минутные свечи
candles_1m <- get_minute_ohlcv(
  con_tradelog = con$tradelog,
  date = day,
  seccode = sec
)
View(candles_1m)

# Задание 1 плюс задание 3: свечи с произвольной частотой.
# Параметр freq принимает на вход число секунд, но можно вводить и значения
# других форматов, таких как "5 mins", "3 часа" или "1 d"
candles_own_time <- get_ohlcv(
  con_tradelog = con$tradelog,
  date = day,
  seccode = sec,
  freq = "5 минут"
)
View(candles_own_time)


# Задание 2: секундные срезы best bid / best ask
quotes_1s <- get_second_best_quotes(
  con_orderlog = con$orderlog,
  date = day,
  seccode = sec
)
View(quotes_1s)

# Задание 2 плюс задание 3: срезы стакана для произвольного кол-ва секунд (freq)
quotes_own_seconds_amount <- get_best_quotes(
  con_orderlog = con$orderlog,
  date = day,
  seccode = sec,
  freq = 3611
)
View(quotes_own_seconds_amount)


# По окончании работы с базой данных рекомендуется отключиться от нее
dbDisconnect(con$tradelog)
dbDisconnect(con$orderlog)
