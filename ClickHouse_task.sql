-- 1. Создаем таблицу с сырыми логами событий (TTL 30 дней)
CREATE TABLE user_events
(
    user_id UInt32,
    event_type String,
    points_spent UInt32,
    event_time DateTime
)
ENGINE = MergeTree()
ORDER BY (event_time, user_id)
TTL event_time + INTERVAL 30 DAY;

-- 2. Создаем таблицу агрегатов (TTL 180 дней)
CREATE TABLE user_events_agg
(
    event_date Date,
    event_type String,
    unique_users AggregateFunction(uniq, UInt32),
    total_spent AggregateFunction(sum, UInt32),
    total_actions AggregateFunction(count, UInt32)
)
ENGINE = AggregatingMergeTree()
ORDER BY (event_date, event_type)
TTL event_date + INTERVAL 180 DAY;

-- 3. Создаем MV
CREATE MATERIALIZED VIEW user_events_mv TO user_events_agg AS
SELECT
       toDate(event_time) AS event_date,
       event_type,
       uniqState(user_id) AS unique_users,
       sumState(points_spent) AS total_spent,
       countState() AS total_actions
FROM user_events
GROUP BY event_date, event_type;

-- 4. Запрос для вставки тестовых данных
-- События 10 дней назад
INSERT INTO user_events VALUES
                            (1, 'login', 0, now() - INTERVAL 10 DAY),
                            (2, 'signup', 0, now() - INTERVAL 10 DAY),
                            (3, 'login', 0, now() - INTERVAL 10 DAY);

-- События 7 дней назад
INSERT INTO user_events VALUES
                            (1, 'login', 0, now() - INTERVAL 7 DAY),
                            (2, 'login', 0, now() - INTERVAL 7 DAY),
                            (3, 'purchase', 30, now() - INTERVAL 7 DAY);

-- События 5 дней назад
INSERT INTO user_events VALUES
                            (1, 'purchase', 50, now() - INTERVAL 5 DAY),
                            (2, 'logout', 0, now() - INTERVAL 5 DAY),
                            (4, 'login', 0, now() - INTERVAL 5 DAY);

-- События 3 дня назад
INSERT INTO user_events VALUES
                            (1, 'login', 0, now() - INTERVAL 3 DAY),
                            (3, 'purchase', 70, now() - INTERVAL 3 DAY),
                            (5, 'signup', 0, now() - INTERVAL 3 DAY);

-- События вчера
INSERT INTO user_events VALUES
                            (2, 'purchase', 20, now() - INTERVAL 1 DAY),
                            (4, 'logout', 0, now() - INTERVAL 1 DAY),
                            (5, 'login', 0, now() - INTERVAL 1 DAY);

-- События сегодня
INSERT INTO user_events VALUES
                            (1, 'purchase', 25, now()),
                            (2, 'login', 0, now()),
                            (3, 'logout', 0, now()),
                            (6, 'signup', 0, now()),
                            (6, 'purchase', 100, now());

-- 5. Расчет retention (7-дневный)
WITH cohort AS (
    -- когорты за каждый день
    SELECT
        user_id,
        toDate(MIN(event_time)) AS cohort_date
    FROM user_events
    GROUP BY user_id
),
     activity AS (
         -- активность пользователей в течение 7 дней после когорты
         SELECT
             c.cohort_date,
             c.user_id,
             toDate(ue.event_time) AS activity_date
         FROM cohort c
                  LEFT JOIN user_events ue ON c.user_id = ue.user_id
             AND toDate(ue.event_time) BETWEEN c.cohort_date + INTERVAL 1 DAY
                                                  AND c.cohort_date + INTERVAL 7 DAY
     ),
     user_activity AS (
         -- определяем, вернулся ли пользователь
         SELECT
             c.cohort_date,
             c.user_id,
             MAX(a.activity_date) IS NOT NULL AS has_returned
         FROM cohort c
                  LEFT JOIN activity a ON c.cohort_date = a.cohort_date
             AND c.user_id = a.user_id
         GROUP BY c.cohort_date, c.user_id
     ),
     retention_stats AS (
         -- расчет статистики удержания
         SELECT
             cohort_date,
             COUNT(*) AS total_users_day_0,
             COUNTIf(has_returned) AS returned_in_7_days,
             ROUND((COUNTIf(has_returned) * 100.0) / COUNT(*), 2) AS retention_7d_percent
         FROM user_activity
         GROUP BY cohort_date
     )
SELECT
    total_users_day_0,
    returned_in_7_days,
    retention_7d_percent,
    format('{}|{}|{}%',
           toString(total_users_day_0),
           toString(returned_in_7_days),
           toString(retention_7d_percent)) AS retention_metric
FROM retention_stats
ORDER BY cohort_date;

-- 6. Запрос на быструю аналитику по дням
SELECT
    event_date,
    event_type,
    uniqMerge(unique_users) as unique_users,
    sumMerge(total_spent) as total_spent,
    countMerge(total_actions) as total_actions
FROM user_events_agg
WHERE event_date >= today() - INTERVAL 7 DAY
GROUP BY event_date, event_type
ORDER BY event_date DESC, event_type;
