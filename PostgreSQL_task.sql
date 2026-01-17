CREATE TABLE users (
                       id SERIAL PRIMARY KEY,
                       name TEXT,
                       email TEXT,
                       role TEXT,
                       updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE users_audit (
                        id SERIAL PRIMARY KEY,
                        user_id INTEGER,
                        changed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                        changed_by TEXT,
                        field_changed TEXT,
                        old_value TEXT,
                        new_value TEXT
);
-- Устанавливаем расширение pg_cron
CREATE EXTENSION IF NOT EXISTS pg_cron;

-- 1. Создаем функцию логирования изменений по трем полям (name, email, role)
CREATE OR REPLACE FUNCTION log_user_changes()
    RETURNS TRIGGER AS $$
BEGIN
    -- Логируем изменения поля name
    IF OLD.name IS DISTINCT FROM NEW.name THEN
        INSERT INTO users_audit (user_id, changed_by, field_changed, old_value, new_value)
        VALUES (OLD.id, current_user, 'name', OLD.name, NEW.name);
    END IF;

    -- Логируем изменения поля email
    IF OLD.email IS DISTINCT FROM NEW.email THEN
        INSERT INTO users_audit (user_id, changed_by, field_changed, old_value, new_value)
        VALUES (OLD.id, current_user, 'email', OLD.email, NEW.email);
    END IF;

    -- Логируем изменения поля role
    IF OLD.role IS DISTINCT FROM NEW.role THEN
        INSERT INTO users_audit (user_id, changed_by, field_changed, old_value, new_value)
        VALUES (OLD.id, current_user, 'role', OLD.role, NEW.role);
    END IF;

    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

-- 2. Создаем триггер на таблицу users
CREATE TRIGGER users_audit_trigger
    AFTER UPDATE ON users
    FOR EACH ROW
EXECUTE FUNCTION log_user_changes();

-- 3. Создаем функцию для экспорта свежих данных за вчерашний день
CREATE OR REPLACE FUNCTION export_yesterdays_audit()
    RETURNS void AS $$
DECLARE
    export_file_path TEXT;
    yesterday_date TEXT;
BEGIN
    -- Получаем дату вчерашнего дня в формате YYYYMMDD
    yesterday_date := to_char(CURRENT_DATE - INTERVAL '1 day', 'YYYYMMDD');

    -- Формируем путь к файлу
    export_file_path := '/tmp/users_audit_export_' || yesterday_date || '.csv';

    -- Экспортируем данные за вчерашний день в CSV
    EXECUTE format('
        COPY (
            SELECT
                user_id || ''.'' || field_changed as user_id_field_changed,
                old_value,
                new_value,
                changed_by,
                changed_at
            FROM users_audit
            WHERE changed_at >= CURRENT_DATE - INTERVAL ''1 day''
                AND changed_at < CURRENT_DATE
            ORDER BY changed_at
        ) TO %L WITH CSV HEADER', export_file_path);
END;
$$ LANGUAGE plpgsql;

-- 4. Устанавливаем планировщик pg_cron на 3:00 ночи
SELECT cron.schedule(
               'export-users-audit-daily',  -- имя задачи
               '0 3 * * *',                 -- расписание: каждый день в 3:00
               'SELECT export_yesterdays_audit();'  -- выполняемая функция
       );

-- Проверяем созданные cron задачи
SELECT * FROM cron.job;

-- 5. Создаем тестового пользователя, если его нет
INSERT INTO users (id, name, email, role)
VALUES (1, 'Alice', 'alice@example.com', 'test');


UPDATE users SET name = 'Alice Smith', email = 'alice.smith@example.com' WHERE id = 1;

-- 6. Проверяем таблицу аудита
SELECT * FROM users_audit ORDER BY changed_at DESC LIMIT 10;