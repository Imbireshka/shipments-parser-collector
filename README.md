# shipments-parser-collector
Параллельный парсер для сбора данных в корп. системах пунктов выдачи
Что умеет?
- Параллельный парсинг десятков инстансов
- Группировку по времени поступления
- Сохранение структурированных данных в PostgreSQL
- Пуш-уведомления через Телеграмм с возможностью подписки
- 
sql-запрос для БД (postgre)
- для вайтлиста
CREATE TABLE user_whitelist (
    chat_id BIGINT PRIMARY KEY
);

- сохранение подписок
CREATE TABLE user_subscriptions (
    chat_id BIGINT,
    location_name TEXT,
    time_slot TEXT,
    PRIMARY KEY (chat_id, location_name, time_slot)
);

- Данные о поставках
CREATE TABLE postavki (
    id SERIAL PRIMARY KEY,
    report_id TEXT NOT NULL,
    pvs_name TEXT NOT NULL,
    delivery_date DATE,
    created_at TIMESTAMP,
    unload_started_at TIMESTAMP,
    closed_at TIMESTAMP,
    status TEXT,
    sent INTEGER,
    received INTEGER,
    excess INTEGER,
    group_index INTEGER,
    boxes_count INTEGER,
    unload_duration_seconds INTEGER,
    inserted_at TIMESTAMP DEFAULT NOW()
);
