CREATE TABLE parse_log (
    parse_timestamp TIMESTAMPTZ PRIMARY KEY,
    parsed_date     DATE NOT NULL,            -- дата, на которую получены курсы
    inserted_at     TIMESTAMPTZ DEFAULT now()
);


CREATE TABLE currency_rates (
    id               BIGSERIAL,
    parse_timestamp  TIMESTAMPTZ NOT NULL,    -- ссылка на парсинг
    num_code         INT,
    char_code        VARCHAR(3) NOT NULL,
    nominal          INT,
    currency_name    VARCHAR(100),
    rate             NUMERIC(10,4),
    date             DATE NOT NULL,
    PRIMARY KEY (id, date)
) PARTITION BY RANGE (date);

-- Индексы для быстрого поиска
CREATE INDEX idx_rates_code_date ON currency_rates (char_code, date);
CREATE INDEX idx_rates_parse ON currency_rates (parse_timestamp);