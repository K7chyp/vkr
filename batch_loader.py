#!/usr/bin/env python3
"""
Batch-скрипт инкрементальной загрузки из PostgreSQL в Iceberg (через Trino).
"""

import time
from datetime import datetime, timezone, date as dt_date
from pathlib import Path

import psycopg2
from trino.dbapi import connect as trino_connect

# ---------- Конфигурация ----------

DB_CONFIG = {
    "dbname": "cbrf_db",
    "user": "cbrf_user",
    "password": "cbrf_pass",
    "host": "localhost",
    "port": 5432
}

TRINO_HOST = "localhost"
TRINO_PORT = 8080
TRINO_USER = "trino"
TRINO_CATALOG = "iceberg"
TRINO_SCHEMA = "default"
TRINO_TABLE = "currency_rates"

CHECKPOINT_FILE = "last_checkpoint.txt"
POLL_INTERVAL = 10  # секунд между опросами
BATCH_SIZE = 1000    # максимальное количество строк в одном INSERT

# ---------- Вспомогательные функции ----------

def load_checkpoint() -> str:
    """Возвращает строку с максимальным parse_timestamp последней успешной загрузки."""
    try:
        with open(CHECKPOINT_FILE, "r") as f:
            cp = f.read().strip()
            if cp:
                return cp
    except FileNotFoundError:
        pass
    # если файла нет или он пуст – начинаем с эпохи
    return "1970-01-01T00:00:00+00:00"

def save_checkpoint(new_cp: str):
    """Сохраняет новую контрольную точку в файл."""
    with open(CHECKPOINT_FILE, "w") as f:
        f.write(new_cp)

def create_schema_and_table(cur_trino):
    """Создаёт схему и таблицу в Iceberg, если они ещё не существуют."""
    cur_trino.execute(f"CREATE SCHEMA IF NOT EXISTS {TRINO_CATALOG}.{TRINO_SCHEMA}")
    cur_trino.execute(f"""
        CREATE TABLE IF NOT EXISTS {TRINO_CATALOG}.{TRINO_SCHEMA}.{TRINO_TABLE} (
            id BIGINT,
            parse_timestamp TIMESTAMP(6) WITH TIME ZONE,
            num_code INTEGER,
            char_code VARCHAR(3),
            nominal INTEGER,
            currency_name VARCHAR(100),
            rate DECIMAL(10,4),
            date DATE
        )
        WITH (
            format = 'PARQUET',
            location = 's3://iceberg-warehouse/{TRINO_SCHEMA}/{TRINO_TABLE}'
        )
    """)
    # коммит не нужен для DDL в Trino

# ---------- Основной цикл ----------

def main():
    # Подключаемся к Trino и создаём окружение
    conn_trino = trino_connect(
        host=TRINO_HOST,
        port=TRINO_PORT,
        user=TRINO_USER,
        catalog=TRINO_CATALOG,
        schema=TRINO_SCHEMA,
    )
    cur_trino = conn_trino.cursor()
    create_schema_and_table(cur_trino)

    last_cp = load_checkpoint()
    print(f"Стартовая контрольная точка: {last_cp}")

    conn_pg = None
    while True:
        try:
            # Подключаемся к PostgreSQL на каждой итерации (или держим постоянное соединение)
            if conn_pg is None or conn_pg.closed:
                conn_pg = psycopg2.connect(**DB_CONFIG)

            cur_pg = conn_pg.cursor()
            # Выбираем все строки, чей parse_timestamp строго больше контрольной точки
            cur_pg.execute(
                "SELECT id, parse_timestamp, num_code, char_code, nominal, "
                "currency_name, rate, date FROM currency_rates "
                "WHERE parse_timestamp > %s::timestamptz "
                "ORDER BY parse_timestamp",
                (last_cp,)
            )
            rows = cur_pg.fetchall()
            cur_pg.close()

            if not rows:
                print("Нет новых данных, ожидание...")
                time.sleep(POLL_INTERVAL)
                continue

            # Определяем новую контрольную точку как максимальный parse_timestamp в выборке
            max_ts = max(row[1] for row in rows)  # row[1] — parse_timestamp
            # Преобразуем datetime в строку ISO с таймзоной
            new_cp = max_ts.isoformat()

            # Вставляем данные пакетами
            total_inserted = 0
            for start in range(0, len(rows), BATCH_SIZE):
                batch = rows[start:start+BATCH_SIZE]
                values_list = []
                for r in batch:
                    id_val = r[0]
                    # r[1] – datetime с таймзоной, преобразуем в строку без 'T'
                    ts_raw = r[1].isoformat()
                    ts = ts_raw.replace('T', ' ')  # Trino требует пробел
                    num = r[2] if r[2] is not None else "NULL"
                    char = r[3]
                    nom = r[4] if r[4] is not None else "NULL"
                    name = r[5].replace("'", "''") if r[5] else ""
                    rate_val = float(r[6]) if r[6] else 0.0
                    d = r[7].isoformat()  # date – ok

                    values_list.append(
                        f"({id_val}, TIMESTAMP '{ts}', {num}, '{char}', "
                        f"{nom}, '{name}', {rate_val}, DATE '{d}')"
                    )

                sql = (
                    f"INSERT INTO {TRINO_TABLE} "
                    f"(id, parse_timestamp, num_code, char_code, nominal, currency_name, rate, date) "
                    f"VALUES {', '.join(values_list)}"
                )
                cur_trino.execute(sql)
                # Trino commit не требуется для INSERT, он авто-коммитит, но можно вызвать для ясности
                # conn_trino.commit()  # в dbapi обычно нет, вызовем cur_trino.fetchall() если надо
                total_inserted += len(batch)
                print(f"Вставлен батч из {len(batch)} записей")

            # После успешной вставки всей партии обновляем контрольную точку
            save_checkpoint(new_cp)
            last_cp = new_cp
            print(f"Контрольная точка обновлена: {new_cp} (вставлено {total_inserted} записей)")

            time.sleep(POLL_INTERVAL)

        except KeyboardInterrupt:
            print("Остановка batch-загрузчика пользователем")
            break
        except Exception as e:
            print(f"Ошибка: {e}")
            if conn_pg:
                conn_pg.rollback()
            time.sleep(POLL_INTERVAL)

    # Закрываем ресурсы
    cur_trino.close()
    conn_trino.close()
    if conn_pg and not conn_pg.closed:
        conn_pg.close()

if __name__ == "__main__":
    main()