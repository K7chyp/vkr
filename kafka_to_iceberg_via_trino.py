import json
import base64
from datetime import date, timedelta, datetime
from confluent_kafka import Consumer, KafkaError
from trino.dbapi import connect

# ---------- Конфигурация ----------
KAFKA_BROKER = "localhost:29092"
TOPIC = "dbserver1.public.currency_rates"
GROUP_ID = "python-trino-iceberg-consumer"

TRINO_HOST = "localhost"
TRINO_PORT = 8080
TRINO_USER = "trino"
TRINO_CATALOG = "iceberg"
TRINO_SCHEMA = "default"
TRINO_TABLE = "currency_rates"

BATCH_SIZE = 10_000  # количество записей для одной вставки

consumer = Consumer({
    'bootstrap.servers': KAFKA_BROKER,
    'group.id': GROUP_ID,
    'auto.offset.reset': 'earliest',
    'enable.auto.commit': False
})
consumer.subscribe([TOPIC])

conn = connect(
    host=TRINO_HOST,
    port=TRINO_PORT,
    user=TRINO_USER,
    catalog=TRINO_CATALOG,
    schema=TRINO_SCHEMA,
)
cur = conn.cursor()

def decode_decimal(base64_bytes, scale=4):
    if not base64_bytes:
        return 0.0
    raw = base64.b64decode(base64_bytes)
    int_val = int.from_bytes(raw, byteorder='big', signed=True)
    return int_val / (10 ** scale)

def date_from_epoch_days(days):
    if days is None:
        return date.today()
    epoch = date(1970, 1, 1)
    return epoch + timedelta(days=days)

print(f"Слушаем топик {TOPIC}, пишем батчами по {BATCH_SIZE} записей в {TRINO_CATALOG}.{TRINO_SCHEMA}.{TRINO_TABLE}...")

batch_messages = []  # список кортежей (kafka_message, after_dict)

try:
    while True:
        msg = consumer.poll(timeout=1.0)
        if msg is None:
            # если нет новых сообщений, но есть неполный батч, продолжаем ждать
            continue
        if msg.error():
            if msg.error().code() == KafkaError._PARTITION_EOF:
                continue
            else:
                print(f"Ошибка Kafka: {msg.error()}")
                break

        record = json.loads(msg.value().decode('utf-8'))
        payload = record.get('payload')
        if not payload:
            continue

        op = payload.get('op')
        if op not in ('c', 'r', 'u'):
            continue

        after = payload.get('after')
        if not after:
            continue

        batch_messages.append((msg, payload))

        if len(batch_messages) >= BATCH_SIZE:
            # формируем VALUES для многострочной вставки
            values_list = []
            for kafka_msg, pl in batch_messages:
                after_data = pl['after']
                char_code = after_data['char_code']
                nominal = int(after_data.get('nominal', 0))
                currency_name = after_data['currency_name']
                rate = decode_decimal(after_data.get('rate'), scale=4)
                date_int = after_data.get('date')
                parsed_date = date_from_epoch_days(date_int)

                values_list.append(
                    f"('{char_code}', {nominal}, '{currency_name}', {rate}, DATE '{parsed_date.isoformat()}')"
                )

            sql = f"INSERT INTO {TRINO_TABLE} (char_code, nominal, currency_name, rate, date) VALUES {', '.join(values_list)}"

            try:
                cur.execute(sql)
                conn.commit()
                # коммитим все оффсеты батча
                for kafka_msg, _ in batch_messages:
                    consumer.commit(kafka_msg)
                print(f"Вставлен батч из {len(batch_messages)} записей {datetime.now()}")
            except Exception as e:
                print(f"Ошибка вставки батча: {e}")
                # не коммитим, чтобы при следующем запуске батч обработался снова
            finally:
                batch_messages.clear()

except KeyboardInterrupt:
    print("Остановлено пользователем")
finally:
    # если остался неполный батч, можно дописать его здесь
    if batch_messages:
        values_list = []
        for kafka_msg, pl in batch_messages:
            after_data = pl['after']
            char_code = after_data['char_code']
            nominal = int(after_data.get('nominal', 0))
            currency_name = after_data['currency_name']
            rate = decode_decimal(after_data.get('rate'))
            date_int = after_data.get('date')
            parsed_date = date_from_epoch_days(date_int)
            values_list.append(
                f"('{char_code}', {nominal}, '{currency_name}', {rate}, DATE '{parsed_date.isoformat()}')"
            )
        sql = f"INSERT INTO {TRINO_TABLE} (char_code, nominal, currency_name, rate, date) VALUES {', '.join(values_list)}"
        try:
            cur.execute(sql)
            conn.commit()
            for kafka_msg, _ in batch_messages:
                consumer.commit(kafka_msg)
            print(f"Вставлен завершающий батч из {len(batch_messages)} записей")
        except Exception as e:
            print(f"Ошибка вставки завершающего батча: {e}")

    cur.close()
    conn.close()
    consumer.close()