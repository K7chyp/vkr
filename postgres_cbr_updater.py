import requests
import pandas as pd
from datetime import datetime, timezone, date as dt_date
import xml.etree.ElementTree as ET
import psycopg2
import psycopg2.extras
from io import StringIO
from datetime import date, timedelta
import time

DB_CONFIG = {
    "dbname": "cbrf_db",
    "user": "cbrf_user",
    "password": "cbrf_pass",
    "host": "localhost",
    "port": 5432
}


def generate_date_strings(start: date, end: date):
    """
    Генерирует строки дат в формате ДД/ММ/ГГГГ от start до end включительно.
    """
    current = start
    while current <= end:
        yield current.strftime("%d/%m/%Y")
        current += timedelta(days=1)


class CBRCurrencyLoader:
    """
    Загружает официальные курсы валют ЦБ РФ за указанные даты в PostgreSQL
    с автоматическим партиционированием по месяцам.
    """

    def __init__(self, db_config: dict):
        """
        Параметры
        ---------
        db_config : dict
            Ключи: dbname, user, password, host, port
        """
        self.db_config = db_config
        self.conn = None
        self.cursor = None

    def connect(self):
        """Открываем соединение с БД."""
        self.conn = psycopg2.connect(**self.db_config)
        self.cursor = self.conn.cursor()

    def close(self):
        """Закрываем соединение."""
        if self.cursor:
            self.cursor.close()
        if self.conn:
            self.conn.close()

    def _ensure_monthly_partition(self, target_date: dt_date):
        """
        Проверяет, существует ли партиция для месяца, к которому относится target_date.
        Если нет – создаёт её.
        """
        start_date = target_date.replace(day=1)
        # Определяем следующий месяц как границу < end_date
        year = start_date.year + (start_date.month // 12)
        month = start_date.month % 12 + 1
        end_date = start_date.replace(year=year, month=month)

        partition_name = f"currency_rates_{start_date.strftime('%Y_%m')}"

        # Проверяем существование таблицы-партиции
        self.cursor.execute(
            "SELECT 1 FROM pg_class WHERE relname = %s AND relkind = 'r'",
            (partition_name,)
        )
        if self.cursor.fetchone():
            return  # уже есть

        # Создаём партицию
        self.cursor.execute(f"""
            CREATE TABLE IF NOT EXISTS {partition_name} PARTITION OF currency_rates
            FOR VALUES FROM ('{start_date}') TO ('{end_date}')
        """)
        self.conn.commit()

    def fetch_and_load_date(self, date_str: str):
        """
        Парсит курсы с cbr.ru на дату date_str (ДД/ММ/ГГГГ) и загружает в БД.
        Возвращает True, если загрузка выполнена, иначе False (например, дубликат).
        """
        # 1. HTTP-запрос к API ЦБ
        url = f"https://www.cbr.ru/scripts/XML_daily.asp?date_req={date_str}"
        resp = requests.get(url)
        resp.encoding = 'windows-1251'
        xml_text = resp.text

        # 2. Официальная дата из атрибута <ValCurs Date="...">
        root = ET.fromstring(xml_text)
        date_attr = root.attrib.get('Date')  # "04.05.2026"
        official_date = datetime.strptime(date_attr, '%d.%m.%Y').date()

        # 3. Парсим в DataFrame (через StringIO, чтобы избежать FutureWarning)
        df = pd.read_xml(StringIO(xml_text), encoding='windows-1251')

        # 4. Приводим поля к нужным типам и переименовываем
        df['date'] = pd.to_datetime(official_date)
        df['rate'] = df['Value'].str.replace(',', '.').astype(float)
        df = df.rename(columns={
            'NumCode': 'num_code',
            'CharCode': 'char_code',
            'Nominal': 'nominal',
            'Name': 'currency_name'
        })

        # 5. Генерируем уникальный таймстемп парсинга (UTC)
        parse_ts = datetime.now(timezone.utc)

        try:
            # 6. Регистрируем партию в parse_log (защита от дублирования)
            self.cursor.execute(
                """INSERT INTO parse_log (parse_timestamp, parsed_date)
                   VALUES (%s, %s) ON CONFLICT (parse_timestamp) DO NOTHING
                   RETURNING parse_timestamp""",
                (parse_ts, official_date)
            )
            inserted_ts = self.cursor.fetchone()

            if not inserted_ts:
                print(f"Партия с таймстемпом {parse_ts} уже существует. Пропускаем.")
                self.conn.rollback()
                return False

            # 7. Убеждаемся, что есть партиция для нужного месяца
            self._ensure_monthly_partition(official_date)

            # 8. Формируем список записей для вставки
            records = [
                (
                    parse_ts,
                    row['num_code'],
                    row['char_code'],
                    row['nominal'],
                    row['currency_name'],
                    row['rate'],
                    row['date'].strftime('%Y-%m-%d')
                )
                for _, row in df.iterrows()
            ]

            # 9. Вставляем все валюты за одну операцию
            insert_query = """
                           INSERT INTO currency_rates
                           (parse_timestamp, num_code, char_code, nominal, currency_name, rate, date)
                           VALUES %s \
                           """
            psycopg2.extras.execute_values(self.cursor, insert_query, records)
            self.conn.commit()
            print(f"✓ {date_str} — загружено {len(records)} валют (ts={parse_ts})")
            return True

        except Exception as e:
            self.conn.rollback()
            print(f"Ошибка при загрузке {date_str}: {e}")
            return False

    def load_dates(self, date_list: list[str]):
        """
        Загружает курсы для списка дат (каждая в формате ДД/ММ/ГГГГ).
        """
        self.connect()
        try:
            for date_str in date_list:
                self.fetch_and_load_date(date_str)
        finally:
            self.close()


loader = CBRCurrencyLoader(DB_CONFIG)
while True:
    try:
        dates_to_fetch = list(generate_date_strings(
            start=date(2026, 1, 1),
            end=date(2026, 5, 4)
        ))
        loader.load_dates(dates_to_fetch)
    except Exception as e:
        print(e)
        time.sleep(10)
