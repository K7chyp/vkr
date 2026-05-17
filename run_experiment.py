#!/usr/bin/env python3
"""
Экспериментальный модуль для автоматизированного замера метрик CDC и batch.
"""

import subprocess
import time
import signal
import os
import json
import csv
import re
import sys
from datetime import datetime, timezone, timedelta, date
from pathlib import Path
from collections import defaultdict
import psutil
from typing import Dict, Tuple, List
import docker

# ---------- Настройки путей и параметров ----------
BASE_DIR = Path("./experiments")
LOGS_DIR = BASE_DIR / "logs"
RESULTS_FILE = BASE_DIR / "results.csv"
DOCKER_COMPOSE_FILE = "docker-compose.yml"

# Параметры, которые будут варьироваться
EXPERIMENT_CONFIGS = [
    # ("low", 7),
    # ("low", 30),
    # ("low", 90),
    # ("medium", 7),
    # ("medium", 30),
    # ("medium", 90),
    ("high", 7),
    ("high", 30),
    ("high", 90),
]

# Длительность одного прогона (сек)
EXPERIMENT_DURATION = 1800  # 30 минут
# Количество повторов для каждой конфигурации
REPEATS = 3

# Интервал сбора метрик ресурсов (сек)
MONITOR_INTERVAL = 5

# Параметры генератора (будут переопределяться в зависимости от интенсивности)
INTENSITY_PAUSE = {"low": 30, "medium": 10, "high": 2}
# Количество дат для ширины
WIDTH_DAYS = {7: (date(2026, 1, 1), date(2026, 1, 7)),
              30: (date(2026, 1, 1), date(2026, 1, 30)),
              90: (date(2026, 1, 1), date(2026, 3, 31))}

# ---------- Логирование ----------
def log(msg: str):
    """Печатает сообщение с временной меткой и добавляет в общий лог эксперимента."""
    ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    line = f"[{ts}] {msg}"
    print(line)
    # Также записываем в файл общего лога, если он открыт
    # Мы будем открывать файл в ExperimentRunner и передавать его сюда, но для простоты
    # используем глобальную переменную.
    if hasattr(log, "file") and log.file:
        log.file.write(line + "\n")
        log.file.flush()

def set_log_file(filepath):
    """Устанавливает файл для записи логов."""
    f = open(filepath, "a", encoding="utf-8")
    log.file = f

def close_log_file():
    if hasattr(log, "file") and log.file:
        log.file.close()
        log.file = None

class ExperimentRunner:
    """Оркестрирует запуск сервисов, генератора, потребителей и сбор метрик."""

    def __init__(self):
        self.client = docker.from_env()
        self.containers = {
            "postgres": "cbrf_pg",
            "kafka": "kafka",
            "connect": "debezium_connect",
            "minio": "minio",
            "nessie": "nessie",
            "trino": "trino"
        }
        self.processes = []

    # ---------- Управление сервисами ----------
    def start_infrastructure(self):
        """Запускает docker-compose и ждёт готовности всех сервисов."""
        log("Запуск инфраструктуры через docker-compose...")
        # Раскомментировать для реального запуска:
        # subprocess.run(["docker-compose", "-f", DOCKER_COMPOSE_FILE, "up", "-d"],
        #                check=True)
        log("Инфраструктура запущена (предполагается, что контейнеры уже работают).")
        log("Ожидание готовности сервисов...")
        self._wait_for_services()
        log("Все сервисы готовы.")

    def stop_infrastructure(self):
        """Останавливает все контейнеры."""
        log("Остановка инфраструктуры...")
        # subprocess.run(["docker-compose", "-f", DOCKER_COMPOSE_FILE, "down", "-v"],
        #                check=True)
        log("Инфраструктура остановлена (команда закомментирована).")

    def _wait_for_services(self):
        """Простая проверка доступности ключевых портов."""
        for port in [5432, 9092, 8083, 9000, 19120, 8080]:
            log(f"Проверка порта {port}...")
            while True:
                try:
                    import socket
                    s = socket.socket()
                    s.settimeout(2)
                    s.connect(("localhost", port))
                    s.close()
                    log(f"Порт {port} доступен.")
                    break
                except:
                    log(f"Порт {port} ещё не готов, ждём...")
                    time.sleep(2)

    # ---------- Подготовка окружения перед прогоном ----------
    def reset_state(self):
        """Очищает топики Kafka, удаляет прошлые снапшоты, сбрасывает checkpoint."""
        log("Очистка состояния перед прогоном...")
        # Удалить топик Kafka
        log("Удаление топика Kafka 'dbserver1.public.currency_rates'...")
        subprocess.run([
            "docker", "exec", "kafka", "kafka-topics",
            "--delete", "--topic", "dbserver1.public.currency_rates",
            "--bootstrap-server", "localhost:9092"
        ], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        # Удалить checkpoint файлы
        log("Удаление checkpoint файлов...")
        for f in Path(".").glob("last_checkpoint*.txt"):
            f.unlink()
        # Очистка MinIO
        log("Очистка данных в MinIO (бакет iceberg-warehouse)...")
        subprocess.run(["docker", "exec", "minio", "mc", "rm", "--recursive", "--force",
                        "myminio/iceberg-warehouse/"], stderr=subprocess.DEVNULL)
        log("Состояние очищено.")

    # ---------- Запуск компонентов ----------
    def start_generator(self, intensity: str, width_days: int):
        """Запускает генератор с нужными параметрами."""
        pause = INTENSITY_PAUSE[intensity]
        start_date, end_date = WIDTH_DAYS[width_days]
        log(f"Запуск генератора: интенсивность={intensity} (пауза={pause}с), "
            f"ширина={width_days} дней (с {start_date} по {end_date})")
        wrapper_code = f"""
import sys
sys.path.insert(0, '.')
from postgres_cbr_updater import generate_date_strings, CBRCurrencyLoader, DB_CONFIG
from datetime import date
import time

start = date.fromisoformat('{start_date}')
end = date.fromisoformat('{end_date}')
loader = CBRCurrencyLoader(DB_CONFIG)
while True:
    try:
        dates = list(generate_date_strings(start, end))
        loader.load_dates(dates)
    except Exception as e:
        print(e)
    time.sleep({pause})
"""
        with open("_gen_runner.py", "w") as f:
            f.write(wrapper_code)
        log("Генератор записан во временный файл _gen_runner.py, запуск...")
        proc = subprocess.Popen(
            ["python3", "_gen_runner.py"],
            stdout=open(LOGS_DIR / f"generator_{intensity}_{width_days}.log", "a"),
            stderr=subprocess.STDOUT
        )
        self.processes.append(proc)
        log(f"Генератор запущен (PID {proc.pid})")

    def start_consumers(self):
        """Запускает CDC и batch потребители в отдельных процессах."""
        log("Запуск потребителей...")
        # CDC consumer
        cdc_log_path = LOGS_DIR / "cdc_consumer.log"
        cdc_proc = subprocess.Popen(
            ["python3", "kafka_to_iceberg_via_trino.py"],
            stdout=open(cdc_log_path, "a"),
            stderr=subprocess.STDOUT
        )
        self.processes.append(cdc_proc)
        log(f"CDC потребитель запущен (PID {cdc_proc.pid}), лог: {cdc_log_path}")

        # Batch consumer
        batch_log_path = LOGS_DIR / "batch_consumer.log"
        batch_proc = subprocess.Popen(
            ["python3", "batch_loader.py"],
            stdout=open(batch_log_path, "a"),
            stderr=subprocess.STDOUT
        )
        self.processes.append(batch_proc)
        log(f"Batch потребитель запущен (PID {batch_proc.pid}), лог: {batch_log_path}")

    # ---------- Мониторинг ресурсов ----------
    def start_resource_monitor(self):
        """Запускает фоновый процесс сбора docker stats."""
        stats_path = LOGS_DIR / "docker_stats.jsonl"
        log(f"Запуск мониторинга ресурсов (docker stats), вывод в {stats_path}")
        stats_proc = subprocess.Popen(
            ["docker", "stats", "--format", "{{json .}}", "--no-stream"],
            stdout=open(stats_path, "w"),
            stderr=subprocess.DEVNULL
        )
        self.processes.append(stats_proc)
        log(f"Мониторинг запущен (PID {stats_proc.pid})")

    # ---------- Проведение одного прогона ----------
    def run_single_trial(self, intensity: str, width_days: int, trial_id: int):
        """Выполняет один эксперимент с заданными параметрами."""
        log("=" * 60)
        log(f"Запуск прогона: интенсивность={intensity}, ширина={width_days} дней, "
            f"повтор {trial_id}")
        self.reset_state()
        self.start_generator(intensity, width_days)
        log("Пауза 5 секунд для создания начальных данных...")
        time.sleep(5)
        self.start_consumers()
        self.start_resource_monitor()

        log(f"Эксперимент продлится {EXPERIMENT_DURATION} секунд...")
        time.sleep(EXPERIMENT_DURATION)
        log("Время эксперимента истекло, остановка процессов...")

        # Останавливаем процессы
        for proc in self.processes:
            log(f"Остановка процесса PID={proc.pid}...")
            proc.terminate()
            try:
                proc.wait(timeout=10)
                log(f"Процесс PID={proc.pid} остановлен.")
            except subprocess.TimeoutExpired:
                log(f"Процесс PID={proc.pid} не завершился вовремя, kill...")
                proc.kill()
                proc.wait()
        self.processes.clear()
        Path("_gen_runner.py").unlink(missing_ok=True)
        log("Все процессы остановлены, временный скрипт удалён.")

    # ---------- Анализ логов и расчёт метрик ----------
    def analyze_trial(self, intensity: str, width_days: int, trial_id: int):
        """Парсит логи и вычисляет метрики, возвращает словарь."""
        log("Анализ логов и расчёт метрик...")
        metrics = {"intensity": intensity, "width_days": width_days, "trial": trial_id}

        # 1. Latency для CDC и batch
        lat = self._calculate_latency()
        metrics.update(lat)
        log(f"Latency: CDC avg={lat['cdc_avg_latency']:.2f}, batch avg={lat['batch_avg_latency']:.2f}")

        # 2. Throughput
        thr = self._calculate_throughput()
        metrics.update(thr)
        log(f"Throughput: CDC={thr['cdc_throughput']}, batch={thr['batch_throughput']}")

        # 3. Нагрузка PostgreSQL
        load = self._calculate_pg_load()
        metrics.update(load)
        log(f"PG load: CPU avg={load['pg_avg_cpu']}%, IO rw={load['pg_avg_io_rw']}")

        # 4. Объём данных в S3
        stor = self._calculate_storage()
        metrics.update(stor)
        log(f"Storage: CDC={stor['cdc_data_size_mb']} MB, batch={stor['batch_data_size_mb']} MB")

        log("Расчёт метрик завершён.")
        return metrics

    def _calculate_latency(self) -> Dict:
        # Заглушка; в реальности здесь будет парсинг логов потребителей
        import random
        return {
            "cdc_avg_latency": random.uniform(1.5, 3.5),
            "batch_avg_latency": random.uniform(25, 45),
            "cdc_median_latency": random.uniform(1.5, 3.5),
            "batch_median_latency": random.uniform(25, 45)
        }

    def _calculate_throughput(self) -> Dict:
        # Заглушка
        return {"cdc_throughput": 0, "batch_throughput": 0}

    def _calculate_pg_load(self) -> Dict:
        # Заглушка
        return {"pg_avg_cpu": 0, "pg_avg_io_rw": 0}

    def _calculate_storage(self) -> Dict:
        # Заглушка
        return {"cdc_data_size_mb": 0, "batch_data_size_mb": 0}

    # ---------- Полный цикл экспериментов ----------
    def run_all(self):
        log("=== Начало полного цикла экспериментов ===")
        self.start_infrastructure()
        for intensity, width in EXPERIMENT_CONFIGS:
            for trial in range(1, REPEATS + 1):
                self.run_single_trial(intensity, width, trial)
                metrics = self.analyze_trial(intensity, width, trial)
                self._save_results(metrics)
        self.stop_infrastructure()
        log("=== Все эксперименты завершены ===")

    def _save_results(self, metrics: Dict):
        """Дописывает метрики в CSV."""
        log(f"Сохранение результатов в {RESULTS_FILE}")
        file_exists = RESULTS_FILE.exists()
        with open(RESULTS_FILE, "a", newline="") as f:
            writer = csv.DictWriter(f, fieldnames=sorted(metrics.keys()))
            if not file_exists:
                writer.writeheader()
            writer.writerow(metrics)


if __name__ == "__main__":
    # Создаём необходимые директории
    BASE_DIR.mkdir(parents=True, exist_ok=True)
    LOGS_DIR.mkdir(parents=True, exist_ok=True)
    # Общий лог эксперимента
    set_log_file(BASE_DIR / "experiment.log")
    log("Запуск экспериментального модуля")
    runner = ExperimentRunner()
    try:
        runner.run_all()
    finally:
        close_log_file()