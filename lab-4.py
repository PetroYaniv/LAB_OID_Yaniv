import json
import random
import time
import hashlib
import pandas as pd
import numpy as np
from datetime import datetime, timezone
from typing import Dict, Any, List, Tuple


# =====================================================================
# 1. ГЕНЕРАТОР НЕНАДІЙНИХ ДАНИХ (Симуляція джерела відмов)
# =====================================================================

class FaultySensorGenerator:
    """Генерує потік даних від датчиків із навмисними помилками."""

    def __init__(self, failure_rate: float = 0.3):
        self.failure_rate = failure_rate
        self.sensor_ids = [f"SENSOR_{i:03d}" for i in range(1, 6)]
        self._generated_cache = []  # Для створення дублікатів

    def generate_batch(self, batch_size: int = 50) -> List[str]:
        batch = []
        for _ in range(batch_size):
            # Введення дублікатів
            if self._generated_cache and random.random() < 0.15:
                raw_record = random.choice(self._generated_cache)
                batch.append(raw_record)
                continue

            # Нормальні дані
            record = {
                "event_id": f"evt_{random.randint(10000, 99999)}",
                "sensor_id": random.choice(self.sensor_ids),
                "timestamp": datetime.now(timezone.utc).isoformat(),
                "temperature": round(random.uniform(18.0, 35.0), 2),
                "humidity": round(random.uniform(40.0, 80.0), 2)
            }

            # Введення аномалій та помилок (Failure Injection)
            if random.random() < self.failure_rate:
                error_type = random.choice([
                    "corrupted_json",
                    "missing_field",
                    "invalid_type",
                    "extreme_outlier"
                ])

                if error_type == "corrupted_json":
                    # Пошкоджена синтаксична структура JSON
                    raw_str = json.dumps(record) + "}}--BAD_DATA"
                    batch.append(raw_str)
                    continue

                elif error_type == "missing_field":
                    # Видалення обов'язкового поля
                    del record[random.choice(["sensor_id", "timestamp"])]

                elif error_type == "invalid_type":
                    # Некоректний тип даних (рядок замість числа)
                    record["temperature"] = "CRITICAL_HIGH"

                elif error_type == "extreme_outlier":
                    # Аномальні недопустимі значення датчика
                    record["temperature"] = 999.99

            raw_str = json.dumps(record)
            self._generated_cache.append(raw_str)
            batch.append(raw_str)

        return batch


# =====================================================================
# 2. ВІДМОВОСТІЙКИЙ КОНВЕЄР ДАНИХ (Pipeline Core)
# =====================================================================

class FaultTolerantPipeline:
    """Конвеєр з валідацією, підтримкою ідемпотентності та Dead Letter Queue."""

    def __init__(self):
        self.processed_hashes = set()  # Хеші оброблених записів (Захист від дублікатів)
        self.valid_records: List[Dict[str, Any]] = []
        self.dead_letter_queue: List[Dict[str, Any]] = []  # DLQ для пошкоджених даних

    def _generate_record_hash(self, record: Dict[str, Any]) -> str:
        """Створює детермінований хеш запису для забезпечення ідемпотентності."""
        unique_str = f"{record.get('event_id')}_{record.get('sensor_id')}_{record.get('timestamp')}"
        return hashlib.sha256(unique_str.encode('utf-8')).hexdigest()

    def _validate_record(self, record: Dict[str, Any]) -> Tuple[bool, str]:
        """Схема валідації та перевірка межевих значень."""
        required_fields = ["event_id", "sensor_id", "timestamp", "temperature", "humidity"]
        for field in required_fields:
            if field not in record:
                return False, f"Відсутнє обов'язкове поле: {field}"

        # Перевірка типів даних
        try:
            temp = float(record["temperature"])
            hum = float(record["humidity"])
        except (ValueError, TypeError):
            return False, "Неможливо привести temperature/humidity до float"

        # Перевірка фізично допустимих діапазонів
        if not (-50.0 <= temp <= 100.0):
            return False, f"Температура поза допустимим діапазоном: {temp}"

        if not (0.0 <= hum <= 100.0):
            return False, f"Вологість поза допустимим діапазоном: {hum}"

        return True, "OK"

    def process_batch(self, raw_batch: List[str]):
        """Основний метод обробки пакунка записів."""
        for raw_item in raw_batch:
            # 1. Спроба парсингу JSON
            try:
                data = json.loads(raw_item)
            except Exception as e:
                self.dead_letter_queue.append({
                    "raw_data": raw_item,
                    "error_reason": f"Помилка парсингу JSON: {str(e)}",
                    "failed_at": datetime.now(timezone.utc).isoformat()
                })
                continue

            # 2. Перевірка ідемпотентності (Ігнорування дублікатів)
            rec_hash = self._generate_record_hash(data)
            if rec_hash in self.processed_hashes:
                # Дублікат виявлено та відхилено без помилки в системі
                continue

            # 3. Валідація бізнес-правил
            is_valid, reason = self._validate_record(data)
            if not is_valid:
                self.dead_letter_queue.append({
                    "raw_data": data,
                    "error_reason": reason,
                    "failed_at": datetime.now(timezone.utc).isoformat()
                })
                continue

            # 4. Успішна обробка та фіксація хешу
            self.processed_hashes.add(rec_hash)
            self.valid_records.append(data)


# =====================================================================
# 3. ЗАПУСК ТА АНАЛІЗ РЕЗУЛЬТАТІВ (EDA & Monitoring)
# =====================================================================

def main():
    print("=" * 65)
    print("ЗАПУСК ВІДМОВОСТІЙКОГО КОНВЕЄРА ДАНИХ (FAULT-TOLERANT PIPELINE)")
    print("=" * 65)

    # Ініціалізація компонентів
    generator = FaultySensorGenerator(failure_rate=0.35)
    pipeline = FaultTolerantPipeline()

    # Симуляція потокової обробки (3 батчі)
    total_raw_count = 0
    for i in range(1, 4):
        batch = generator.generate_batch(batch_size=40)
        total_raw_count += len(batch)
        print(f"-> Отримано Батч #{i} ({len(batch)} записів)...")
        pipeline.process_batch(batch)
        time.sleep(0.1)

    # Формування підсумкових DataFrames
    df_valid = pd.DataFrame(pipeline.valid_records)
    df_dlq = pd.DataFrame(pipeline.dead_letter_queue)

    print("\n" + "=" * 65)
    print("СТАТИСТИКА РОБОТИ ТА СТІЙКОСТІ СИСТЕМИ")
    print("=" * 65)
    print(f"Усього отримано сирих записів:   {total_raw_count}")
    print(f"Успішно оброблено (Valid):        {len(df_valid)}")
    print(f"Відправлено в DLQ (Bad/Failed):   {len(df_dlq)}")
    print(f"Відфільтровано дублікатів:       {total_raw_count - len(df_valid) - len(df_dlq)}")

    print("\n--- Приклад пошкоджених записів у Dead Letter Queue (DLQ) ---")
    if not df_dlq.empty:
        print(df_dlq[['error_reason', 'raw_data']].head(5).to_string(index=False))

    print("\n--- Первинна описова статистика валідних даних ---")
    if not df_valid.empty:
        print(df_valid[['temperature', 'humidity']].describe().T)

    # Збереження результатів у файли
    df_valid.to_csv("clean_sensor_data.csv", index=False)
    df_dlq.to_csv("dead_letter_queue.csv", index=False)
    print("\n[УСПІХ] Валідні дані збережено в 'clean_sensor_data.csv'")
    print("[УСПІХ] Помилкові дані (DLQ) збережено в 'dead_letter_queue.csv'")


if __name__ == "__main__":
    main()