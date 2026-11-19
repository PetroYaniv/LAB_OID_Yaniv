import os
import time
import polars as pl

CSV_FILE = r"D:\Магістр\1курс\Машинне навчання\Crimes_-_2001_to_Present.csv"
PARQUET_FILE = r"D:\Магістр\1курс\Машинне навчання\Crimes_2001_to_Present.parquet"


def convert_csv_to_parquet(csv_path: str, parquet_path: str):
    """Конвертація CSV в Parquet зі стисненням Snappy."""
    print("Converting CSV to Parquet...")
    start_time = time.time()
    lazy_df = pl.scan_csv(csv_path, ignore_errors=True)
    lazy_df.sink_parquet(parquet_path, compression="snappy")
    print(f"Conversion completed in {time.time() - start_time:.2f} seconds.")


def compare_file_sizes(csv_path: str, parquet_path: str):
    """Порівняння розмірів файлів на диску."""
    csv_size_mb = os.path.getsize(csv_path) / (1024 * 1024)
    parquet_size_mb = os.path.getsize(parquet_path) / (1024 * 1024)

    print("\n" + "=" * 50)
    print(" 1. FILE SIZE COMPARISON")
    print("=" * 50)
    print(f"CSV Size:     {csv_size_mb:.2f} MB ({csv_size_mb / 1024:.2f} GB)")
    print(f"Parquet Size: {parquet_size_mb:.2f} MB ({parquet_size_mb / 1024:.2f} GB)")
    print(f"Compression Ratio: {csv_size_mb / parquet_size_mb:.2f}x smaller")


def run_benchmark(query_name: str, csv_lazy, parquet_lazy):
    """Вспоміжна функція для вимірювання часу виконання та виводу порівняння."""
    print("\n" + "=" * 50)
    print(f" BENCHMARK: {query_name}")
    print("=" * 50)

    # Виконання запиту для CSV
    t0 = time.time()
    _ = csv_lazy.collect()
    csv_time = time.time() - t0
    print(f"CSV Time:     {csv_time:.4f} seconds")

    # Виконання запиту для Parquet
    t0 = time.time()
    _ = parquet_lazy.collect()
    parquet_time = time.time() - t0
    print(f"Parquet Time: {parquet_time:.4f} seconds")

    speedup = csv_time / parquet_time if parquet_time > 0 else 0
    print(f"-> Parquet is {speedup:.2f}x faster")


def benchmark_comprehensive_queries(csv_path: str, parquet_path: str):
    """Набір із 4 тестових запитів різної складності та специфіки."""
    
    # -------------------------------------------------------------
    # ЗАПИТ 1: Колоночна агрегація з фільтрацією (OLAP-сценарій)
    # Зчитуються лише 3 колонки з 22+
    # -------------------------------------------------------------
    q1_csv = (
        pl.scan_csv(csv_path, ignore_errors=True)
        .filter(pl.col("Arrest") == "true")
        .select(["Primary Type", "District", "Arrest"])
        .group_by(["Primary Type", "District"])
        .len()
    )
    q1_parquet = (
        pl.scan_parquet(parquet_path)
        .filter(pl.col("Arrest") == True)
        .select(["Primary Type", "District", "Arrest"])
        .group_by(["Primary Type", "District"])
        .len()
    )
    run_benchmark("1. Selective Aggregation (3 columns only)", q1_csv, q1_parquet)

    # -------------------------------------------------------------
    # ЗАПИТ 2: Точкова пошукова фільтрація (Point Lookup / Filtering)
    # Фільтрація по конкретному ID / Case Number
    # -------------------------------------------------------------
    q2_csv = (
        pl.scan_csv(csv_path, ignore_errors=True)
        .filter(pl.col("Primary Type") == "HOMICIDE")
    )
    q2_parquet = (
        pl.scan_parquet(parquet_path)
        .filter(pl.col("Primary Type") == "HOMICIDE")
    )
    run_benchmark("2. High-Selectivity Filter (Rows matching HOMICIDE)", q2_csv, q2_parquet)

    # -------------------------------------------------------------
    # ЗАПИТ 3: Комплексна часова та просторова аналітика
    # Парсинг дат, створення нових колонок, агрегації
    # -------------------------------------------------------------
    q3_csv = (
        pl.scan_csv(csv_path, ignore_errors=True)
        .select(["Date", "Primary Type", "Domestic"])
        .with_columns(
            pl.col("Date").str.to_datetime("%m/%d/%Y %I:%M:%S %p", strict=False).dt.year().alias("Year_Parsed")
        )
        .filter(pl.col("Year_Parsed") >= 2015)
        .group_by(["Year_Parsed", "Primary Type"])
        .agg([
            pl.len().alias("total_incidents"),
            pl.col("Domestic").alias("domestic_count")
        ])
    )
    q3_parquet = (
        pl.scan_parquet(parquet_path)
        .select(["Date", "Primary Type", "Domestic"])
        .with_columns(
            pl.col("Date").str.to_datetime("%m/%d/%Y %I:%M:%S %p", strict=False).dt.year().alias("Year_Parsed")
        )
        .filter(pl.col("Year_Parsed") >= 2015)
        .group_by(["Year_Parsed", "Primary Type"])
        .agg([
            pl.len().alias("total_incidents"),
            pl.col("Domestic").alias("domestic_count")
        ])
    )
    run_benchmark("3. Time-Series Aggregation & Date Parsing (2015+)", q3_csv, q3_parquet)

    # -------------------------------------------------------------
    # ЗАПИТ 4: Сканування практично всіх колонок (Full Table Scan)
    # Зчитування більшості полів та сортування
    # -------------------------------------------------------------
    q4_csv = (
        pl.scan_csv(csv_path, ignore_errors=True)
        .select(["ID", "Case Number", "Date", "Block", "Primary Type", "Description", "Location Description", "Arrest"])
        .sort("ID", descending=True)
        .head(100_000)
    )
    q4_parquet = (
        pl.scan_parquet(parquet_path)
        .select(["ID", "Case Number", "Date", "Block", "Primary Type", "Description", "Location Description", "Arrest"])
        .sort("ID", descending=True)
        .head(100_000)
    )
    run_benchmark("4. Heavy Scan & Sort (Reading 8 columns + Sorting 100k rows)", q4_csv, q4_parquet)


def main():
    if not os.path.exists(CSV_FILE):
        print(f"Error: CSV file not found: {CSV_FILE}")
        return

    if not os.path.exists(PARQUET_FILE):
        convert_csv_to_parquet(CSV_FILE, PARQUET_FILE)

    compare_file_sizes(CSV_FILE, PARQUET_FILE)
    benchmark_comprehensive_queries(CSV_FILE, PARQUET_FILE)


if __name__ == "__main__":
    main()