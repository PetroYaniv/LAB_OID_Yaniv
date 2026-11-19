import sqlite3
import time
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns

# -------------------------------------------------------------------
# 1. ВІЗУАЛІЗАЦІЯ СТРУКТУРИ СХЕМ (ДІАГРАМИ)
# -------------------------------------------------------------------

def draw_schema_diagrams():
    """Малює наочні текстово-графічні структури схем у консолі."""
    star_structure = """
    ========================================================================
                          СХЕМА «ЗІРКА» (STAR SCHEMA)
    ========================================================================

                             +-----------------------+
                             |   dim_category_star   |
                             +-----------------------+
                             | PK | category_id      |
                             |    | category_name    |
                             |    | pct_share        |
                             +-----------------------+
                                         | 1
                                         |
                                         | N
     +-------------------+   N       +-------------------+
     |   dim_date_star   |-----------| fact_youtube_star |
     +-------------------+           +-------------------+
     | PK | date_id      |           | PK | fact_id      |
     |    | days_to_trend|           | FK | category_id  |
     +-------------------+           | FK | date_id      |
                                     |    | total_views  |
                                     |    | avg_views    |
                                     |    | avg_likes    |
                                     +-------------------+
    """

    snowflake_structure = """
    ========================================================================
                       СХЕМА «СНІЖИНКА» (SNOWFLAKE SCHEMA)
    ========================================================================

                         +-------------------------+
                         |  dim_parent_category    |
                         +-------------------------+
                         | PK | parent_cat_id      |
                         |    | parent_name        |
                         +-------------------------+
                                      | 1
                                      |
                                      | N
                         +-------------------------+
                         | dim_category_snowflake  |
                         +-------------------------+
                         | PK | category_id        |
                         | FK | parent_cat_id      |
                         |    | category_name      |
                         |    | pct_share          |
                         +-------------------------+
                                      | 1
                                      |
                                      | N
     +-------------------+   N   +-------------------------+
     |dim_date_snowflake |-------| fact_youtube_snowflake  |
     +-------------------+       +-------------------------+
     | PK | date_id      |       | PK | fact_id            |
     |    | days_to_trend|       | FK | category_id        |
     +-------------------+       | FK | date_id            |
                                 |    | total_views        |
                                 |    | avg_views          |
                                 |    | avg_likes          |
                                 +-------------------------+
    """
    print(star_structure)
    print(snowflake_structure)

# -------------------------------------------------------------------
# 2. СТВОРЕННЯ БАЗИ ДАНИХ ТА ГЕНЕРАЦІЯ ВЕЛИКОГО ДАТАСЕТУ
# -------------------------------------------------------------------

def build_analytics_environment(num_records=100000):
    conn = sqlite3.connect(':memory:')
    cursor = conn.cursor()

    # Створення таблиць «Зірка»
    cursor.executescript("""
    CREATE TABLE dim_category_star (
        category_id INTEGER PRIMARY KEY AUTOINCREMENT,
        category_name TEXT,
        pct_share REAL
    );

    CREATE TABLE dim_date_star (
        date_id INTEGER PRIMARY KEY AUTOINCREMENT,
        days_to_trend REAL
    );

    CREATE TABLE fact_youtube_star (
        fact_id INTEGER PRIMARY KEY AUTOINCREMENT,
        category_id INTEGER,
        date_id INTEGER,
        total_views REAL,
        avg_views REAL,
        avg_likes REAL,
        FOREIGN KEY (category_id) REFERENCES dim_category_star(category_id),
        FOREIGN KEY (date_id) REFERENCES dim_date_star(date_id)
    );
    """)

    # Створення таблиць «Сніжинка»
    cursor.executescript("""
    CREATE TABLE dim_parent_category (
        parent_cat_id INTEGER PRIMARY KEY AUTOINCREMENT,
        parent_name TEXT
    );

    CREATE TABLE dim_category_snowflake (
        category_id INTEGER PRIMARY KEY AUTOINCREMENT,
        category_name TEXT,
        parent_cat_id INTEGER,
        pct_share REAL,
        FOREIGN KEY (parent_cat_id) REFERENCES dim_parent_category(parent_cat_id)
    );

    CREATE TABLE dim_date_snowflake (
        date_id INTEGER PRIMARY KEY AUTOINCREMENT,
        days_to_trend REAL
    );

    CREATE TABLE fact_youtube_snowflake (
        fact_id INTEGER PRIMARY KEY AUTOINCREMENT,
        category_id INTEGER,
        date_id INTEGER,
        total_views REAL,
        avg_views REAL,
        avg_likes REAL,
        FOREIGN KEY (category_id) REFERENCES dim_category_snowflake(category_id),
        FOREIGN KEY (date_id) REFERENCES dim_date_snowflake(date_id)
    );
    """)

    print(f"\nГенерація тестових даних ({num_records:,} рядків)...")

    # Довідники
    parents = [('Media',), ('Technology',), ('Lifestyle',)]
    cursor.executemany("INSERT INTO dim_parent_category (parent_name) VALUES (?)", parents)

    categories = [
        ('Entertainment', 1, 32.44), ('Music', 1, 22.10),
        ('Gaming', 2, 15.70), ('Education', 2, 5.80),
        ('News', 1, 9.06), ('Vlogs', 3, 14.90)
    ]
    cursor.executemany("INSERT INTO dim_category_star (category_name, pct_share) VALUES (?, ?)", 
                       [(c[0], c[2]) for c in categories])
    cursor.executemany("INSERT INTO dim_category_snowflake (category_name, parent_cat_id, pct_share) VALUES (?, ?, ?)", categories)

    dates = [(float(i),) for i in range(1, 101)]
    cursor.executemany("INSERT INTO dim_date_star (days_to_trend) VALUES (?)", dates)
    cursor.executemany("INSERT INTO dim_date_snowflake (days_to_trend) VALUES (?)", dates)

    # Генерація фактів
    np.random.seed(42)
    cat_ids = np.random.randint(1, len(categories) + 1, size=num_records)
    date_ids = np.random.randint(1, len(dates) + 1, size=num_records)
    total_views = np.random.uniform(1000, 10000000, size=num_records)
    avg_views = total_views / np.random.uniform(1, 50, size=num_records)
    avg_likes = avg_views * np.random.uniform(0.01, 0.1, size=num_records)

    fact_data = list(zip(cat_ids.tolist(), date_ids.tolist(), total_views.tolist(), avg_views.tolist(), avg_likes.tolist()))

    cursor.executemany("""
    INSERT INTO fact_youtube_star (category_id, date_id, total_views, avg_views, avg_likes)
    VALUES (?, ?, ?, ?, ?)
    """, fact_data)

    cursor.executemany("""
    INSERT INTO fact_youtube_snowflake (category_id, date_id, total_views, avg_views, avg_likes)
    VALUES (?, ?, ?, ?, ?)
    """, fact_data)

    conn.commit()
    return conn

# -------------------------------------------------------------------
# 3. Р розширений АНАЛІЗ ПРОДУКТИВНОСТІ
# -------------------------------------------------------------------

def run_performance_benchmarks(conn, iterations=10):
    print("\n" + "=" * 70)
    print("РОЗШИРЕНА АНАЛІТИКА ТА БЕНЧМАРКІНГ ПРОДУКТИВНОСТІ")
    print("=" * 70)

    # Запит 1: Агрегація з групуванням за категорією
    q_star_1 = """
    SELECT c.category_name, SUM(f.total_views) as sum_views, AVG(f.avg_likes) as avg_likes
    FROM fact_youtube_star f
    JOIN dim_category_star c ON f.category_id = c.category_id
    GROUP BY c.category_name;
    """

    q_snow_1 = """
    SELECT c.category_name, p.parent_name, SUM(f.total_views) as sum_views, AVG(f.avg_likes) as avg_likes
    FROM fact_youtube_snowflake f
    JOIN dim_category_snowflake c ON f.category_id = c.category_id
    JOIN dim_parent_category p ON c.parent_cat_id = p.parent_cat_id
    GROUP BY c.category_name, p.parent_name;
    """

    # Запит 2: Складне фільтрування та агрегація з 2+ JOINs
    q_star_2 = """
    SELECT c.category_name, d.days_to_trend, COUNT(*) as cnt, SUM(f.total_views)
    FROM fact_youtube_star f
    JOIN dim_category_star c ON f.category_id = c.category_id
    JOIN dim_date_star d ON f.date_id = d.date_id
    WHERE d.days_to_trend < 10
    GROUP BY c.category_name, d.days_to_trend;
    """

    q_snow_2 = """
    SELECT c.category_name, p.parent_name, d.days_to_trend, COUNT(*) as cnt, SUM(f.total_views)
    FROM fact_youtube_snowflake f
    JOIN dim_category_snowflake c ON f.category_id = c.category_id
    JOIN dim_parent_category p ON c.parent_cat_id = p.parent_cat_id
    JOIN dim_date_snowflake d ON f.date_id = d.date_id
    WHERE d.days_to_trend < 10
    GROUP BY c.category_name, p.parent_name, d.days_to_trend;
    """

    def measure(query):
        times = []
        for _ in range(iterations):
            t0 = time.perf_counter()
            conn.execute(query).fetchall()
            times.append((time.perf_counter() - t0) * 1000)
        return np.mean(times), np.std(times)

    m_star_1, s_star_1 = measure(q_star_1)
    m_snow_1, s_snow_1 = measure(q_snow_1)
    m_star_2, s_star_2 = measure(q_star_2)
    m_snow_2, s_snow_2 = measure(q_snow_2)

    # Вивід результатів
    metrics_df = pd.DataFrame({
        "Сценарій запиту": [
            "1. Базове групування (Category)", 
            "1. Базове групування (Category)", 
            "2. Фільтр + Множинні JOIN", 
            "2. Фільтр + Множинні JOIN"
        ],
        "Схема": ["Star", "Snowflake", "Star", "Snowflake"],
        "Середній час (мс)": [m_star_1, m_snow_1, m_star_2, m_snow_2],
        "Відхилення (std)": [s_star_1, s_snow_1, s_star_2, s_snow_2]
    })

    print("\nТаблиця порівняння часу виконання запитів:")
    print(metrics_df.to_string(index=False))

    # Візуалізація результатів
    plt.figure(figsize=(10, 5))
    sns.barplot(data=metrics_df, x="Сценарій запиту", y="Середній час (мс)", hue="Схема", palette="Set2")
    plt.title("Порівняння швидкості виконання запитів (Star vs Snowflake)", fontsize=13)
    plt.ylabel("Час виконання (мілісекунди)")
    plt.tight_layout()
    plt.savefig("query_performance_comparison.png", dpi=300)
    print("\n[УСПІХ] Побудовано та збережено графік порівняння: 'query_performance_comparison.png'")

# -------------------------------------------------------------------
# ГОЛОВНИЙ СЦЕНАРІЙ
# -------------------------------------------------------------------
if __name__ == "__main__":
    draw_schema_diagrams()
    db_conn = build_analytics_environment(num_records=200000)
    run_performance_benchmarks(db_conn)