"""
ETL Скрипт инициализации базы данных для C-MAPSS Симулятора.
Читает сырой текстовый файл NASA, валидирует данные (Data Contract L1)
и выполняет пакетную загрузку в SQLite с добавлением системных колонок.
"""

import sqlite3
import logging
from pathlib import Path
from typing import List, Tuple

# Настройка логирования для красивого вывода в консоль
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)


def setup_database(db_path: Path) -> None:
    """
    Создает базу данных и таблицу flights (Идемпотентно).

    Args:
        db_path (Path): Абсолютный путь к файлу SQLite.
    """
    # Создаем папку data, если её вдруг нет
    db_path.parent.mkdir(parents=True, exist_ok=True)

    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()

    logging.info("Инициализация базы данных SQLite и настройка PRAGMA...")
    # Оптимизация SQLite для будущих конкурентных записей
    cursor.execute("PRAGMA journal_mode=WAL;")
    cursor.execute("PRAGMA synchronous=NORMAL;")

    logging.info("Создание таблицы 'flights' (DROP IF EXISTS)...")
    cursor.execute("DROP TABLE IF EXISTS flights;")

    # DDL схемы на основе дата-контракта (26 физических + 3 системные колонки)
    create_table_query = """
    CREATE TABLE flights (
        -- Системные колонки (Метаданные симулятора)
        status TEXT DEFAULT 'PENDING',
        flight_start_time TEXT DEFAULT NULL,
        target_duration_sec INTEGER DEFAULT NULL,
        
        -- Физические колонки NASA
        unit_number INTEGER,
        time_cycles INTEGER,
        op_setting_1 REAL,
        op_setting_2 REAL,
        op_setting_3 REAL,
        T2 REAL,
        T24 REAL,
        T30 REAL,
        T50 REAL,
        P2 REAL,
        P15 REAL,
        P30 REAL,
        Nf REAL,
        Nc REAL,
        epr REAL,
        Ps30 REAL,
        phi REAL,
        NRf REAL,
        NRc REAL,
        BPR REAL,
        farB REAL,
        htBleed REAL,
        Nf_dmd REAL,
        PCNfR_dmd REAL,
        W31 REAL,
        W32 REAL
    );
    """
    cursor.execute(create_table_query)
    conn.commit()
    conn.close()
    logging.info("Схема таблицы 'flights' успешно создана.")


def parse_and_validate_line(
    line: str, line_number: int
) -> Tuple[
    int,
    int,
    float,
    float,
    float,
    float,
    float,
    float,
    float,
    float,
    float,
    float,
    float,
    float,
    float,
    float,
    float,
    float,
    float,
    float,
    float,
    float,
    float,
    float,
    float,
    float,
]:
    """
    Парсит строку текста, убирает мусор и строго валидирует типы (Data Contract L1).

    Args:
        line (str): Сырая строка из текстового файла.
        line_number (int): Номер строки для вывода информативной ошибки.

    Returns:
        tuple: Кортеж из 26 значений (2 int, 24 float).

    Raises:
        ValueError: Если количество колонок не равно 26 или нарушены типы данных.
    """
    # Изящный Native: split() без аргументов сам схлопнет двойные/тройные пробелы
    parts = line.strip().split()

    if len(parts) != 26:
        raise ValueError(
            f"Нарушение Data Contract! Строка {line_number} содержит {len(parts)} значений вместо 26."
        )

    try:
        # Кастинг типов: первые два значения - ID и цикл (INTEGER)
        unit_number = int(parts[0])
        time_cycles = int(parts[1])

        # Остальные 24 значения - показания датчиков (REAL/Float)
        sensors = [float(x) for x in parts[2:]]

        # Распаковываем всё в единый кортеж (Tuple)
        return (unit_number, time_cycles, *sensors)

    except ValueError as e:
        raise ValueError(
            f"Ошибка типов в строке {line_number}. Невозможно конвертировать в int/float: {e}"
        )


def load_data_to_sqlite(txt_path: Path, db_path: Path) -> None:
    """
    Читает текстовый файл и загружает данные в SQLite батчами.

    Args:
        txt_path (Path): Путь к сырому txt файлу.
        db_path (Path): Путь к целевой БД.
    """
    logging.info(f"Чтение файла: {txt_path}")

    if not txt_path.exists():
        logging.error(f"Файл не найден: {txt_path}")
        return

    batch_data = []

    with open(txt_path, "r", encoding="utf-8") as f:
        for line_number, line in enumerate(f, 1):
            if not line.strip():  # Пропуск пустых строк, если они есть в конце файла
                continue

            parsed_tuple = parse_and_validate_line(line, line_number)
            batch_data.append(parsed_tuple)

    logging.info(
        f"Успешно провалидировано {len(batch_data)} строк. Начинается загрузка (Batch Insert)..."
    )

    # Формируем строку из 26 знаков вопроса (?, ?, ... , ?)
    placeholders = ", ".join(["?"] * 26)
    insert_query = f"""
        INSERT INTO flights (
            unit_number, time_cycles, op_setting_1, op_setting_2, op_setting_3, 
            T2, T24, T30, T50, P2, P15, P30, Nf, Nc, epr, Ps30, phi, NRf, NRc, 
            BPR, farB, htBleed, Nf_dmd, PCNfR_dmd, W31, W32
        ) VALUES ({placeholders})
    """

    # Пакетная вставка (сверхбыстрая)
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    cursor.executemany(insert_query, batch_data)
    conn.commit()
    conn.close()

    logging.info("✅ Загрузка завершена! ДНК Аэродрома готово.")


def main():
    # Определение путей с помощью современного pathlib
    # __file__ указывает на scripts/init_db.py. parent.parent - это корень проекта
    project_root = Path(__file__).resolve().parent.parent

    # Тут я предполагаю, что файл NASA лежит в папке data/
    # (если ты положил его в другое место, измени путь ниже)
    txt_file_path = project_root / "data" / "cmapss_data" / "train_FD001.txt"
    sqlite_db_path = project_root / "data" / "blueprints.sqlite"

    setup_database(sqlite_db_path)
    load_data_to_sqlite(txt_file_path, sqlite_db_path)


if __name__ == "__main__":
    main()
