# config_generator.py
import datetime # Добавим импорт datetime, если он используется для дат

# --- Строка подключения и общие параметры ---
DB_CONN_STR = (
    "DRIVER={ODBC Driver 17 for SQL Server};"
    "SERVER=ACER;"  # << Взято из вашего второго конфига
    "DATABASE=TestDB;" # << Взято из вашего второго конфига
    "UID=iimin;" # Закомментировано, т.к. Trusted_Connection=yes
    "Trusted_Connection=yes;" # << Взято из вашего второго конфига
)


DB_POOL_MIN_SIZE = 5
DB_POOL_MAX_SIZE = 20


# Диапазон лет в прошлом для генерации дат (например, регистрации, создания ресторана)
# DATA_START_DATE из вашего второго конфига соответствует 25 годам назад
DATE_RANGE_YEARS = 25

BATCH_SIZE = 100       # Количество записей для одного executemany (оставим как есть, если генератор это учитывает)


# --- Параметры для первоначальной генерации (аналогично INITIAL_... из второго конфига) ---
NUM_CLIENTS_TO_GENERATE = 1000        # Соответствует INITIAL_CLIENTS
NUM_RESTAURANTS_TO_GENERATE = 50      # Соответствует INITIAL_RESTAURANTS

AVG_TABLES_PER_RESTAURANT = 5         # Соответствует INITIAL_TABLES_PER_RESTAURANT
AVG_MENU_ITEMS_PER_RESTAURANT = 40    # Оставим текущее значение или можно подстроить, если есть аналог в "втором конфиге"


AVG_BOOKINGS_PER_CLIENT_OR_TABLE = 5 # Попытка приблизить, но это ОЧЕНЬ грубая оценка.

CHANCE_BOOKING_HAS_ORDER = 0.90       # Соответствует AVG_ORDERS_PER_BOOKING
AVG_ITEMS_PER_ORDER = 3               # Соответствует AVG_ITEMS_PER_ORDER


AVG_REVIEWS_PER_RESTAURANT_OR_CLIENT = 0.3 # Компромиссное значение, настройте.

