import datetime


DB_CONN_STR = (
    "DRIVER={ODBC Driver 17 for SQL Server};"
    "SERVER=ACER;"
    "DATABASE=TestDB;"
    "UID=iimin;"
    "Trusted_Connection=yes;"
)
# Максимальное кол-во подключений в пуле для aioodbc
DB_POOL_MIN_SIZE = 10
DB_POOL_MAX_SIZE = 50


# Базовое количество сущностей для ПЕРВОГО запуска
INITIAL_CLIENTS = 1000
INITIAL_RESTAURANTS = 50
INITIAL_TABLES_PER_RESTAURANT = 5 # Среднее

# Множители и пропорции для генерации зависимых данных
AVG_BOOKINGS_PER_CLIENT_PER_YEAR = 30 # Среднее кол-во бронирований на клиента в год
AVG_ORDERS_PER_BOOKING = 0.9 # Не каждое бронирование становится заказом (вероятность)
AVG_ITEMS_PER_ORDER = 3      # Среднее кол-во позиций в заказе
REVIEW_PROBABILITY_PER_BOOKING = 0.15 # Вероятность отзыва на бронирование/заказ

# Диапазон дат для генерации 
DATA_START_DATE = datetime.datetime.now() - datetime.timedelta(days=25*365)
DATA_END_DATE = datetime.datetime.now()

# Сколько записей добавлять при КАЖДОМ ПОСЛЕДУЮЩЕМ запуске генератора
INCREMENTAL_CLIENTS = 200
INCREMENTAL_RESTAURANTS = 5
# При повторных запусках, новые бронирования/заказы будут генерироваться
# для существующих и новых клиентов/ресторанов за последний год.

SIMULATION_DURATION_SECONDS = 180 # Как долго работает симуляция
NUM_CONCURRENT_USERS = 50       # Количество "одновременных" пользователей
MIN_USER_WAIT_SECONDS = 0.5     # Минимальная пауза между действиями пользователя
MAX_USER_WAIT_SECONDS = 3.0     # Максимальная пауза

# Веса для разных типов операций (чем выше вес, тем чаще операция)
QUERY_WEIGHTS = {
    "read_restaurant_menu": 25,
    "read_client_orders": 20,
    "read_booking_status": 15,
    "read_restaurant_reviews": 10,
    "read_find_restaurants": 10,
    "create_booking": 5,
    "update_booking_status": 4,
    "create_order_from_booking": 3,
    "create_review": 3,
    "update_client_info": 2,
    "read_payment_status": 2,
    "update_order_status": 1, 

}