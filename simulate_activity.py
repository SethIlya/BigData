import asyncio
import aioodbc 
import pyodbc 
import random
import time
import datetime
import traceback
from faker import Faker
from decimal import Decimal # Для корректной работы с ценами перед преобразованием в строку

try:
    from config import (DB_CONN_STR, DB_POOL_MIN_SIZE, DB_POOL_MAX_SIZE,
                        SIMULATION_DURATION_SECONDS, NUM_CONCURRENT_USERS,
                        MIN_USER_WAIT_SECONDS, MAX_USER_WAIT_SECONDS, QUERY_WEIGHTS)
except ImportError:
    print("WARNING: config.py not found or incomplete. Using default simulation parameters.")
    DB_CONN_STR = "DRIVER={ODBC Driver 17 for SQL Server};SERVER=ACER;DATABASE=TestDB;Trusted_Connection=yes;" # Обновлено
    DB_POOL_MIN_SIZE = 2
    DB_POOL_MAX_SIZE = 10
    SIMULATION_DURATION_SECONDS = 180 
    NUM_CONCURRENT_USERS = 20     
    MIN_USER_WAIT_SECONDS = 0.5
    MAX_USER_WAIT_SECONDS = 2.5
    QUERY_WEIGHTS = {
        "read_restaurant_menu": 15, "read_client_orders": 10, "read_booking_status": 8,
        "read_restaurant_reviews": 12, "read_find_restaurants": 18, "read_payment_status": 7,
        "read_specific_client_info": 5, "read_restaurant_details": 6, "read_available_tables": 9,
        "create_booking": 20, "create_order_from_booking": 15, "create_review": 10,
        "create_client": 3, "add_menu_item": 2,
        "update_booking_status": 10, "update_client_info": 7, "update_order_status": 8,
        "update_menu_item_price": 2,
        "delete_review": 3, "cancel_booking": 5, "remove_menu_item": 1,
    }

fake = Faker('ru_RU')


DB_BOOKING_STATUS_NAME_MAX_LEN = 10
DB_CLIENT_NAME_MAX_LEN = 50
DB_CLIENT_PHONE_MAX_LEN = 10
DB_CLIENT_EMAIL_MAX_LEN = 20
DB_RESTAURANT_NAME_MAX_LEN = 30
DB_RESTAURANT_ADDRES_MAX_LEN = 50
DB_RESTAURANT_CUISINE_MAX_LEN = 50
DB_MENU_DISHNAME_MAX_LEN = 100
DB_PAYMENT_METHOD_MAX_LEN = 20
DB_PAYMENT_STATUS_MAX_LEN = 20
DB_REVIEW_COMMENT_MAX_LEN = 500


def trim_str(text: str, max_len: int) -> str:
    return text[:max_len] if text else ""

# --- Списки активных ID для симуляции ---
ACTIVE_CLIENT_IDS = []
ACTIVE_RESTAURANT_IDS = []
ACTIVE_BOOKING_IDS = []
ACTIVE_ORDER_IDS = []
ACTIVE_MENU_IDS = []
ACTIVE_TABLE_IDS = [] # Будут ID из RestaurantTable
ACTIVE_REVIEW_IDS = []

async def fetch_initial_ids(pool):
    global ACTIVE_CLIENT_IDS, ACTIVE_RESTAURANT_IDS, ACTIVE_BOOKING_IDS, ACTIVE_ORDER_IDS, ACTIVE_MENU_IDS, ACTIVE_TABLE_IDS, ACTIVE_REVIEW_IDS
    print("Fetching initial IDs for simulation...")
    required_ids_fetched = True
    try:
        async with pool.acquire() as conn:
            async with conn.cursor() as cursor:
                async def fetch_ids_from_table(id_list, table, id_column, order_by_col=None, limit=1000, name="Entities"): # Уменьшил лимит для скорости
                    order_clause = f"ORDER BY {order_by_col} DESC" if order_by_col else "ORDER BY NEWID()"
                    query = f"SELECT TOP ({limit}) {id_column} FROM {table} {order_clause}"
                    print(f"  Fetching {name} IDs ({table})...")
                    await cursor.execute(query)
                    fetched = [row[0] for row in await cursor.fetchall()]
                    id_list.clear()
                    id_list.extend(fetched)
                    print(f"    Found {len(id_list)} {name} IDs.")
                    # Для симуляции критично иметь хоть какие-то данные для работы
                    if not id_list and name in ["Client", "Restaurant", "Menu", "Table", "BookingStatus"]:
                        print(f"    WARNING: No {name} IDs found. This might be critical for some operations.")
                        # Не будем делать это фатальным, симулятор попробует создать данные
                        # return False
                    elif not id_list:
                        print(f"    Warning: No {name} IDs found. Some actions might fail or do nothing.")
                    return True

                # Загружаем ID. Если таблицы пусты после генерации, симулятор будет создавать новые.
                await fetch_ids_from_table(ACTIVE_CLIENT_IDS, "dbo.Client", "ClientID", name="Client")
                await fetch_ids_from_table(ACTIVE_RESTAURANT_IDS, "dbo.Restaraunt", "RestarauntID", name="Restaurant")
                await fetch_ids_from_table(ACTIVE_BOOKING_IDS, "dbo.Booking", "BookingID", "BookingDate", name="Booking")
                await fetch_ids_from_table(ACTIVE_ORDER_IDS, "dbo.Orders", "OrderID", "OrderID", name="Order") # Используем Orders
                await fetch_ids_from_table(ACTIVE_MENU_IDS, "dbo.Menu", "MenuID", name="Menu")
                await fetch_ids_from_table(ACTIVE_TABLE_IDS, "dbo.RestaurantTable", "TableID", name="Table")
                await fetch_ids_from_table(ACTIVE_REVIEW_IDS, "dbo.Review", "ReviewID", "ReviewDate", name="Review")

        print(f"Fetched IDs summary: Clients: {len(ACTIVE_CLIENT_IDS)}, Restaurants: {len(ACTIVE_RESTAURANT_IDS)}, "
              f"Bookings: {len(ACTIVE_BOOKING_IDS)}, Orders: {len(ACTIVE_ORDER_IDS)}, MenuItems: {len(ACTIVE_MENU_IDS)}, "
              f"Tables: {len(ACTIVE_TABLE_IDS)}, Reviews: {len(ACTIVE_REVIEW_IDS)}")
        return required_ids_fetched
    except pyodbc.Error as db_err: # Ловим специфичную ошибку ODBC
        print(f"FATAL: DB Error during initial ID fetch: {db_err}")
        traceback.print_exc()
        return False
    except Exception as e:
        print(f"FATAL: Could not fetch initial IDs: {e}")
        traceback.print_exc()
        return False

# --- CREATE операции (адаптированы под вашу схему, предполагая IDENTITY для PK) ---
async def create_client(cursor):
    """Создает нового клиента. Предполагает ClientID - IDENTITY."""
    name = trim_str(fake.name(), DB_CLIENT_NAME_MAX_LEN)
    phone = trim_str(fake.msisdn(), DB_CLIENT_PHONE_MAX_LEN) # fake.msisdn() более простой, чем fake.phone_number()
    email = trim_str(fake.email(), DB_CLIENT_EMAIL_MAX_LEN)
    # registration_date будет DEFAULT или установлено триггером в БД, если не передавать
    # Если registration_date обязателен и не имеет DEFAULT, его нужно генерировать:
    # registration_date = datetime.datetime.now() - datetime.timedelta(days=random.randint(0, 365*5))
    # await cursor.execute(
    # "SET NOCOUNT ON; INSERT INTO dbo.Client (Name, phone, email, registration_date) VALUES (?, ?, ?, ?); SELECT SCOPE_IDENTITY();",
    # name, phone, email, registration_date
    # )
    # Для простоты симулятора, если ClientID - IDENTITY, и registration_date имеет DEFAULT GETDATE()
    await cursor.execute(
        "SET NOCOUNT ON; INSERT INTO dbo.Client (Name, phone, email, registration_date) VALUES (?, ?, ?, GETDATE()); SELECT SCOPE_IDENTITY();",
        name, phone, email # Предполагаем, что registration_date ставится по умолчанию или не критична для симуляции этой операции
    )
    new_client_id_row = await cursor.fetchone()
    if new_client_id_row and new_client_id_row[0]:
        new_client_id = int(new_client_id_row[0])
        ACTIVE_CLIENT_IDS.append(new_client_id)
        # print(f"    New client created: ID {new_client_id}")

async def create_booking(cursor):
    """Создает бронирование. BookingID - IDENTITY. TableID из RestaurantTable. Нет NumberOfGuests."""
    if not ACTIVE_CLIENT_IDS: print("Warning: No active clients for create_booking"); return
    if not ACTIVE_TABLE_IDS: print("Warning: No active tables for create_booking"); return
    # В вашей таблице Booking_status 4 статуса. "В ожидании" обычно имеет ID=1.
    # Загрузим статусы в fetch_initial_ids или захардкодим здесь для простоты симулятора
    # Если статусы загружаются, то: status_id = random.choice(ACTIVE_BOOKING_STATUS_IDS)
    status_id = 1 # "В ожидании" (предположение)
    if not status_id:
        print("Warning: No booking statuses available for create_booking"); return


    client_id = random.choice(ACTIVE_CLIENT_IDS)
    table_id = random.choice(ACTIVE_TABLE_IDS) # Это ID из RestaurantTable
    booking_date = datetime.datetime.now() + datetime.timedelta(days=random.randint(1, 30), hours=random.randint(10,20))

    await cursor.execute(
        "SET NOCOUNT ON; INSERT INTO dbo.Booking (ClientID, StatusID, TableID, BookingDate) VALUES (?, ?, ?, ?); SELECT SCOPE_IDENTITY();",
        client_id, status_id, table_id, booking_date
    )
    new_booking_id_row = await cursor.fetchone()
    if new_booking_id_row and new_booking_id_row[0]:
        new_booking_id = int(new_booking_id_row[0])
        ACTIVE_BOOKING_IDS.append(new_booking_id)
        # print(f"    New booking created: ID {new_booking_id} for Client {client_id}")

async def create_order_from_booking(cursor):
    """Создает заказ из брони. OrderID, OrderItemID, PaymentID - IDENTITY. Цены - int."""
    if not ACTIVE_BOOKING_IDS: print("Warning: No active bookings for create_order"); return
    
    # Выбираем подтвержденное бронирование (StatusID = 2) без заказа
    # (предполагаем, что StatusID=2 это "Подтвержено")
    await cursor.execute("""
        SELECT TOP 1 B.BookingID
        FROM dbo.Booking B
        LEFT JOIN dbo.Orders O ON B.BookingID = O.BookingID
        WHERE B.StatusID = 2 AND O.OrderID IS NULL 
        ORDER BY NEWID() 
    """)
    booking_res_row = await cursor.fetchone()
    if not booking_res_row or not booking_res_row[0]:
        # print("    No suitable booking found to create an order from.")
        return
    booking_id = booking_res_row[0]

    if not ACTIVE_MENU_IDS: print("Warning: No active menu items for create_order"); return

    # В вашей схеме Orders.total_price - int, OrdersItem.Price_At_Order - int
    # В вашей схеме OrdersItem нет Quantity. Заказ будет состоять из одной случайной позиции.
    order_total_int = 0
    
    menu_id_for_order = random.choice(ACTIVE_MENU_IDS)
    # Получаем цену из Menu.Price (decimal), но будем использовать int для OrderItem
    await cursor.execute("SELECT Price FROM dbo.Menu WHERE MenuID = ?", menu_id_for_order)
    price_row = await cursor.fetchone()
    if not price_row or price_row[0] is None:
        # print(f"    Could not get price for MenuID {menu_id_for_order}")
        return
    
    price_at_order_int = int(price_row[0]) # Преобразуем decimal/float из БД в int
    order_total_int = price_at_order_int # Так как одна позиция

    # StatusOrder: 1-Новый, 2-Готовится, 3-Завершен (как в вашем pyodbc генераторе)
    status_order = 1 

    await cursor.execute(
        "SET NOCOUNT ON; INSERT INTO dbo.Orders (BookingID, StatusOrder, total_price) VALUES (?, ?, ?); SELECT SCOPE_IDENTITY();",
        booking_id, status_order, order_total_int
    )
    order_id_res_row = await cursor.fetchone()
    if not order_id_res_row or not order_id_res_row[0]: return
    order_id = int(order_id_res_row[0])
    ACTIVE_ORDER_IDS.append(order_id)

    # Вставляем одну позицию в OrderItem
    await cursor.execute(
        "INSERT INTO dbo.OrdersItem (OrderID, MenuID, Price_At_Order) VALUES (?, ?, ?)",
        order_id, menu_id_for_order, price_at_order_int
    ) # OrderItemID - IDENTITY

    # Вставляем платеж. Payments не имеет Amount.
    method = trim_str(random.choice(["Наличные", "Карта", "Онлайн"]), DB_PAYMENT_METHOD_MAX_LEN)
    pay_status = trim_str(random.choice(["Оплачено", "В ожидании", "Не прошло"]), DB_PAYMENT_STATUS_MAX_LEN)
    await cursor.execute(
        "INSERT INTO dbo.Payments (OrderID, Method, Payment_Status) VALUES (?, ?, ?)", # PaymentID - IDENTITY
        order_id, method, pay_status
    )
    # print(f"    New order created: ID {order_id} from Booking {booking_id}")

async def create_review(cursor):
    """Создает отзыв. ReviewID - IDENTITY."""
    if not ACTIVE_CLIENT_IDS: print("Warning: No active clients for create_review"); return
    if not ACTIVE_RESTAURANT_IDS: print("Warning: No active restaurants for create_review"); return

    client_id = random.choice(ACTIVE_CLIENT_IDS)
    rest_id = random.choice(ACTIVE_RESTAURANT_IDS)
    rating = random.randint(1, 5)
    comment = trim_str(fake.text(max_nb_chars=random.randint(50, 450)), DB_REVIEW_COMMENT_MAX_LEN)
    review_date = datetime.datetime.now() - datetime.timedelta(days=random.randint(0, 30))
    
    await cursor.execute(
        "SET NOCOUNT ON; INSERT INTO dbo.Review (ClientID, RestaurantID, Rating, Comment, ReviewDate) VALUES (?, ?, ?, ?, ?); SELECT SCOPE_IDENTITY();",
        client_id, rest_id, rating, comment, review_date
    )
    new_review_id_row = await cursor.fetchone()
    if new_review_id_row and new_review_id_row[0]:
        new_review_id = int(new_review_id_row[0])
        ACTIVE_REVIEW_IDS.append(new_review_id)
        # print(f"    New review created: ID {new_review_id}")

async def add_menu_item(cursor):
    """Добавляет блюдо. MenuID - IDENTITY. Нет Description, Category."""
    if not ACTIVE_RESTAURANT_IDS: print("Warning: No active restaurants for add_menu_item"); return
    rest_id = random.choice(ACTIVE_RESTAURANT_IDS)
    dish_name = trim_str(fake.bs(), DB_MENU_DISHNAME_MAX_LEN) # fake.bs() для разнообразия
    price_decimal = Decimal(round(random.uniform(5.0, 100.0), 2)) # Как в вашем pyodbc генераторе
    price_str = str(price_decimal) # Передаем цену как строку

    await cursor.execute(
        "SET NOCOUNT ON; INSERT INTO dbo.Menu (RestaurantID, DishName, Price) VALUES (?, ?, ?); SELECT SCOPE_IDENTITY();",
        rest_id, dish_name, price_str
    )
    new_menu_id_row = await cursor.fetchone()
    if new_menu_id_row and new_menu_id_row[0]:
        new_menu_id = int(new_menu_id_row[0])
        ACTIVE_MENU_IDS.append(new_menu_id)
        # print(f"    New menu item added: ID {new_menu_id} ('{dish_name}')")

# --- READ операции (адаптированы) ---
async def read_restaurant_menu(cursor):
    if not ACTIVE_RESTAURANT_IDS: return
    rest_id = random.choice(ACTIVE_RESTAURANT_IDS)
    # Убран Category
    await cursor.execute("SELECT TOP 20 DishName, Price FROM dbo.Menu WHERE RestaurantID = ?", rest_id)
    await cursor.fetchall()

async def read_client_orders(cursor):
    if not ACTIVE_CLIENT_IDS: return
    client_id = random.choice(ACTIVE_CLIENT_IDS)
    await cursor.execute("""
        SELECT O.OrderID, O.total_price, O.StatusOrder, B.BookingDate
        FROM dbo.Orders O 
        JOIN dbo.Booking B ON O.BookingID = B.BookingID
        WHERE B.ClientID = ?
        ORDER BY B.BookingDate DESC
        OFFSET 0 ROWS FETCH NEXT 10 ROWS ONLY;
    """, client_id)
    await cursor.fetchall()

async def read_booking_status(cursor):
    if not ACTIVE_BOOKING_IDS: return
    booking_id = random.choice(ACTIVE_BOOKING_IDS)
    await cursor.execute("""
        SELECT B.BookingID, B.BookingDate, BS.status_name
        FROM dbo.Booking B
        JOIN dbo.Booking_status BS ON B.StatusID = BS.StatusID
        WHERE B.BookingID = ?
    """, booking_id)
    await cursor.fetchone()

async def read_restaurant_reviews(cursor):
     if not ACTIVE_RESTAURANT_IDS: return
     rest_id = random.choice(ACTIVE_RESTAURANT_IDS)
     await cursor.execute("""
         SELECT TOP 10 R.Rating, R.Comment, R.ReviewDate, C.Name as ClientName
         FROM dbo.Review R
         JOIN dbo.Client C ON R.ClientID = C.ClientID
         WHERE R.RestaurantID = ?
         ORDER BY R.ReviewDate DESC
     """, rest_id)
     await cursor.fetchall()

async def read_find_restaurants(cursor):
    # Убран average_rating, т.к. его нет в dbo.Restaraunt
    cuisines = ["итальянская", "китайская", "мексиканская", "японская", "французская", "индийская"] # Из вашего pyodbc
    cuisine_query = random.choice(cuisines)
    
    query = """
        SELECT TOP 10 Name, addres, cuisine 
        FROM dbo.Restaraunt 
        WHERE cuisine LIKE ? 
        ORDER BY NEWID() 
    """ # Убрана сортировка по average_rating
    params = [f"%{trim_str(cuisine_query, DB_RESTAURANT_CUISINE_MAX_LEN)}%"]
    await cursor.execute(query, *params)
    await cursor.fetchall()

async def read_payment_status(cursor):
    if not ACTIVE_ORDER_IDS: return
    order_id = random.choice(ACTIVE_ORDER_IDS)
    # Убраны Amount, PaymentDate, т.к. их нет в dbo.Payments
    await cursor.execute("""
        SELECT P.Payment_Status, P.Method 
        FROM dbo.Payments P 
        WHERE P.OrderID = ?
    """, order_id)
    await cursor.fetchone()

async def read_specific_client_info(cursor):
    if not ACTIVE_CLIENT_IDS: return
    client_id = random.choice(ACTIVE_CLIENT_IDS)
    # registration_date есть в вашей таблице Client
    await cursor.execute("SELECT ClientID, Name, phone, email, registration_date FROM dbo.Client WHERE ClientID = ?", client_id)
    await cursor.fetchone()

async def read_restaurant_details(cursor):
    if not ACTIVE_RESTAURANT_IDS: return
    rest_id = random.choice(ACTIVE_RESTAURANT_IDS)
    # Убраны phone_number, working_hours, average_rating
    await cursor.execute("SELECT RestarauntID, Name, addres, cuisine FROM dbo.Restaraunt WHERE RestarauntID = ?", rest_id)
    await cursor.fetchone()

async def read_available_tables(cursor):
    if not ACTIVE_RESTAURANT_IDS: return
    rest_id = random.choice(ACTIVE_RESTAURANT_IDS)
    search_datetime = datetime.datetime.now() + datetime.timedelta(days=random.randint(0, 7), hours=random.randint(10,20))
    # num_guests не используется в запросе, т.к. в Booking нет NumberOfGuests, а в RestaurantTable есть 'max'
    
    # Убрано LocationDescription. Capacity заменено на max.
    # num_guests из запроса не используется для фильтрации по вместимости, т.к. это усложнит запрос без NumberOfGuests в Booking.
    # Просто ищем столы, не занятые на +/- 2 часа.
    await cursor.execute("""
        SELECT TOP 5 T.TableID, T.max
        FROM dbo.RestaurantTable T
        WHERE T.RestaurantID = ?
          AND NOT EXISTS (
              SELECT 1 FROM dbo.Booking B
              WHERE B.TableID = T.TableID AND B.StatusID IN (1, 2) -- "В ожидании" или "Подтверждено"
              AND B.BookingDate BETWEEN DATEADD(hour, -2, ?) AND DATEADD(hour, 2, ?)
          )
        ORDER BY T.max 
    """, rest_id, search_datetime, search_datetime)
    await cursor.fetchall()

# --- UPDATE операции (адаптированы) ---
async def update_booking_status(cursor):
    if not ACTIVE_BOOKING_IDS: return
    booking_id = random.choice(ACTIVE_BOOKING_IDS)
    # Статусы из вашего pyodbc: 1-"В ожидании", 2-"Подтвержено", 3-"Отменено", 4-"Завершено"
    # Обновляем на существующие, кроме текущего и "В ожидании"
    # Предположим, мы хотим изменить на "Подтвержено", "Отменено", "Завершено"
    possible_new_statuses = [2, 3, 4] 
    # Можно получить текущий статус и исключить его, но для простоты выберем из этого списка
    new_status_id = random.choice(possible_new_statuses)

    # Убран LastUpdated
    await cursor.execute("UPDATE dbo.Booking SET StatusID = ? WHERE BookingID = ?", new_status_id, booking_id)
    # print(f"    Booking {booking_id} status updated to {new_status_id}")


async def update_client_info(cursor):
    if not ACTIVE_CLIENT_IDS: return
    client_id = random.choice(ACTIVE_CLIENT_IDS)
    new_phone = trim_str(fake.msisdn(), DB_CLIENT_PHONE_MAX_LEN)
    new_email = trim_str(fake.email(), DB_CLIENT_EMAIL_MAX_LEN)

    if random.random() < 0.5:
        await cursor.execute("UPDATE dbo.Client SET phone = ? WHERE ClientID = ?", new_phone, client_id)
    else:
        await cursor.execute("UPDATE dbo.Client SET email = ? WHERE ClientID = ?", new_email, client_id)
    # print(f"    Client {client_id} info updated.")

async def update_order_status(cursor):
     if not ACTIVE_ORDER_IDS: return
     order_id = random.choice(ACTIVE_ORDER_IDS)
     # StatusOrder в вашей БД int (1-3 из pyodbc генератора).
     # Симулятор может менять на другие статусы, если они определены для заказов.
     # Для примера, обновим на 2 или 3.
     new_status = random.choice([2, 3])
     await cursor.execute("UPDATE dbo.Orders SET StatusOrder = ? WHERE OrderID = ?", new_status, order_id)
     # print(f"    Order {order_id} status updated to {new_status}")

async def update_menu_item_price(cursor):
    if not ACTIVE_MENU_IDS: return
    menu_item_id = random.choice(ACTIVE_MENU_IDS)
    new_price_decimal = Decimal(round(random.uniform(5.0, 100.0), 2))
    new_price_str = str(new_price_decimal) # Передаем как строку
    await cursor.execute("UPDATE dbo.Menu SET Price = ? WHERE MenuID = ?", new_price_str, menu_item_id)
    # print(f"    Menu item {menu_item_id} price updated to {new_price_str}")

# --- DELETE операции (адаптированы) ---
async def delete_review(cursor):
    if not ACTIVE_REVIEW_IDS: return
    review_id_to_delete = random.choice(ACTIVE_REVIEW_IDS)
    await cursor.execute("DELETE FROM dbo.Review WHERE ReviewID = ?", review_id_to_delete)
    if await cursor.rowcount > 0:
        if review_id_to_delete in ACTIVE_REVIEW_IDS: ACTIVE_REVIEW_IDS.remove(review_id_to_delete)
        # print(f"    Review ID {review_id_to_delete} deleted.")

async def cancel_booking(cursor):
    if not ACTIVE_BOOKING_IDS: return

    # Удаляем бронирования, которые еще не начались или "в ожидании" / "отменено"
    # Это более простая логика, чем в оригинале, т.к. меньше полей для фильтрации.
    await cursor.execute("""
        SELECT TOP 1 BookingID
        FROM dbo.Booking
        WHERE StatusID IN (1, 3) OR (StatusID != 4 AND BookingDate > GETDATE())
        ORDER BY NEWID()
    """) # Статус 1: В ожидании, 3: Отменено, 4: Завершено
    booking_to_delete_res = await cursor.fetchone()
    if not booking_to_delete_res: return

    booking_id_to_delete = booking_to_delete_res[0]

    # Удаляем связанные заказы, их позиции и платежи
    await cursor.execute("SELECT OrderID FROM dbo.Orders WHERE BookingID = ?", booking_id_to_delete)
    related_order_ids = [row[0] for row in await cursor.fetchall()]

    for order_id in related_order_ids:
        await cursor.execute("DELETE FROM dbo.Payments WHERE OrderID = ?", order_id)
        await cursor.execute("DELETE FROM dbo.OrdersItem WHERE OrderID = ?", order_id) # Имя таблицы OrdersItem
        await cursor.execute("DELETE FROM dbo.Orders WHERE OrderID = ?", order_id)
        if order_id in ACTIVE_ORDER_IDS: ACTIVE_ORDER_IDS.remove(order_id)

    await cursor.execute("DELETE FROM dbo.Booking WHERE BookingID = ?", booking_id_to_delete)
    if await cursor.rowcount > 0 and booking_id_to_delete in ACTIVE_BOOKING_IDS:
        ACTIVE_BOOKING_IDS.remove(booking_id_to_delete)
        # print(f"    Booking ID {booking_id_to_delete} and related data deleted.")

async def remove_menu_item(cursor):
    if not ACTIVE_MENU_IDS: return
    menu_item_id_to_delete = random.choice(ACTIVE_MENU_IDS)
    # В реальной системе нужна проверка, не используется ли это блюдо в активных заказах (OrdersItem)
    # Для симуляции упрощаем
    await cursor.execute("DELETE FROM dbo.Menu WHERE MenuID = ?", menu_item_id_to_delete)
    if await cursor.rowcount > 0:
        if menu_item_id_to_delete in ACTIVE_MENU_IDS: ACTIVE_MENU_IDS.remove(menu_item_id_to_delete)
        # print(f"    Menu Item ID {menu_item_id_to_delete} removed.")

# --- Карта функций ---
QUERY_FUNCTION_MAP = {
    "create_client": create_client, "create_booking": create_booking,
    "create_order_from_booking": create_order_from_booking, "create_review": create_review,
    "add_menu_item": add_menu_item, "read_restaurant_menu": read_restaurant_menu,
    "read_client_orders": read_client_orders, "read_booking_status": read_booking_status,
    "read_restaurant_reviews": read_restaurant_reviews, "read_find_restaurants": read_find_restaurants,
    "read_payment_status": read_payment_status, "read_specific_client_info": read_specific_client_info,
    "read_restaurant_details": read_restaurant_details, "read_available_tables": read_available_tables,
    "update_booking_status": update_booking_status, "update_client_info": update_client_info,
    "update_order_status": update_order_status, "update_menu_item_price": update_menu_item_price,
    "delete_review": delete_review, "cancel_booking": cancel_booking,
    "remove_menu_item": remove_menu_item,
}

# --- Функция симуляции одного пользователя ---
async def simulate_user_activity(pool, user_id, stop_event):
    # print(f"User {user_id}: Started simulation.") # Можно раскомментировать для детального лога
    initial_delay = random.uniform(0.01, 0.5)
    await asyncio.sleep(initial_delay)

    valid_query_names = [name for name in QUERY_WEIGHTS.keys() if name in QUERY_FUNCTION_MAP]
    if not valid_query_names:
        print(f"User {user_id}: ERROR - No valid actions. Check QUERY_WEIGHTS & QUERY_FUNCTION_MAP. Stopping.")
        return

    valid_query_weights = [QUERY_WEIGHTS[name] for name in valid_query_names]
    action_count = 0

    while not stop_event.is_set():
        chosen_action_name = None
        try:
            if not valid_query_names or not valid_query_weights :
                 print(f"User {user_id}: No actions available. Waiting...")
                 await asyncio.sleep(1)
                 continue
            chosen_action_name = random.choices(valid_query_names, weights=valid_query_weights, k=1)[0]
            action_func = QUERY_FUNCTION_MAP[chosen_action_name]
        except Exception as e_choice: # Ловим ошибки выбора действия
            print(f"User {user_id}: ERROR choosing action (names: {len(valid_query_names)}, weights: {len(valid_query_weights)}): {e_choice}")
            await asyncio.sleep(1)
            continue

        conn = None
        try:
            conn = await pool.acquire()
            async with conn.cursor() as cursor:
                 # print(f"User {user_id}: -> {chosen_action_name}") # Детальный лог
                 await action_func(cursor)
                 await conn.commit()
                 action_count += 1
        except pyodbc.Error as db_err: # ИЗМЕНЕНО: Ловим pyodbc.Error
            print(f"User {user_id}: *** DB ERROR ({chosen_action_name}): {db_err} ***")
            if conn:
                 try: await conn.rollback()
                 except Exception as rb_exc: print(f"User {user_id}: Rollback error: {rb_exc}")
        except Exception as e:
            print(f"User {user_id}: *** GENERAL ERROR ({chosen_action_name}): {e} ***")
            traceback.print_exc()
            if conn:
                 try: await conn.rollback()
                 except Exception as rb_exc: print(f"User {user_id}: Rollback error: {rb_exc}")
        finally:
             if conn:
                 await pool.release(conn)

        if stop_event.is_set(): break
        user_think_time = random.uniform(MIN_USER_WAIT_SECONDS, MAX_USER_WAIT_SECONDS)
        try:
            await asyncio.wait_for(stop_event.wait(), timeout=user_think_time)
            break
        except asyncio.TimeoutError:
            pass
    # print(f"User {user_id}: Exited. Actions: {action_count}.") # Детальный лог

# --- Основная функция запуска симуляции ---
async def main():
    pool = None
    start_time_main = time.time()
    print(f"[{datetime.datetime.now()}] Simulation script started.")
    try:
        print(f"[{datetime.datetime.now()}] Creating connection pool (Min: {DB_POOL_MIN_SIZE}, Max: {DB_POOL_MAX_SIZE})...")
        pool = await aioodbc.create_pool(
            dsn=DB_CONN_STR,
            minsize=DB_POOL_MIN_SIZE,
            maxsize=max(DB_POOL_MAX_SIZE, NUM_CONCURRENT_USERS + 5), # Немного больше для запаса
            autocommit=False
        )
        print(f"[{datetime.datetime.now()}] Connection pool created successfully.")

        print(f"[{datetime.datetime.now()}] Starting initial ID fetch...")
        fetch_start_time = time.time()
        ids_fetched_ok = await fetch_initial_ids(pool) # Эта функция теперь более устойчива
        fetch_end_time = time.time()
        print(f"[{datetime.datetime.now()}] Initial ID fetch finished in {fetch_end_time - fetch_start_time:.2f} seconds. Success: {ids_fetched_ok}")

        # Не выходим, даже если ids_fetched_ok is False, симулятор попытается создать данные
        # if not ids_fetched_ok:
        #     print(f"[{datetime.datetime.now()}] Exiting simulation due to failure/lack of initial IDs.")
        #     return

        stop_event = asyncio.Event()
        user_tasks = []
        print(f"\n[{datetime.datetime.now()}] Starting simulation: {NUM_CONCURRENT_USERS} users, {SIMULATION_DURATION_SECONDS}s duration...")
        sim_start_time = time.time()
        for i in range(NUM_CONCURRENT_USERS):
            task = asyncio.create_task(simulate_user_activity(pool, i + 1, stop_event))
            user_tasks.append(task)

        try:
            await asyncio.wait_for(stop_event.wait(), timeout=SIMULATION_DURATION_SECONDS)
            # print(f"\n[{datetime.datetime.now()}] Stop event set or timeout early.")
        except asyncio.TimeoutError:
            print(f"\n[{datetime.datetime.now()}] Simulation duration ({SIMULATION_DURATION_SECONDS}s) elapsed.")

        if not stop_event.is_set():
             print(f"[{datetime.datetime.now()}] Setting stop event to terminate user tasks...")
             stop_event.set()

        print(f"[{datetime.datetime.now()}] Waiting for user tasks to complete...")
        results = await asyncio.gather(*user_tasks, return_exceptions=True)
        print(f"[{datetime.datetime.now()}] All user tasks completed.")

        errors_count = sum(1 for r in results if isinstance(r, Exception))
        if errors_count > 0:
            print(f"[{datetime.datetime.now()}] Warning: {errors_count} user tasks finished with errors.")
            # for i, r_exc in enumerate(results): # Для детального вывода ошибок по задачам
            #     if isinstance(r_exc, Exception):
            #         print(f"  Error in user task {i+1}: {r_exc}")


        sim_end_time = time.time()
        # Сбор статистики по action_count может быть сложным, если задачи завершились с ошибкой до инициализации action_count
        # Этот способ примерный и может не сработать для всех случаев.
        total_actions_sim = 0
        for task in user_tasks:
            try:
                # Пытаемся получить результат, чтобы убедиться, что задача не упала до того, как action_count был доступен
                task.result() # Это выбросит исключение, если оно было в задаче
                # Доступ к f_locals очень интроспективный и может быть ненадежным/измениться
                if hasattr(task.get_coro(), 'cr_frame') and task.get_coro().cr_frame is not None:
                     action_count_val = task.get_coro().cr_frame.f_locals.get('action_count', 0)
                     total_actions_sim += action_count_val
            except Exception: # Если задача упала, ее action_count не учитываем или считаем 0
                pass

        print(f"\n[{datetime.datetime.now()}] Simulation finished. Actual duration: {sim_end_time - sim_start_time:.2f} seconds. Approx total actions: {total_actions_sim}")

    except Exception as e:
        print(f"[{datetime.datetime.now()}] *** An error occurred in the main simulation loop: {e} ***")
        traceback.print_exc()
    finally:
        if pool:
            print(f"[{datetime.datetime.now()}] Closing connection pool...")
            pool.close()
            await pool.wait_closed()
            print(f"[{datetime.datetime.now()}] Connection pool closed.")
        else:
            print(f"[{datetime.datetime.now()}] Connection pool was not created or already closed.")

    end_time_main = time.time()
    print(f"[{datetime.datetime.now()}] Simulation script finished. Total execution time: {end_time_main - start_time_main:.2f} seconds.")

if __name__ == "__main__":
    # Убедитесь, что config.py существует и содержит актуальные DB_CONN_STR и QUERY_WEIGHTS
    # а также что структура БД (имена таблиц, столбцов, типы, длины, IDENTITY)
    # соответствует ожиданиям этого симулятора.
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\nSimulation interrupted by user (Ctrl+C). Exiting gracefully...")
    except Exception as e_global: # Глобальный отловщик на всякий случай
        print(f"Unhandled exception in __main__ or asyncio.run: {e_global}")
        traceback.print_exc()