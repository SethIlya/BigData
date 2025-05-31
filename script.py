
import pyodbc 
from pymongo import MongoClient
from bson.objectid import ObjectId 
from datetime import datetime 

sql_conn_str = (
    "DRIVER={ODBC Driver 17 for SQL Server};"
    "SERVER=ACER;"
    "DATABASE=TestDB;"          
    "UID=iimin;"
    "Trusted_Connection=yes;"  

)

mongo_client = MongoClient('mongodb://localhost:27017/') 
mongo_db = mongo_client['restaurant_nosql'] 

print("Connecting to databases...")
try:
    sql_conn = pyodbc.connect(sql_conn_str)
    sql_cursor = sql_conn.cursor()
    print("Connected to SQL Server.")
except Exception as e:
    print(f"Error connecting to SQL Server: {e}")
    exit()

try:
    # Проверка соединения с MongoDB
    mongo_client.admin.command('ping')
    print("Connected to MongoDB.")
except Exception as e:
    print(f"Error connecting to MongoDB: {e}")
    sql_cursor.close()
    sql_conn.close()
    exit()

# --- Очистка коллекций в MongoDB перед миграцией
print("Dropping existing collections...")
mongo_db.booking_statuses.drop()
mongo_db.clients.drop()
mongo_db.restaurants.drop()
mongo_db.tables.drop()
mongo_db.menu_items.drop()
mongo_db.bookings.drop()
mongo_db.orders.drop()
mongo_db.payments.drop()
mongo_db.reviews.drop()
print("Collections dropped.")


# --- Миграция booking_statuses (таблица dbo.Booking_status) ---
print("Migrating booking statuses...")

sql_cursor.execute("SELECT StatusID, status_name FROM dbo.Booking_status")
status_data = sql_cursor.fetchall()
status_docs = []
for row in status_data:
    status_docs.append({
        "statusId": row.StatusID,
        "statusName": row.status_name # Используем status_name
    })
if status_docs:
    mongo_db.booking_statuses.insert_many(status_docs)
print(f"Migrated {len(status_docs)} booking statuses.")


# --- Миграция clients (таблица dbo.Client) ---
print("Migrating clients...")
sql_cursor.execute("SELECT ClientID, Name, phone, email, registration_date FROM dbo.Client")
clients_data = sql_cursor.fetchall()
clients_docs = []
for row in clients_data:
    clients_docs.append({
        "clientId": row.ClientID,
        "name": row.Name,
        "phone": row.phone,       
        "email": row.email,      
        "registrationDate": row.registration_date 
    })
if clients_docs:
    mongo_db.clients.insert_many(clients_docs)
print(f"Migrated {len(clients_docs)} clients.")


print("Migrating restaurants...")
sql_cursor.execute("SELECT RestarauntID, Name, addres, cuisine FROM dbo.Restaraunt")
restaurants_data = sql_cursor.fetchall()
restaurants_docs = []
for row in restaurants_data:
     restaurants_docs.append({
         "restaurantId": row.RestarauntID, 
         "name": row.Name,
         "address": row.addres, 
         "cuisine": row.cuisine
     })
if restaurants_docs:
      mongo_db.restaurants.insert_many(restaurants_docs)
print(f"Migrated {len(restaurants_docs)} restaurants.")


# --- Миграция tables (таблица dbo.RestaurantTable) ---
print("Migrating tables...")
sql_cursor.execute("SELECT TableID, RestaurantID, max FROM dbo.RestaurantTable")
tables_data = sql_cursor.fetchall()
tables_docs = []
for row in tables_data:
     tables_docs.append({
         "tableId": row.TableID,
         "restaurantId": row.RestaurantID,
         "capacity": row.max 
     })
if tables_docs:
      mongo_db.tables.insert_many(tables_docs)
print(f"Migrated {len(tables_docs)} tables.")


# --- Миграция menu_items (таблица dbo.Menu) ---
print("Migrating menu items...")
sql_cursor.execute("SELECT MenuID, RestaurantID, DishName, Price FROM dbo.Menu")
menu_data = sql_cursor.fetchall()
menu_docs = []
menu_names_map = {}
for row in menu_data:
    menu_docs.append({
        "menuItemId": row.MenuID,
        "restaurantId": row.RestaurantID,
        "dishName": row.DishName,
        "price": float(row.Price) 
    })
    menu_names_map[row.MenuID] = row.DishName 
if menu_docs:
     mongo_db.menu_items.insert_many(menu_docs)
print(f"Migrated {len(menu_docs)} menu items.")


# --- Миграция bookings (таблица dbo.Booking) ---
print("Migrating bookings...")
sql_cursor.execute("SELECT BookingID, ClientID, StatusID, TableID, BookingDate FROM dbo.Booking")
bookings_data = sql_cursor.fetchall()
bookings_docs = []
for row in bookings_data:
    bookings_docs.append({
        "bookingId": row.BookingID,
        "clientId": row.ClientID, # Ссылка на client (храним ID)
        "statusId": row.StatusID, # Ссылка на booking_status (храним ID)
        "tableId": row.TableID, # Ссылка на table (храним ID)
        "bookingDate": row.BookingDate # Сохраняем полную дату и время (datetime -> ISODate)
    })
if bookings_docs:
    mongo_db.bookings.insert_many(bookings_docs)
print(f"Migrated {len(bookings_docs)} bookings.")

# --- Миграция orders (таблица dbo.Orders) с встраиванием order_items (dbo.OrdersItem) ---
print("Migrating orders with embedded items...")
sql_cursor.execute("SELECT OrderID, BookingID, StatusOrder, total_price FROM dbo.Orders")
orders_data = sql_cursor.fetchall()


order_items_map = {}

sql_cursor.execute("""
    SELECT
        oi.OrderID,
        oi.MenuID,
        oi.Price_At_Order,
        m.DishName -- Получаем имя блюда для встраивания
    FROM dbo.OrdersItem oi -- Используем OrdersItem как в вашей таблице
    LEFT JOIN dbo.Menu m ON oi.MenuID = m.MenuID -- JOIN для имени блюда
""")
order_items_data = sql_cursor.fetchall()

for item_row in order_items_data:
    order_id = item_row.OrderID
    if order_id not in order_items_map:
        order_items_map[order_id] = []
    # АДАПТИРУЙТЕ ИМЕНА ПОЛЕЙ для подокумента позиции заказа
    order_items_map[order_id].append({
        "menuItemId": item_row.MenuID,
        "priceAtOrder": float(item_row.Price_At_Order) if item_row.Price_At_Order is not None else None, # Преобразуем в float, обрабатываем NULL
        "itemName": item_row.DishName if item_row.DishName is not None else "Unknown Dish" # Встраиваем имя, обрабатываем NULL
    })
print(f"Fetched and mapped {len(order_items_data)} order items.")


# Получим словарь {BookingID: {ClientID, BookingDate}} из dbo.Booking для встраивания в Orders

booking_details_map = {}
sql_cursor.execute("SELECT BookingID, ClientID, BookingDate FROM dbo.Booking WHERE ClientID IS NOT NULL") # Убираем бронирования без клиента
booking_details_data = sql_cursor.fetchall()
for row in booking_details_data:
    booking_details_map[row.BookingID] = {
        "clientId": row.ClientID,
        "bookingDate": row.BookingDate # Дата бронирования
    }
print(f"Fetched {len(booking_details_map)} booking details.")


orders_docs = []
for order_row in orders_data:
    order_id = order_row.OrderID
    booking_id = order_row.BookingID 
    total_price = float(order_row.total_price) if order_row.total_price is not None else None 

    client_id_for_order = None
    booking_date_for_order = None
    if booking_id is not None and booking_id in booking_details_map:
         client_id_for_order = booking_details_map[booking_id]["clientId"]
         booking_date_for_order = booking_details_map[booking_id]["bookingDate"]



    orders_docs.append({
        "orderId": order_id,
        "bookingId": booking_id, # Ссылка на бронирование 
        "statusOrder": order_row.StatusOrder, 
        "totalPrice": total_price,
        "clientId": client_id_for_order, 
        "orderDate": booking_date_for_order, 
        "items": order_items_map.get(order_id, []) # Встраивание массива позиций
    })

if orders_docs:
     mongo_db.orders.insert_many(orders_docs)
print(f"Migrated {len(orders_docs)} orders.")


# --- Миграция payments (таблица dbo.Payments) ---
print("Migrating payments...")
sql_cursor.execute("SELECT OrderID, Method, Payment_Status FROM dbo.Payments")
payments_data = sql_cursor.fetchall()
payments_docs = []
for row in payments_data:
     payments_docs.append({
         "_id": ObjectId(), 
         "orderId": row.OrderID,
         "method": row.Method,
         "status": row.Payment_Status
     })
if payments_docs:
      mongo_db.payments.insert_many(payments_docs)
print(f"Migrated {len(payments_docs)} payments.")


# --- Миграция reviews (таблица dbo.Review) ---
print("Migrating reviews...")
sql_cursor.execute("SELECT ClientID, RestaurantID, Rating, Comment, ReviewDate FROM dbo.Review")
reviews_data = sql_cursor.fetchall()
reviews_docs = []
for row in reviews_data:
     reviews_docs.append({
         "_id": ObjectId(),
         "clientId": row.ClientID, 
         "restaurantId": row.RestaurantID, 
         "rating": row.Rating,
         "comment": row.Comment,
         "reviewDate": row.ReviewDate
     })
if reviews_docs:
      mongo_db.reviews.insert_many(reviews_docs)
print(f"Migrated {len(reviews_docs)} reviews.")


# Закрытие соединений
print("Closing connections...")
sql_cursor.close()
sql_conn.close()
mongo_client.close()
print("Migration complete.")