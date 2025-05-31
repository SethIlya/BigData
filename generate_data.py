import asyncio
import aioodbc
import random
import time
import datetime
import traceback
from faker import Faker
from decimal import Decimal

# КОНФИГУРАЦИЯ 
try:
    from config_generator import (
        DB_CONN_STR, DATE_RANGE_YEARS, NUM_CLIENTS_TO_GENERATE,
        NUM_RESTAURANTS_TO_GENERATE, AVG_TABLES_PER_RESTAURANT,
        AVG_MENU_ITEMS_PER_RESTAURANT, AVG_BOOKINGS_PER_CLIENT_OR_TABLE,
        CHANCE_BOOKING_HAS_ORDER, AVG_ITEMS_PER_ORDER,
        AVG_REVIEWS_PER_RESTAURANT_OR_CLIENT, BATCH_SIZE
    )
except ImportError:
    print("WARNING: config_generator.py not found or incomplete. Using default generation parameters.")
    DB_CONN_STR = "DRIVER={ODBC Driver 17 for SQL Server};SERVER=ACER;DATABASE=TestDB;Trusted_Connection=yes;"
    DATE_RANGE_YEARS = 25
    NUM_CLIENTS_TO_GENERATE = 500
    NUM_RESTAURANTS_TO_GENERATE = 500
    AVG_TABLES_PER_RESTAURANT = 1
    AVG_MENU_ITEMS_PER_RESTAURANT = 1
    AVG_BOOKINGS_PER_CLIENT_OR_TABLE = 1
    CHANCE_BOOKING_HAS_ORDER = 1
    AVG_ITEMS_PER_ORDER = 1
    AVG_REVIEWS_PER_RESTAURANT_OR_CLIENT = 1
    BATCH_SIZE = 100

fake = Faker('ru_RU')

#  Глобальные списки для хранения ID 
existing_client_ids = []
existing_restaurant_ids = []
existing_table_ids = []
existing_menu_ids = []
existing_booking_status_ids = {}
existing_booking_ids = []
existing_order_ids = []
existing_review_ids = []


client_registration_dates_cache = {}
restaurant_menu_items_cache = {}
menu_item_prices_cache = {} # menu_id: Decimal price

#  Счетчики для ID 
next_available_client_id_counter = 1
next_available_restaurant_id_counter = 1
next_available_table_id_counter = 1
next_available_booking_status_id_counter = 1

#  Константы для длины полей 
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

# Глобальный словарь target_statuses для использования в populate и generate_bookings
target_statuses = {
    "В ожидании": 1, "Подтвержено": 2, "Отменено": 3, "Завершено": 4
}

async def get_db_pool():
    try:
        pool = await aioodbc.create_pool(dsn=DB_CONN_STR, autocommit=False, minsize=2, maxsize=10)
        print("Connection pool created successfully.")
        return pool
    except Exception as e:
        print(f"FATAL: Could not create connection pool: {e}")
        traceback.print_exc()
        exit(1)

async def fetch_existing_ids_and_cache(pool):
    global existing_client_ids, existing_restaurant_ids, existing_table_ids, existing_menu_ids
    global existing_booking_status_ids, existing_booking_ids, existing_order_ids, existing_review_ids
    global client_registration_dates_cache, restaurant_menu_items_cache, menu_item_prices_cache
    global next_available_client_id_counter, next_available_restaurant_id_counter
    global next_available_table_id_counter, next_available_booking_status_id_counter
    print("Fetching existing IDs and caching data...")
    try:
        async with pool.acquire() as conn:
            async with conn.cursor() as cur:
                await cur.execute("SELECT StatusID, status_name FROM dbo.Booking_status")
                current_booking_status_ids_in_db = []
                temp_bs_cache = {} # Временный для имен из БД
                for row_id, row_name in await cur.fetchall():
                    current_booking_status_ids_in_db.append(row_id)
                    temp_bs_cache[row_id] = row_name.strip() if row_name else ""
                
                for name_key, id_val in target_statuses.items():
                    if id_val in temp_bs_cache:
                        existing_booking_status_ids[name_key] = id_val
                if current_booking_status_ids_in_db:
                    next_available_booking_status_id_counter = max(current_booking_status_ids_in_db) + 1
                else:
                    next_available_booking_status_id_counter = 1 # Начнется с 1, если target_statuses пуст или не совпадает
                print(f"  Found {len(temp_bs_cache)} booking statuses in DB. Next potential StatusID: {next_available_booking_status_id_counter}. Cache size: {len(existing_booking_status_ids)}")


                await cur.execute("SELECT ClientID, registration_date FROM dbo.Client")
                current_client_ids_in_db = []
                for row in await cur.fetchall():
                    current_client_ids_in_db.append(row[0])
                    client_registration_dates_cache[row[0]] = row[1]
                existing_client_ids.extend(current_client_ids_in_db)
                if current_client_ids_in_db:
                    next_available_client_id_counter = max(current_client_ids_in_db) + 1
                else:
                    next_available_client_id_counter = 1
                print(f"  Found and cached {len(existing_client_ids)} clients. Next ClientID: {next_available_client_id_counter}")

                await cur.execute("SELECT RestarauntID FROM dbo.Restaraunt")
                current_restaurant_ids_in_db = [row[0] for row in await cur.fetchall()]
                existing_restaurant_ids.extend(current_restaurant_ids_in_db)
                if current_restaurant_ids_in_db:
                    next_available_restaurant_id_counter = max(current_restaurant_ids_in_db) + 1
                else:
                    next_available_restaurant_id_counter = 1
                print(f"  Found {len(existing_restaurant_ids)} restaurants. Next RestarauntID: {next_available_restaurant_id_counter}")

                await cur.execute("SELECT TableID FROM dbo.RestaurantTable")
                current_table_ids_in_db = [row[0] for row in await cur.fetchall()]
                existing_table_ids.extend(current_table_ids_in_db)
                if current_table_ids_in_db:
                    next_available_table_id_counter = max(current_table_ids_in_db) + 1
                else:
                    next_available_table_id_counter = 1
                print(f"  Found {len(existing_table_ids)} restaurant tables. Next TableID: {next_available_table_id_counter}")

                await cur.execute("SELECT MenuID, RestaurantID, Price FROM dbo.Menu")
                for row in await cur.fetchall():
                    menu_id, rest_id, price = row[0], row[1], Decimal(row[2])
                    existing_menu_ids.append(menu_id)
                    menu_item_prices_cache[menu_id] = price # Храним Decimal
                    if rest_id not in restaurant_menu_items_cache:
                        restaurant_menu_items_cache[rest_id] = []
                    restaurant_menu_items_cache[rest_id].append(menu_id)
                print(f"  Found and cached {len(existing_menu_ids)} menu items.")

                await cur.execute("SELECT BookingID FROM dbo.Booking")
                existing_booking_ids = [row[0] for row in await cur.fetchall()]
                print(f"  Found {len(existing_booking_ids)} bookings.")

                await cur.execute("SELECT OrderID FROM dbo.Orders")
                existing_order_ids = [row[0] for row in await cur.fetchall()]
                print(f"  Found {len(existing_order_ids)} orders from dbo.Orders.")

                await cur.execute("SELECT ReviewID FROM dbo.Review")
                existing_review_ids = [row[0] for row in await cur.fetchall()]
                print(f"  Found {len(existing_review_ids)} reviews.")
    except Exception as e:
        print(f"Error fetching/caching: {e}")
        traceback.print_exc()

def generate_random_date(start_year_offset=DATE_RANGE_YEARS, end_year_offset=0, past_only=True):
    if start_year_offset == 0 and end_year_offset == 0 and past_only: # Для дат "в этом году"
        return fake.date_time_this_decade(before_now=True, after_now=False) # Ближе к date_time_this_year
    try:
        now = datetime.datetime.now()
        current_year = now.year
        effective_start_offset = max(0, start_year_offset)
        start_date_dt = datetime.datetime(current_year - effective_start_offset, 1, 1)

        if past_only:
            effective_end_offset = max(0, end_year_offset)
            end_date_dt = now - datetime.timedelta(days=effective_end_offset * 365)
        else:
            effective_end_offset_days = end_year_offset
            end_date_dt = now + datetime.timedelta(days=effective_end_offset_days * 365 if effective_end_offset_days > 0 else effective_end_offset_days)

        if start_date_dt > end_date_dt:
            if past_only:
                end_date_dt = now
                if start_date_dt > end_date_dt : start_date_dt = end_date_dt - datetime.timedelta(days=1)
            else:
                temp = start_date_dt
                start_date_dt = end_date_dt
                end_date_dt = temp
                if start_date_dt > end_date_dt:
                     start_date_dt = now - datetime.timedelta(days=1)
                     end_date_dt = now + datetime.timedelta(days=1)

        time_between_dates = end_date_dt - start_date_dt
        if time_between_dates.total_seconds() < 0:
            return fake.date_time_this_decade(before_now=True, after_now=False)

        random_seconds_offset = random.uniform(0, time_between_dates.total_seconds())
        random_date_dt = start_date_dt + datetime.timedelta(seconds=random_seconds_offset)
        return random_date_dt.replace(microsecond=0)
    except Exception:
        return fake.date_time_this_decade(before_now=True, after_now=False)


async def populate_booking_statuses(pool):
    global existing_booking_status_ids, next_available_booking_status_id_counter
    print("Populating/Verifying Booking_status table...")
    async with pool.acquire() as conn:
        async with conn.cursor() as cur:
            db_statuses_by_id = {}
            db_statuses_by_name = {}
            await cur.execute("SELECT StatusID, status_name FROM dbo.Booking_status")
            for db_id, db_name_raw in await cur.fetchall():
                db_name = db_name_raw.strip() if db_name_raw else ""
                db_statuses_by_id[db_id] = db_name
                db_statuses_by_name[db_name] = db_id

            made_changes = False
            for name_key, id_val in target_statuses.items():
                name_to_use = name_key[:DB_BOOKING_STATUS_NAME_MAX_LEN].strip()

                if id_val in db_statuses_by_id:
                    actual_name_in_db = db_statuses_by_id[id_val]
                    if actual_name_in_db != name_to_use:
                        print(f"  Info: Booking_status ID {id_val} exists as '{actual_name_in_db}', script uses '{name_to_use}'.")
                        # Попытка обновления имени, если оно не совпадает и ID тот же, и длина позволяет
                        if len(name_to_use) <= DB_BOOKING_STATUS_NAME_MAX_LEN:
                            try:
                                if actual_name_in_db.lower() != name_to_use.lower():
                                    print(f"    Attempting to update name for StatusID {id_val} to '{name_to_use}'")
                                    await cur.execute("UPDATE dbo.Booking_status SET status_name = ? WHERE StatusID = ?", name_to_use, id_val)
                                    db_statuses_by_id[id_val] = name_to_use
                                    if actual_name_in_db in db_statuses_by_name and actual_name_in_db != name_to_use :
                                        del db_statuses_by_name[actual_name_in_db]
                                    db_statuses_by_name[name_to_use] = id_val
                                    made_changes = True
                            except Exception as e_update:
                                print(f"    Failed to update name for StatusID {id_val}: {e_update}")
                    existing_booking_status_ids[name_key] = id_val
                elif name_to_use in db_statuses_by_name:
                    actual_id_in_db = db_statuses_by_name[name_to_use]
                    print(f"  Warning: Status name '{name_to_use}' exists with ID {actual_id_in_db}, script expects {id_val}. Using existing DB ID {actual_id_in_db} for '{name_key}'.")
                    existing_booking_status_ids[name_key] = actual_id_in_db
                else:
                    id_to_insert = id_val
                    if id_val in db_statuses_by_id: # Этот ID уже занят другим именем, генерируем новый
                        id_to_insert = next_available_booking_status_id_counter
                        print(f"  Info: Target ID {id_val} for '{name_to_use}' is taken. Using next available ID {id_to_insert}.")
                        next_available_booking_status_id_counter +=1
                    
                    try:
                        print(f"  Attempting to insert status: ID={id_to_insert}, Name='{name_to_use}'")
                        await cur.execute("INSERT INTO dbo.Booking_status (StatusID, status_name) VALUES (?, ?)", id_to_insert, name_to_use)
                        existing_booking_status_ids[name_key] = id_to_insert
                        db_statuses_by_id[id_to_insert] = name_to_use
                        db_statuses_by_name[name_to_use] = id_to_insert
                        made_changes = True
                        print(f"    Added booking status: '{name_to_use}' (ID: {id_to_insert})")
                    except Exception as e_insert:
                        print(f"    Could not insert status '{name_to_use}' with ID {id_to_insert}: {e_insert}")
                        await cur.execute("SELECT status_name FROM dbo.Booking_status WHERE StatusID = ?", id_to_insert)
                        row_check = await cur.fetchone()
                        if row_check:
                            print(f"    StatusID {id_to_insert} found in DB after failed insert with name '{row_check[0]}'. Updating cache.")
                            existing_booking_status_ids[name_key] = id_to_insert

            if made_changes:
                await conn.commit()
    print("Booking_status table population attempt finished.")
    for name_key, id_val in target_statuses.items():
        if name_key not in existing_booking_status_ids:
            print(f"  CRITICAL WARNING: Target status '{name_key}' (expected ID {id_val}) IS NOT IN CACHE!")
        elif existing_booking_status_ids[name_key] != id_val:
            print(f"  Cache Info: Target status '{name_key}' (expected ID {id_val}) is cached with ID {existing_booking_status_ids[name_key]}. This can happen if name exists with different ID in DB or generated new ID.")


async def generate_clients(pool, num_clients):
    # ... (код без изменений) ...
    global next_available_client_id_counter
    if num_clients == 0: return []
    print(f"Generating {num_clients} clients...")
    clients_data_with_ids = []
    newly_generated_ids = []

    for _ in range(num_clients):
        reg_date = fake.date_time_this_decade(before_now=True, after_now=False) 
        raw_phone_number = fake.msisdn() 
        display_phone = raw_phone_number[:DB_CLIENT_PHONE_MAX_LEN] # Обрезка
        current_client_id = next_available_client_id_counter
        clients_data_with_ids.append((
            current_client_id,
            fake.name()[:DB_CLIENT_NAME_MAX_LEN],
            display_phone,
            fake.email()[:DB_CLIENT_EMAIL_MAX_LEN],
            reg_date
        ))
        next_available_client_id_counter += 1

    async with pool.acquire() as conn:
        async with conn.cursor() as cur:
            inserted_count = 0
            for client_tuple_with_id in clients_data_with_ids:
                try:
                    await cur.execute(
                        "INSERT INTO dbo.Client (ClientID, Name, phone, email, registration_date) VALUES (?, ?, ?, ?, ?);",
                        *client_tuple_with_id
                    )
                    new_id = client_tuple_with_id[0]
                    newly_generated_ids.append(new_id)
                    existing_client_ids.append(new_id)
                    client_registration_dates_cache[new_id] = client_tuple_with_id[4]
                    inserted_count +=1
                except Exception as e:
                    print(f"  Error inserting client with explicit ID {client_tuple_with_id[0]} (Phone: {client_tuple_with_id[2]}, Email: {client_tuple_with_id[3]}): {e}")
            if inserted_count > 0: await conn.commit()
            else: await conn.rollback()
    print(f"  Generated/inserted {len(newly_generated_ids)} clients with explicit IDs.")
    return newly_generated_ids


async def generate_restaurants(pool, num_restaurants):
    global next_available_restaurant_id_counter
    if num_restaurants == 0: return []
    print(f"Generating {num_restaurants} restaurants...")
    restaurants_data_with_ids = []
    newly_generated_ids = []
    cuisines = ["итальянская", "китайская", "мексиканская", "японская", "французская", "индийская"] 
    for _ in range(num_restaurants):
        current_restaurant_id = next_available_restaurant_id_counter
        restaurants_data_with_ids.append((
            current_restaurant_id,
            fake.company()[:DB_RESTAURANT_NAME_MAX_LEN],
            fake.address().replace("\n", ", ")[:DB_RESTAURANT_ADDRES_MAX_LEN], 
            random.choice(cuisines)[:DB_RESTAURANT_CUISINE_MAX_LEN] 
        ))
        next_available_restaurant_id_counter += 1

    if not restaurants_data_with_ids: return []
    async with pool.acquire() as conn:
        async with conn.cursor() as cur:
            inserted_count = 0
            for rest_tuple_with_id in restaurants_data_with_ids:
                try:
                    await cur.execute(
                        "INSERT INTO dbo.Restaraunt (RestarauntID, Name, addres, cuisine) VALUES (?, ?, ?, ?);",
                        *rest_tuple_with_id
                    )
                    new_id = rest_tuple_with_id[0]
                    newly_generated_ids.append(new_id)
                    existing_restaurant_ids.append(new_id)
                    inserted_count += 1
                except Exception as e:
                    print(f"  Error inserting restaurant with explicit ID {rest_tuple_with_id[0]} (Name: {rest_tuple_with_id[1]}, Addr: {rest_tuple_with_id[2]}): {e}")
            if inserted_count > 0: await conn.commit()
            else: await conn.rollback()
    print(f"  Generated/inserted {len(newly_generated_ids)} restaurants with explicit IDs.")
    return newly_generated_ids


async def generate_restaurant_tables(pool, avg_tables_per_restaurant):
    global next_available_table_id_counter
    if not existing_restaurant_ids or avg_tables_per_restaurant == 0: return []

    num_tables_target = NUM_CLIENTS_TO_GENERATE 
    if num_tables_target == 0 and avg_tables_per_restaurant > 0: 
        num_tables_target = int(len(existing_restaurant_ids) * avg_tables_per_restaurant)
    if num_tables_target == 0 : return []


    print(f"Generating approximately {num_tables_target} restaurant tables...")
    tables_data_with_ids = []
    newly_generated_table_ids_total = []

    for _ in range(num_tables_target):
        if not existing_restaurant_ids: break 
        current_table_id = next_available_table_id_counter
        rest_id = random.choice(existing_restaurant_ids)
        tables_data_with_ids.append((current_table_id, rest_id, random.choice([2, 4, 6, 8, 10])))
        next_available_table_id_counter += 1


    if not tables_data_with_ids: return []
    async with pool.acquire() as conn:
        async with conn.cursor() as cur:
            inserted_count = 0
            for table_tuple_with_id in tables_data_with_ids:
                try:
                    await cur.execute(
                        "INSERT INTO dbo.RestaurantTable (TableID, RestaurantID, max) VALUES (?, ?, ?);",
                        *table_tuple_with_id
                    )
                    new_id = table_tuple_with_id[0]
                    newly_generated_table_ids_total.append(new_id)
                    existing_table_ids.append(new_id)
                    inserted_count += 1
                except Exception as e:
                    print(f"  Error inserting restaurant table with explicit ID {table_tuple_with_id[0]}: {e}")
            if inserted_count > 0: await conn.commit()
            else: await conn.rollback()
    print(f"  Generated/inserted {len(newly_generated_table_ids_total)} restaurant tables with explicit IDs.")
    return newly_generated_table_ids_total


async def generate_menu_items(pool, avg_items_per_restaurant):
    if not existing_restaurant_ids or avg_items_per_restaurant == 0: return []
    
    num_menu_items_target = NUM_CLIENTS_TO_GENERATE 
    if num_menu_items_target == 0 and avg_items_per_restaurant > 0 :
        num_menu_items_target = int(len(existing_restaurant_ids) * avg_items_per_restaurant)
    if num_menu_items_target == 0 : return []

    print(f"Generating approximately {num_menu_items_target} menu items...")
    menu_data_to_insert = []
    newly_generated_menu_ids_total = []
    
    generated_count = 0
    while generated_count < num_menu_items_target and existing_restaurant_ids :
        rest_id = random.choice(existing_restaurant_ids)
        price_decimal = Decimal(round(random.uniform(5, 100), 2))
        price_str = str(price_decimal)
        dish_name = fake.word().capitalize()[:DB_MENU_DISHNAME_MAX_LEN]
        menu_data_to_insert.append((rest_id, dish_name, price_str)) 
        generated_count +=1
        if generated_count >= num_menu_items_target : break

    if not menu_data_to_insert: return []
    async with pool.acquire() as conn:
        async with conn.cursor() as cur:
            inserted_count = 0
            for item_tuple in menu_data_to_insert: 
                try:
 
                    await cur.execute(
                        "SET NOCOUNT ON; INSERT INTO dbo.Menu (RestaurantID, DishName, Price) VALUES (?, ?, ?); SELECT SCOPE_IDENTITY();",
                        item_tuple[0], item_tuple[1], item_tuple[2] 
                    )
                    new_id_row = await cur.fetchone()
                    if new_id_row and new_id_row[0] is not None:
                        new_id = int(new_id_row[0])
                        newly_generated_menu_ids_total.append(new_id)
                        existing_menu_ids.append(new_id)
                        menu_item_prices_cache[new_id] = Decimal(item_tuple[2])
                        rest_id_for_menu = item_tuple[0]
                        if rest_id_for_menu not in restaurant_menu_items_cache:
                            restaurant_menu_items_cache[rest_id_for_menu] = []
                        restaurant_menu_items_cache[rest_id_for_menu].append(new_id)
                        inserted_count += 1
                except Exception as e:
                    print(f"  Error inserting menu item (RestID: {item_tuple[0]}, Name: {item_tuple[1]}, PriceStr: {item_tuple[2]}): {e}")
            if inserted_count > 0: await conn.commit()
            else: await conn.rollback()
    print(f"  Generated/inserted {len(newly_generated_menu_ids_total)} menu items.")
    return newly_generated_menu_ids_total

async def generate_bookings(pool, avg_bookings_param):
    if not existing_client_ids or not existing_table_ids or not existing_booking_status_ids: return []
    num_bookings_target = NUM_CLIENTS_TO_GENERATE
    if num_bookings_target == 0 and avg_bookings_param > 0:
        num_bookings_target = int(min(len(existing_client_ids) * 5, len(existing_table_ids) * 3) * avg_bookings_param)
    if num_bookings_target == 0: return []


    print(f"Generating {num_bookings_target} bookings...")
    bookings_data = []
    newly_generated_booking_ids_total = []

    status_ids_for_choice = [existing_booking_status_ids[name] for name in target_statuses.keys() if name in existing_booking_status_ids]
    if not status_ids_for_choice:
        print("  Error: No valid status IDs found in cache for booking generation. Check populate_booking_statuses.")
        return []

    for _ in range(num_bookings_target):
        if not existing_client_ids or not status_ids_for_choice or not existing_table_ids: break
        client_id = random.choice(existing_client_ids)
        status_id = random.choice(status_ids_for_choice)
        table_id = random.choice(existing_table_ids)
        booking_date = fake.date_time_this_decade(before_now=True, after_now=False) 
        bookings_data.append((client_id, status_id, table_id, booking_date))

    if not bookings_data: return []
    async with pool.acquire() as conn:
        async with conn.cursor() as cur:
            inserted_count = 0
            for book_tuple in bookings_data:
                try:
                    await cur.execute(
                        "SET NOCOUNT ON; INSERT INTO dbo.Booking (ClientID, StatusID, TableID, BookingDate) VALUES (?, ?, ?, ?); SELECT SCOPE_IDENTITY();",
                        *book_tuple
                    )
                    new_id_row = await cur.fetchone()
                    if new_id_row and new_id_row[0] is not None:
                        new_id = int(new_id_row[0])
                        newly_generated_booking_ids_total.append(new_id)
                        existing_booking_ids.append(new_id)
                        inserted_count +=1
                except Exception as e:
                     print(f"  Error inserting booking: {e}")
            if inserted_count > 0: await conn.commit()
            else: await conn.rollback()
    print(f"  Generated/inserted {len(newly_generated_booking_ids_total)} bookings.")
    return newly_generated_booking_ids_total


async def generate_orders_and_related(pool, chance_booking_has_order, avg_items_per_order):
    num_orders_target = NUM_CLIENTS_TO_GENERATE # Ориентир
    if not existing_booking_ids or not existing_menu_ids: return
    if num_orders_target == 0 and chance_booking_has_order > 0:
        num_orders_target = int(len(existing_booking_ids) * chance_booking_has_order)
    if num_orders_target == 0: return


    print(f"Attempting to generate {num_orders_target} orders...")

    order_status_choices = [1, 2, 3] 
    payment_methods = ["Наличные", "Карта", "Онлайн"]
    payment_statuses = ["Оплачено", "В ожидании", "Не прошло"]

    orders_created_count = 0
    generated_order_data = []
    generated_orderitem_data = []
    generated_payment_data = []
    temp_order_index = 0

    for _ in range(num_orders_target):
        if not existing_booking_ids or not existing_menu_ids: break
        booking_id = random.choice(existing_booking_ids)
        status_order = random.choice(order_status_choices)
        
        menu_id_for_item = random.choice(existing_menu_ids)
        # Price_At_Order в OrdersItem в int
        price_at_order_for_item = random.randint(1, 10) 

        # total_price в Orders у  int
        total_price_for_order = price_at_order_for_item 

        generated_order_data.append((booking_id, status_order, total_price_for_order))
        generated_orderitem_data.append((temp_order_index, menu_id_for_item, price_at_order_for_item))

        method = random.choice(payment_methods)[:DB_PAYMENT_METHOD_MAX_LEN]
        pay_status = random.choice(payment_statuses)[:DB_PAYMENT_STATUS_MAX_LEN]
        generated_payment_data.append((temp_order_index, method, pay_status))
        temp_order_index +=1

    async with pool.acquire() as conn:
        async with conn.cursor() as cur:
            final_orderitem_data = []
            final_payment_data = []
            for idx, order_data_tuple in enumerate(generated_order_data):
                new_order_id = None
                try:
                    await cur.execute(
                        "SET NOCOUNT ON; INSERT INTO dbo.Orders (BookingID, StatusOrder, total_price) VALUES (?, ?, ?); SELECT SCOPE_IDENTITY();",
                        *order_data_tuple
                    )
                    new_order_id_row = await cur.fetchone()
                    if not (new_order_id_row and new_order_id_row[0] is not None):
                        print(f"  Skipped order item/payment due to failed Order insert for booking {order_data_tuple[0]}")
                        continue
                    new_order_id = int(new_order_id_row[0])
                    existing_order_ids.append(new_order_id)
                    orders_created_count +=1

                    for oi_idx, oi_menu_id, oi_price in generated_orderitem_data:
                        if oi_idx == idx:
                            final_orderitem_data.append((new_order_id, oi_menu_id, oi_price))
                    for p_idx, p_method, p_status in generated_payment_data:
                        if p_idx == idx:
                            final_payment_data.append((new_order_id, p_method, p_status))
                except Exception as e_order:
                    print(f"  Error inserting order for booking {order_data_tuple[0]}: {e_order}")

            if final_orderitem_data:
                try:
                    await cur.executemany(
                        "INSERT INTO dbo.OrdersItem (OrderID, MenuID, Price_At_Order) VALUES (?, ?, ?)",
                        final_orderitem_data
                    )
                    print(f"    Inserted {len(final_orderitem_data)} order items.")
                except Exception as e_oi:
                    print(f"    Error inserting order items: {e_oi}")
            if final_payment_data:
                try:
                    await cur.executemany(
                        "INSERT INTO dbo.Payments (OrderID, Method, Payment_Status) VALUES (?, ?, ?)",
                        final_payment_data
                    )
                    print(f"    Inserted {len(final_payment_data)} payments.")
                except Exception as e_p:
                    print(f"    Error inserting payments: {e_p}")

            if orders_created_count > 0 or final_orderitem_data or final_payment_data:
                 await conn.commit()
            else:
                 await conn.rollback()
    print(f"  Generated/inserted {orders_created_count} orders and their related items/payments.")


async def generate_reviews(pool, avg_reviews_param):
    num_reviews_target = NUM_CLIENTS_TO_GENERATE
    if not existing_client_ids or not existing_restaurant_ids: return
    if num_reviews_target == 0 and avg_reviews_param > 0:
        num_reviews_target = int(min(len(existing_client_ids), len(existing_restaurant_ids) * 10) * avg_reviews_param)
    if num_reviews_target == 0: return

    print(f"Generating {num_reviews_target} reviews...")
    reviews_data = []

    for _ in range(num_reviews_target):
        if not existing_client_ids or not existing_restaurant_ids: break
        client_id = random.choice(existing_client_ids)
        restaurant_id = random.choice(existing_restaurant_ids)
        rating = random.randint(1, 5)
        comment = fake.text(max_nb_chars=DB_REVIEW_COMMENT_MAX_LEN)
        review_date = fake.date_time_this_decade(before_now=True, after_now=False) 
        reviews_data.append((client_id, restaurant_id, rating, comment, review_date))

    if not reviews_data: return
    async with pool.acquire() as conn:
        async with conn.cursor() as cur:
            inserted_count = 0
            for review_tuple in reviews_data:
                try:
                    await cur.execute(
                        "SET NOCOUNT ON; INSERT INTO dbo.Review (ClientID, RestaurantID, Rating, Comment, ReviewDate) VALUES (?, ?, ?, ?, ?); SELECT SCOPE_IDENTITY();",
                        *review_tuple
                    )
                    new_id_row = await cur.fetchone()
                    if new_id_row and new_id_row[0] is not None:
                         existing_review_ids.append(int(new_id_row[0]))
                    inserted_count += 1
                except Exception as e:
                    print(f"  Error inserting review (Client: {review_tuple[0]}, Rest: {review_tuple[1]}): {e}")
            if inserted_count > 0: await conn.commit()
            else: await conn.rollback()
            print(f"  Generated/inserted {inserted_count} reviews.")


async def main_generate():
    start_time = time.time()
    print(f"[{datetime.datetime.now()}] Data generation script started.")
    pool = await get_db_pool()
    if not pool: return

    try:
        # fetch_existing_ids_and_cache должен быть первым, чтобы инициализировать счетчики
        await fetch_existing_ids_and_cache(pool)
        # populate_booking_statuses использует свой счетчик или вставляет по ID из target_statuses
        await populate_booking_statuses(pool)

        if NUM_CLIENTS_TO_GENERATE > 0:
            await generate_clients(pool, NUM_CLIENTS_TO_GENERATE)
        if NUM_RESTAURANTS_TO_GENERATE > 0:
            await generate_restaurants(pool, NUM_RESTAURANTS_TO_GENERATE)
        if AVG_TABLES_PER_RESTAURANT > 0 and existing_restaurant_ids: # AVG_TABLES_PER_RESTAURANT из конфига
            await generate_restaurant_tables(pool, AVG_TABLES_PER_RESTAURANT)
        
        if AVG_MENU_ITEMS_PER_RESTAURANT > 0 and existing_restaurant_ids: # AVG_MENU_ITEMS_PER_RESTAURANT из конфига
            await generate_menu_items(pool, AVG_MENU_ITEMS_PER_RESTAURANT)
        
        if AVG_BOOKINGS_PER_CLIENT_OR_TABLE > 0 and existing_client_ids and existing_table_ids and existing_booking_status_ids:
            await generate_bookings(pool, AVG_BOOKINGS_PER_CLIENT_OR_TABLE)
        
        if CHANCE_BOOKING_HAS_ORDER > 0 and AVG_ITEMS_PER_ORDER > 0 and existing_booking_ids and existing_menu_ids and existing_booking_status_ids:
            await generate_orders_and_related(pool, CHANCE_BOOKING_HAS_ORDER, AVG_ITEMS_PER_ORDER)
        
        if AVG_REVIEWS_PER_RESTAURANT_OR_CLIENT > 0 and existing_client_ids and existing_restaurant_ids:
            await generate_reviews(pool, AVG_REVIEWS_PER_RESTAURANT_OR_CLIENT)
        
        print("Data generation phase complete.")
    except Exception as e:
        print(f"Error in main_generate: {e}")
        traceback.print_exc()
    finally:
        if pool:
            print("Closing connection pool...")
            pool.close()
            await pool.wait_closed()
            print("Connection pool closed.")
    end_time = time.time()
    print(f"[{datetime.datetime.now()}] Script finished. Total time: {end_time - start_time:.2f}s.")

if __name__ == "__main__":
    print(f"Using DATE_RANGE_YEARS = {DATE_RANGE_YEARS} for data span.")
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
    # ----------------------------------------------------------------------------------
    print(f"Booking_status.status_name will be trimmed to {DB_BOOKING_STATUS_NAME_MAX_LEN} chars.")
    print(f"Client.Name will be trimmed to {DB_CLIENT_NAME_MAX_LEN} chars.")


    try:
        asyncio.run(main_generate())
    except KeyboardInterrupt:
        print("\nData generation interrupted.")
    except Exception as e_main:
        print(f"Unhandled exception in __main__: {e_main}")
        traceback.print_exc()