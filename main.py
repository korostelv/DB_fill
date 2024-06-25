import psycopg2
from psycopg2 import Error
from faker import Faker
import random
from datetime import datetime

fake = Faker('ru_RU')

# try:
#     connection = psycopg2.connect(user="postgres",
#                                   password="12345",
#                                   host="127.0.0.1",
#                                   port="5432",
#                                   database="shops_db")
#     cursor = connection.cursor()
#
#     cursor.execute('select * from shops')
#     record = cursor.fetchall()
#     print("Результат", record)
#     for i in record:
#         print(i)
#
# except (Exception, Error) as error:
#     print("Ошибка при работе с PostgreSQL", error)


def add_data_employ(row):
    job_list = ['продавец', 'управляющий', 'бухгалтер', 'уборщик']

    try:
        connection = psycopg2.connect(user="postgres",
                                      password="12345",
                                      host="127.0.0.1",
                                      port="5432",
                                      database="shops_db")
        cursor = connection.cursor()
        cursor.execute('select id from shops')
        record = cursor.fetchall()
        shop_id_list = []
        for i in record:
            for j in i:
                shop_id_list.append(str(j))

        for i in range(row):
            insert_query = '''INSERT INTO public.employees(
                                first_name, last_name, phone, "e-mail", job_name, shop_id)
                                VALUES (%s, %s, %s, %s, %s, %s);'''

            fake = Faker('ru_RU')
            first_name = fake.first_name()
            last_name = fake.last_name()
            phone = fake.phone_number()
            email = fake.email()
            job = random.choice(job_list)
            if len(shop_id_list)>0:
                shop_id = random.choice(shop_id_list)
            else:
                shop_id = None

            cursor.execute(insert_query, (first_name, last_name, phone, email, job, shop_id))
            connection.commit()

        print(f'В таблицу Employees  добавлено {row} строк.')

    except (Exception, Error) as error:
        print("Ошибка при работе с PostgreSQL", error)


def add_data_shops(row):
    regions_list = ['Пермский край', 'Тюменская обл', 'Татарстан', 'Свердловская обл', 'Омская обл']

    try:
        connection = psycopg2.connect(user="postgres",
                                      password="12345",
                                      host="127.0.0.1",
                                      port="5432",
                                      database="shops_db")
        cursor = connection.cursor()
        cursor.execute('select id from public.employees')
        record = cursor.fetchall()
        exploy_id_list = []
        for i in record:
            for j in i:
                exploy_id_list.append(str(j))

        string = ''
        for i in range(row):
            insert_query = '''INSERT INTO public.shops(
                                 name, region, city, address, manager_id)
                                 VALUES (%s, %s, %s, %s, %s);'''

            fake = Faker('ru_RU')
            name = fake.company()
            region = random.choice(regions_list)
            addr = fake.address()
            city = addr.split(',')[0]
            address = string.join(addr.split(',')[1:])
            if len(exploy_id_list)>0:
                manager_id = random.choice(exploy_id_list)
            else:
                manager_id = None

            cursor.execute(insert_query, (name, region, city, address, manager_id))
            connection.commit()

        print(f'В таблицу Shops добавлено {row} строк.')

    except (Exception, Error) as error:
        print("Ошибка при работе с PostgreSQL", error)


def add_data_products(row: int):

    products_list = ["Молоко", "Хлеб", "Яблоки", "Помидоры", "Макароны", "Чай", "Кофе", "Сок", "Яйца", "Масло", "Сыр",
                     "Колбаса", "Рис", "Гречка", "Огурцы", "Печенье", "Картофель", "Сахар", "Шоколад", "Пельмени",
                     "Курица", "Грейпфрут", "Персики", "Апельсины", "Виноград", "Бананы", "Грецкие орехи", "Авокадо",
                     "Мандарины", "Сливы"]

    try:
        connection = psycopg2.connect(user="postgres",
                                      password="12345",
                                      host="127.0.0.1",
                                      port="5432",
                                      database="shops_db")
        cursor = connection.cursor()
        cursor.execute('select id from public.employees')
        record = cursor.fetchall()
        exploy_id_list = []
        for i in record:
            for j in i:
                exploy_id_list.append(str(j))

        for i in range(row):
            insert_query = '''INSERT INTO public.products(
                                   code, name)
                                   VALUES (%s, %s);'''


            code = ''.join([str(random.randint(0, 9)) for _ in range(10)])
            name = random.choice(products_list)

            cursor.execute(insert_query, (code, name))
            connection.commit()

        print(f'В таблицу Products добавлено {row} строк.')

    except (Exception, Error) as error:
        print("Ошибка при работе с PostgreSQL", error)


def add_data_purchases(row: int):
    global datetime
    try:
        connection = psycopg2.connect(user="postgres",
                                      password="12345",
                                      host="127.0.0.1",
                                      port="5432",
                                      database="shops_db")
        cursor = connection.cursor()
        cursor.execute('select id from public.employees')
        record = cursor.fetchall()
        exploy_id_list = []
        for i in record:
            for j in i:
                exploy_id_list.append(str(j))

        for i in range(row):
            insert_query = '''INSERT INTO public.purchases(
                                     datetime, amount, seller_id)
                                     VALUES (%s, %s, %s);'''

            datetime = str(fake.date_time_between(start_date=datetime(2024, 1, 1)))
            amount = random.randint(500,5000)
            if len(exploy_id_list)>0:
                seller_id = random.choice(exploy_id_list)
            else:
                seller_id = None

            cursor.execute(insert_query, (datetime, amount, seller_id))
            connection.commit()

        print(f'В таблицу Purchases добавлено {row} строк.')

    except (Exception, Error) as error:
        print("Ошибка при работе с PostgreSQL", error)


def add_purchase_receipts(row: int):
    try:
        connection = psycopg2.connect(user="postgres",
                                      password="12345",
                                      host="127.0.0.1",
                                      port="5432",
                                      database="shops_db")
        cursor = connection.cursor()
        cursor.execute('select id from public.products')
        record = cursor.fetchall()
        products_id_list = []
        for i in record:
            for j in i:
                products_id_list.append(str(j))

        cursor_1 = connection.cursor()
        cursor_1.execute('select id from public.purchases')
        record_1 = cursor_1.fetchall()
        purchases_id_list = []
        for i in record_1:
            for j in i:
                purchases_id_list.append(str(j))

        for i in range(row):
            insert_query = '''INSERT INTO public.purchase_receipts(
                                   purchase_id, product_id, quantity, amount_full, amount_discount)
                                    VALUES (%s, %s, %s, %s, %s);'''
            if len(purchases_id_list) > 0:
                purchase_id = random.choice(purchases_id_list)
            else:
                purchase_id = None
            if len(products_id_list) > 0:
                product_id = random.choice(products_id_list)
            else:
                product_id = None
            quantity = str(random.randint(1,10))
            amount_full = str(random.randint(100,1000))
            amount_discount = str(random.randint(0, 10))

            cursor.execute(insert_query, (purchase_id, product_id, quantity, amount_full, amount_discount))
            connection.commit()

        print(f'В таблицу Purchase_receipts добавлено {row} строк.')

    except (Exception, Error) as error:
        print("Ошибка при работе с PostgreSQL", error)




#if __name__ == '__main__':
    # add_data_employ(15)
    # add_data_shops(4)
    # add_data_products(17)
    #add_purchase_receipts(2)
    #add_data_purchases(1)
