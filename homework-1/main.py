"""Скрипт для заполнения данными таблиц в БД Postgres."""
import psycopg2
import csv
import os

CUSTOMERS = os.path.join('north_data', 'customers_data.csv')
EMPLOYEES = os.path.join('north_data', 'employees_data.csv')
ORDERS = os.path.join('north_data', 'orders_data.csv')
PASSWORD = os.getenv('DataBase')


def load_from_csv(file):
    with open(file, encoding='utf-8') as f:
        csv_file = csv.DictReader(f)
        return list(csv_file)


with psycopg2.connect(
    host='localhost',
    database='north',
    user='postgres',
    password=PASSWORD
) as conn:
    with conn.cursor() as cur:

        customers = load_from_csv(CUSTOMERS)
        employees = load_from_csv(EMPLOYEES)
        orders = load_from_csv(ORDERS)

        for item in customers:
            cur.execute('INSERT INTO customers VALUES (%s, %s, %s)',
                        (item['customer_id'], item['company_name'], item['contact_name']))

        for item in employees:
            cur.execute('INSERT INTO employees VALUES (%s, %s, %s, %s, %s, %s)',
                        (item['employee_id'], item['first_name'], item['last_name'],
                         item['title'], item['birth_date'], item['notes']))

        for item in orders:
            cur.execute('INSERT INTO orders VALUES (%s, %s, %s, %s, %s)',
                        (item['order_id'], item['customer_id'], item['employee_id'],
                         item['order_date'], item['ship_city']))

conn.close()
